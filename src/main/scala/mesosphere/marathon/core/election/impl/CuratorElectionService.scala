package mesosphere.marathon.core.election.impl

import com.typesafe.scalalogging.StrictLogging
import java.util
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.event.EventStream
import com.codahale.metrics.MetricRegistry
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.core.base.{ toRichRuntime, ShutdownHooks }
import mesosphere.marathon.metrics.Metrics
import org.apache.curator.framework.api.ACLProvider
import org.apache.curator.{ RetrySleeper, RetryPolicy }
import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory, AuthInfo }
import org.apache.curator.framework.recipes.leader.{ LeaderLatch, LeaderLatchListener }
import org.apache.zookeeper.data.ACL
import org.apache.zookeeper.{ ZooDefs, KeeperException, CreateMode }
import scala.concurrent.Future

import scala.util.control.NonFatal
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext

import CuratorElectionService._

/**
  * Handles the bulk of our election concerns.
  *
  * TODO - This code should be substantially simplified https://github.com/mesosphere/marathon/issues/4908
  */
class CuratorElectionService(
  config: MarathonConf,
  system: ActorSystem,
  eventStream: EventStream,
  http: HttpConf,
  metrics: Metrics = new Metrics(new MetricRegistry),
  hostPort: String,
  backoff: ExponentialBackoff,
  shutdownHooks: ShutdownHooks) extends ElectionServiceBase(
  config, system, eventStream, metrics, backoff, shutdownHooks
) with StrictLogging {
  private[this] val serialExecutor = Executors.newSingleThreadExecutor()
  /* We go ahead and re-use the serial executor here because this thing blocks, a lot. Changing that would be a
   * substantial change because the parent class locks, too. */
  override protected implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(serialExecutor)

  private[this] lazy val client = provideCuratorClient()
  private[this] var maybeLatch: Option[LeaderLatch] = None

  private[this] val twitterCommonsTombstone = new TwitterCommonsTombstone(config.zooKeeperLeaderPath, client, hostPort)

  override def leaderHostPortImpl: Option[String] = synchronized {
    try {
      maybeLatch.flatMap { l =>
        val participant = l.getLeader
        if (participant.isLeader) Some(participant.getId) else None
      }
    } catch {
      case NonFatal(e) =>
        logger.error("error while getting current leader", e)
        None
    }
  }

  override def offerLeadershipImpl(): Unit = synchronized {
    logger.info("Using HA and therefore offering leadership")
    maybeLatch.foreach { l =>
      logger.info("Offering leadership while being candidate")
      l.close()
    }

    val latch = new LeaderLatch(
      client, config.zooKeeperLeaderPath + "-curator", hostPort, LeaderLatch.CloseMode.NOTIFY_LEADER
    )
    maybeLatch = Some(latch)

    val asyncListener = new AsyncDelegateLeaderLatchListener(
      new LeaderLatchListener {
        override def isLeader(): Unit = becomeLeader()
        override def notLeader(): Unit = unbecomeLeader()
      }
    )
    latch.addListener(asyncListener, serialExecutor)
    latch.start()
  }

  private[this] def unbecomeLeader(): Unit = synchronized {
    logger.info(s"Leader defeated. New leader: ${leaderHostPort.getOrElse("-")}")

    // remove tombstone for twitter commons
    twitterCommonsTombstone.delete(onlyMyself = true)

    stopLeadership()
  }

  /**
    * Called, ultimately by LeaderLatchListener. Transitions the election service to the leader state and fires
    * necessary events.
    */
  private[this] def becomeLeader(): Unit = synchronized {
    logger.info("Leader elected")
    startLeadership(error => synchronized {
      maybeLatch match {
        case None => logger.error("Abdicating leadership while not being leader")
        case Some(l) =>
          maybeLatch = None
          l.close()
      }
    })

    // write a tombstone into the old twitter commons leadership election path which always
    // wins the selection. Check that startLeadership was successful and didn't abdicate.
    if (isLeader) {
      try {
        twitterCommonsTombstone.create()
      } catch {
        case e: Exception =>
          logger.error(s"Exception while creating tombstone for twitter commons leader election: ${e.getMessage}")
          abdicateLeadership(error = true)
      }
    }
  }

  private[this] def provideCuratorClient(): CuratorFramework = {
    logger.info(s"Will do leader election through ${config.zkHosts}")

    // let the world read the leadership information as some setups depend on that to find Marathon
    val acl = new util.ArrayList[ACL]()
    acl.addAll(config.zkDefaultCreationACL)
    acl.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)

    val builder = CuratorFrameworkFactory.builder().
      connectString(config.zkHosts).
      sessionTimeoutMs(config.zooKeeperSessionTimeout().toInt).
      aclProvider(new ACLProvider {
        override def getDefaultAcl: util.List[ACL] = acl
        override def getAclForPath(path: String): util.List[ACL] = acl
      }).
      retryPolicy(new RetryPolicy {
        override def allowRetry(retryCount: Int, elapsedTimeMs: Long, sleeper: RetrySleeper): Boolean = {
          logger.error("ZooKeeper access failed - Committing suicide to avoid invalidating ZooKeeper state")
          Runtime.getRuntime.asyncExit()(ExecutionContext.global)
          false
        }
      })

    // optionally authenticate
    val client = (config.zkUsername, config.zkPassword) match {
      case (Some(user), Some(pass)) =>
        builder.authorization(List(
          new AuthInfo("digest", (user + ":" + pass).getBytes("UTF-8"))
        )).build()
      case _ =>
        builder.build()
    }

    client.start()
    client.getZookeeperClient.blockUntilConnectedOrTimedOut()
    client
  }
}

object CuratorElectionService {
  private[impl] class TwitterCommonsTombstone(zooKeeperLeaderPath: String, client: CuratorFramework, hostPort: String) extends StrictLogging {
    private def memberPath(member: String): String = {
      zooKeeperLeaderPath.stripSuffix("/") + "/" + member
    }

    // - precedes 0-9 in ASCII and hence this instance overrules other candidates
    private lazy val memberName = "member_-00000000"
    private lazy val path = memberPath(memberName)

    var fallbackCreated = false

    /**
      * Create a ephemeral node which is not removed when loosing leadership. This is necessary to avoid a race of old
      * Marathon instances which think that they can become leader in the moment the new instances failover and no
      * tombstone is existing (yet).
      *
      * Synchronous. Throws an exception on failure.
      */
    def create(): Unit = {
      delete(onlyMyself = false)

      client.createContainers(zooKeeperLeaderPath)

      if (!fallbackCreated) {
        client.create().
          creatingParentsIfNeeded().
          withMode(CreateMode.EPHEMERAL_SEQUENTIAL).
          forPath(memberPath("member_-1"), hostPort.getBytes("UTF-8"))
        fallbackCreated = true
      }

      logger.info("Creating tombstone for old twitter commons leader election")
      client.create().
        creatingParentsIfNeeded().
        withMode(CreateMode.EPHEMERAL).
        forPath(path, hostPort.getBytes("UTF-8"))

    }

    def delete(onlyMyself: Boolean = false): Unit = {
      Option(client.checkExists().forPath(path)) match {
        case None =>
        case Some(tombstone) =>
          try {
            if (!onlyMyself || client.getData.forPath(memberPath(memberName)).toString == hostPort) {
              logger.info("Deleting existing tombstone for old twitter commons leader election")
              client.delete().guaranteed().withVersion(tombstone.getVersion).forPath(path)
            }
          } catch {
            case _: KeeperException.NoNodeException =>
            case _: KeeperException.BadVersionException =>
          }
      }
    }
  }

  /**
    * Listener which forwards leadership status via the provided function.
    *
    * We delegate the methods asynchronously so they are processed outside of the synchronized lock for LeaderLatch.setLeadership
    */
  private[impl] class AsyncDelegateLeaderLatchListener(delegate: LeaderLatchListener)(implicit ec: ExecutionContext) extends LeaderLatchListener {
    override final def notLeader(): Unit = Future { delegate.notLeader() }
    override final def isLeader(): Unit = Future { delegate.isLeader() }
  }
}
