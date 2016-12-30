package mesosphere.marathon
package core.election.impl

import java.util
import java.util.Collections
import java.util.concurrent.Executors

import akka.actor.Scheduler
import akka.event.EventStream
import mesosphere.marathon.core.base._
import mesosphere.marathon.metrics.Metrics
import org.apache.curator.framework.api.ACLProvider
import org.apache.curator.framework.recipes.leader.{ LeaderLatch, LeaderLatchListener }
import org.apache.curator.framework.{ AuthInfo, CuratorFramework, CuratorFrameworkFactory }
import org.apache.curator.{ RetryPolicy, RetrySleeper }
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

class CuratorElectionService(
    config: ZookeeperConfig,
    hostPort: String)(implicit
  scheduler: Scheduler,
    eventStream: EventStream,
    metrics: Metrics,
    backoff: Backoff,
    shutdownHooks: ShutdownHooks) extends ElectionServiceBase {
  private lazy val log = LoggerFactory.getLogger(getClass.getName)

  private val callbackExecutor = Executors.newSingleThreadExecutor()

  private lazy val client = provideCuratorClient()
  private var maybeLatch: Option[LeaderLatch] = None

  override def leaderHostPortImpl: Option[String] = synchronized {
    try {
      maybeLatch.flatMap { l =>
        val participant = l.getLeader
        if (participant.isLeader) Some(participant.getId) else None
      }
    } catch {
      case NonFatal(e) =>
        log.error("error while getting current leader", e)
        None
    }
  }

  override def offerLeadershipImpl(): Unit = synchronized {
    log.info("Using HA and therefore offering leadership")
    maybeLatch.foreach { l =>
      log.info("Offering leadership while being candidate")
      l.close()
    }

    try {
      val latch = new LeaderLatch(client, config.leaderPath, hostPort, LeaderLatch.CloseMode.NOTIFY_LEADER)
      latch.addListener(Listener, callbackExecutor)
      latch.start()
      maybeLatch = Some(latch)
    } catch {
      case NonFatal(e) =>
        log.error(s"ZooKeeper initialization failed - Committing suicide: ${e.getMessage}")
        Runtime.getRuntime.asyncExit()(scala.concurrent.ExecutionContext.global)
    }
  }

  private object Listener extends LeaderLatchListener {
    override def notLeader(): Unit = CuratorElectionService.this.synchronized {
      log.info(s"Defeated (LeaderLatchListener Interface). New leader: ${leaderHostPort.getOrElse("-")}")
      stopLeadership()
    }

    override def isLeader(): Unit = CuratorElectionService.this.synchronized {
      log.info("Elected (LeaderLatchListener Interface)")
      startLeadership(error => CuratorElectionService.this.synchronized {
        maybeLatch match {
          case None => log.error("Abdicating leadership while not being leader")
          case Some(l) =>
            maybeLatch = None
            l.close()
        }
        // stopLeadership() is called in notLeader
      })
    }
  }

  def provideCuratorClient(): CuratorFramework = {
    log.info(s"Will do leader election through ${config.hosts}")

    // let the world read the leadership information as some setups depend on that to find Marathon
    val acl = new util.ArrayList[ACL]()
    acl.addAll(config.zkDefaultCreationACL)
    acl.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)

    val builder = CuratorFrameworkFactory.builder().
      connectString(config.hosts).
      sessionTimeoutMs(config.sessionTimeout.toMillis.toInt).
      connectionTimeoutMs(config.connectionTimeout.toMillis.toInt).
      aclProvider(new ACLProvider {
        val rootAcl = {
          val acls = new util.ArrayList[ACL]()
          acls.addAll(acls)
          acls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE)
          acls
        }

        override def getDefaultAcl: util.List[ACL] = acl

        override def getAclForPath(path: String): util.List[ACL] = if (path != config.path) {
          acl
        } else {
          rootAcl
        }
      }).
      retryPolicy(new RetryPolicy {
        override def allowRetry(retryCount: Int, elapsedTimeMs: Long, sleeper: RetrySleeper): Boolean = {
          log.error("ZooKeeper access failed - Committing suicide to avoid invalidating ZooKeeper state")
          Runtime.getRuntime.asyncExit()(scala.concurrent.ExecutionContext.global)
          false
        }
      })

    // optionally authenticate
    val client = (config.username, config.password) match {
      case (Some(user), Some(pass)) =>
        builder.authorization(Collections.singletonList(
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
