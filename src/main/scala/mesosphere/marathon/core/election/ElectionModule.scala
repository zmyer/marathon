package mesosphere.marathon
package core.election

import akka.actor.Scheduler
import akka.event.EventStream
import com.typesafe.config.Config
import mesosphere.marathon.core.base.ShutdownHooks
import mesosphere.marathon.core.election.impl.{ Backoff, CuratorElectionService, ExponentialBackoff, PseudoElectionService }
import mesosphere.marathon.metrics.Metrics

trait ElectionConf extends ZookeeperConf {
  lazy val highlyAvailable = toggle(
    "ha",
    descrYes = "(Default) Run Marathon in HA mode with leader election. " +
      "Allows starting an arbitrary number of other Marathons but all need " +
      "to be started in HA mode. This mode requires a running ZooKeeper",
    descrNo = "Run Marathon in single node mode.",
    prefix = "disable_",
    noshort = true,
    default = Some(true))

  lazy val leaderElectionBackend = opt[String](
    "leader_election_backend",
    descr = "The backend for leader election to use.",
    hidden = true,
    validate = Set("curator").contains,
    default = Some("curator")
  )
}

sealed trait ElectionBackend {
  def apply(hostPort: String)(implicit scheduler: Scheduler, eventStream: EventStream,
    metrics: Metrics, backoff: Backoff,
    shutdownHooks: ShutdownHooks): ElectionService
}
case class Curator(zkConfig: ZookeeperConfig) extends ElectionBackend {
  def apply(hostPort: String)(implicit scheduler: Scheduler, eventStream: EventStream,
    metrics: Metrics, backoff: Backoff,
    shutdownHooks: ShutdownHooks): ElectionService =
    new CuratorElectionService(zkConfig, hostPort)
}

case object NoBackend extends ElectionBackend {
  def apply(hostPort: String)(implicit scheduler: Scheduler, eventStream: EventStream,
    metrics: Metrics, backoff: Backoff,
    shutdownHooks: ShutdownHooks): ElectionService =
    new PseudoElectionService(hostPort)
}

case class ElectionConfig(backend: ElectionBackend)
object ElectionConfig {
  def apply(conf: ElectionConf with ZookeeperConf): ElectionConfig = {
    if (conf.highlyAvailable()) {
      conf.leaderElectionBackend.get match {
        case Some("curator") =>
          ElectionConfig(Curator(conf.zkConfig))
        case backend =>
          throw new IllegalArgumentException(s"Leader election backend $backend not known!")
      }
    } else {
      ElectionConfig(NoBackend)
    }
  }

  def apply(conf: Config): ElectionConfig = {
    conf.getString("backend") match {
      case "curator" =>
        ElectionConfig(Curator(ZookeeperConfig(conf)))
      case "none" =>
        ElectionConfig(NoBackend)
      case backend =>
        throw new IllegalArgumentException(s"Leader election backend $backend not known!")
    }
  }
}

case class ElectionModule(config: ElectionConfig, hostPort: String)(implicit
  scheduler: Scheduler,
    eventStream: EventStream,
    metrics: Metrics,
    shutdownHooks: ShutdownHooks) {

  private[this] implicit lazy val backoff = new ExponentialBackoff(name = "offerLeadership")

  implicit lazy val electionService: ElectionService = config.backend(hostPort)
}

object ElectionModule {
  def apply(conf: ElectionConf with ZookeeperConf, hostPort: String)(implicit
    scheduler: Scheduler,
    eventStream: EventStream,
    metrics: Metrics,
    shutdownHooks: ShutdownHooks): ElectionModule =
    ElectionModule(ElectionConfig(conf), hostPort)

  def apply(config: Config, hostPort: String)(implicit
    scheduler: Scheduler,
    eventStream: EventStream,
    metrics: Metrics,
    shutdownHooks: ShutdownHooks): ElectionModule =
    ElectionModule(ElectionConfig(config), hostPort)
}