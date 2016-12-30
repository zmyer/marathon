package mesosphere.marathon
package core.election.impl

import akka.actor.Scheduler
import akka.event.EventStream
import mesosphere.marathon.core.base.ShutdownHooks
import mesosphere.marathon.metrics.Metrics
import org.slf4j.LoggerFactory

class PseudoElectionService(hostPort: String)(implicit
  scheduler: Scheduler,
    eventStream: EventStream,
    metrics: Metrics,
    backoff: Backoff,
    shutdownHooks: ShutdownHooks) extends ElectionServiceBase {
  private val log = LoggerFactory.getLogger(getClass.getName)

  override def leaderHostPortImpl: Option[String] = if (isLeader) Some(hostPort) else None

  override def offerLeadershipImpl(): Unit = synchronized {
    log.info("Not using HA and therefore electing as leader by default")
    startLeadership(_ => stopLeadership())
  }
}
