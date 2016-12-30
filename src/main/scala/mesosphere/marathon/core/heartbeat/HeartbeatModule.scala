package mesosphere.marathon
package core.heartbeat

import java.util.concurrent.TimeUnit

import akka.actor.{ ActorRef, ActorRefFactory }
import org.apache.mesos.Scheduler
import org.rogach.scallop.ScallopConf

import scala.concurrent.duration.FiniteDuration

trait HeartbeatConf extends ScallopConf {
  lazy val mesosHeartbeatInterval = opt[Long](
    "mesos_heartbeat_interval",
    descr = "(milliseconds) in the absence of receiving a message from the mesos master " +
      "during a time window of this duration, attempt to coerce mesos into communicating with marathon.",
    noshort = true,
    hidden = true,
    default = Some(MesosHeartbeatMonitor.DEFAULT_HEARTBEAT_INTERVAL_MS))

  lazy val mesosHeartbeatFailureThreshold = opt[Int](
    "mesos_heartbeat_failure_threshold",
    descr = "after missing this number of expected communications from the mesos master, " +
      "infer that marathon has become disconnected from the master.",
    noshort = true,
    hidden = true,
    default = Some(MesosHeartbeatMonitor.DEFAULT_HEARTBEAT_FAILURE_THRESHOLD))
}

case class HeartbeatModule(actorRefFactory: ActorRefFactory, conf: HeartbeatConf, scheduler: Scheduler) {
  private[this] lazy val heartbeatActor: ActorRef = actorRefFactory.actorOf(Heartbeat.props(Heartbeat.Config(
    FiniteDuration(conf.mesosHeartbeatInterval.get.getOrElse(
      MesosHeartbeatMonitor.DEFAULT_HEARTBEAT_INTERVAL_MS), TimeUnit.MILLISECONDS),
    conf.mesosHeartbeatFailureThreshold.get.getOrElse(MesosHeartbeatMonitor.DEFAULT_HEARTBEAT_FAILURE_THRESHOLD)
  )), ModuleNames.MESOS_HEARTBEAT_ACTOR)

  lazy val heartbeatMonitor: MesosHeartbeatMonitor = new MesosHeartbeatMonitor(scheduler, heartbeatActor)
}
