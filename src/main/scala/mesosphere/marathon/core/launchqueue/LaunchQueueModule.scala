package mesosphere.marathon
package core.launchqueue

import java.util.concurrent.TimeUnit

import akka.actor.{ ActorRef, Props }
import com.typesafe.config.Config
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.flow.OfferReviver
import mesosphere.marathon.core.launcher.InstanceOpFactory
import mesosphere.marathon.core.launchqueue.impl._
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.manager.OfferMatcherManager
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.RunSpec
import org.rogach.scallop.ScallopConf
import mesosphere.marathon.util.toRichConfig
import scala.concurrent.duration.Duration

trait LaunchQueueConf extends ScallopConf {

  lazy val launchQueueRequestTimeout = opt[Int](
    "launch_queue_request_timeout",
    descr = "INTERNAL TUNING PARAMETER: Timeout (in ms) for requests to the launch queue actor.",
    hidden = true,
    default = Some(1000))

  lazy val taskOpNotificationTimeout = opt[Int](
    "task_operation_notification_timeout",
    descr = "INTERNAL TUNING PARAMETER: Timeout (in ms) for matched task opereations to be accepted or rejected.",
    hidden = true,
    default = Some(10000))
}

case class LaunchQueueConfig(
  requestTimeout: Duration,
  taskOpNotificationTimeout: Duration)

object LaunchQueueConfig {
  def apply(conf: LaunchQueueConf): LaunchQueueConfig =
    LaunchQueueConfig(
      requestTimeout = Duration(conf.launchQueueRequestTimeout().toLong, TimeUnit.MILLISECONDS),
      taskOpNotificationTimeout = Duration(conf.taskOpNotificationTimeout().toLong, TimeUnit.MILLISECONDS)
    )

  def apply(conf: Config): LaunchQueueConfig =
    LaunchQueueConfig(
      requestTimeout = conf.duration("request-timeout"),
      taskOpNotificationTimeout = conf.duration("task-op-notification-timeout")
    )
}
/**
  * Provides a [[LaunchQueue]] implementation which can be used to launch tasks for a given RunSpec.
  */
case class LaunchQueueModule(
  config: LaunchQueueConfig,
    leadershipModule: LeadershipModule,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    maybeOfferReviver: Option[OfferReviver],
    taskTracker: InstanceTracker,
    taskOpFactory: InstanceOpFactory) {

  private[this] val offerMatchStatisticsActor: ActorRef = {
    leadershipModule.startWhenLeader(OfferMatchStatisticsActor.props(), "offerMatcherStatistics")
  }

  private[this] val launchQueueActorRef: ActorRef = {
    def runSpecActorProps(runSpec: RunSpec, count: Int): Props =
      TaskLauncherActor.props(
        config,
        subOfferMatcherManager,
        clock,
        taskOpFactory,
        maybeOfferReviver,
        taskTracker,
        rateLimiterActor,
        offerMatchStatisticsActor)(runSpec, count)
    val props = LaunchQueueActor.props(config, offerMatchStatisticsActor, runSpecActorProps)
    leadershipModule.startWhenLeader(props, "launchQueue")
  }

  val rateLimiter: RateLimiter = new RateLimiter(clock)
  private[this] val rateLimiterActor: ActorRef = {
    val props = RateLimiterActor.props(
      rateLimiter, launchQueueActorRef)
    leadershipModule.startWhenLeader(props, "rateLimiter")
  }
  val launchQueue: LaunchQueue = new LaunchQueueDelegate(config, launchQueueActorRef, rateLimiterActor)
}

object LaunchQueueModule {
  def apply(
    launchQueueConf: LaunchQueueConf,
    leadershipModule: LeadershipModule,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    maybeOfferReviver: Option[OfferReviver],
    taskTracker: InstanceTracker,
    taskOpFactory: InstanceOpFactory): LaunchQueueModule =
    LaunchQueueModule(LaunchQueueConfig(launchQueueConf), leadershipModule,
      clock, subOfferMatcherManager, maybeOfferReviver, taskTracker, taskOpFactory)

  def apply(
    launchQueueConf: Config,
    leadershipModule: LeadershipModule,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    maybeOfferReviver: Option[OfferReviver],
    taskTracker: InstanceTracker,
    taskOpFactory: InstanceOpFactory): LaunchQueueModule =
    LaunchQueueModule(LaunchQueueConfig(launchQueueConf), leadershipModule,
      clock, subOfferMatcherManager, maybeOfferReviver, taskTracker, taskOpFactory)

}