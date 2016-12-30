package mesosphere.marathon
package core.launcher

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.impl.{ InstanceOpFactoryImpl, OfferProcessorImpl, TaskLauncherImpl }
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.core.task.tracker.InstanceCreationHandler
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.util.toRichConfig
import org.rogach.scallop.{ ScallopConf, ScallopOption }

import scala.concurrent.duration.Duration

trait LauncherConf extends ScallopConf {

  lazy val offerMatchingTimeout = opt[Int](
    "offer_matching_timeout",
    descr = "Offer matching timeout (ms). Stop trying to match additional tasks for this offer after this time.",
    default = Some(1000))

  lazy val saveTasksToLaunchTimeout = opt[Int](
    "save_tasks_to_launch_timeout",
    descr = "Timeout (ms) after matching an offer for saving all matched tasks that we are about to launch. " +
      "When reaching the timeout, only the tasks that we could save within the timeout are also launched. " +
      "All other task launches are temporarily rejected and retried later.",
    default = Some(3000))

  lazy val declineOfferDuration = opt[Long](
    "decline_offer_duration",
    descr = "(Default: 120 seconds) " +
      "The duration (milliseconds) for which to decline offers by default",
    default = Some(120000))

  lazy val taskReservationTimeout = opt[Long](
    "task_reservation_timeout",
    descr = "Time, in milliseconds, to wait for a new reservation to be acknowledged " +
      "via a received offer before deleting it.",
    default = Some(20000L)) // 20 seconds

  def executor: Executor = Executor.dispatch(defaultExecutor())

  lazy val defaultExecutor = opt[String](
    "executor",
    descr = "Executor to use when none is specified. If not defined the Mesos command executor is used by default.",
    default = Some("//cmd"))

  def mesosRole: ScallopOption[String]
  def mesosAuthenticationPrincipal: ScallopOption[String]
  def defaultAcceptedResourceRolesSet: Set[String]
  def envVarsPrefix: ScallopOption[String]
}

case class LauncherConfig(
  offerMatchingTimeout: Duration,
  saveTasksToLaunchTimeout: Duration,
  declineOfferDuration: Duration,
  taskReservationTimeout: Duration,
  executor: Executor,
  mesosRole: Option[String],
  defaultAcceptedResourceRoles: Set[String],
  mesosAuthenticationPrincipal: Option[String],
  envVarsPrefix: Option[String])

object LauncherConfig {
  def apply(conf: LauncherConf): LauncherConfig =
    LauncherConfig(
      offerMatchingTimeout = Duration(conf.offerMatchingTimeout().toLong, TimeUnit.MILLISECONDS),
      saveTasksToLaunchTimeout = Duration(conf.saveTasksToLaunchTimeout().toLong, TimeUnit.MILLISECONDS),
      declineOfferDuration = Duration(conf.declineOfferDuration(), TimeUnit.MILLISECONDS),
      taskReservationTimeout = Duration(conf.taskReservationTimeout(), TimeUnit.MILLISECONDS),
      executor = conf.executor,
      mesosRole = conf.mesosRole.get,
      defaultAcceptedResourceRoles = conf.defaultAcceptedResourceRolesSet,
      mesosAuthenticationPrincipal = conf.mesosAuthenticationPrincipal.get,
      envVarsPrefix = conf.envVarsPrefix.get
    )

  def apply(config: Config): LauncherConfig =
    LauncherConfig(
      offerMatchingTimeout = config.duration("offer-matching-timeout"),
      saveTasksToLaunchTimeout = config.duration("save-tasks-to-launch-timeout"),
      declineOfferDuration = config.duration("decline-offer-duration"),
      taskReservationTimeout = config.duration("task-reservation-timeout"),
      executor = Executor.dispatch(config.string("default-executor")),
      mesosRole = config.optionalString("mesos-role"),
      defaultAcceptedResourceRoles = config.stringList("default-accepted-resource-roles").to[Set],
      mesosAuthenticationPrincipal = config.optionalString("mesos-authentication-principal"),
      envVarsPrefix = config.optionalString("env-var-prefix")
    )
}

/**
  * This module contains the glue code between matching tasks to resource offers
  * and actually launching the matched tasks.
  */
case class LauncherModule(config: LauncherConfig)(implicit
  clock: Clock,
    metrics: Metrics,
    taskCreationHandler: InstanceCreationHandler,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    offerMatcher: OfferMatcher,
    pluginManager: PluginManager) {

  lazy val offerProcessor: OfferProcessor =
    new OfferProcessorImpl(
      config, clock,
      metrics,
      offerMatcher, taskLauncher, taskCreationHandler)

  lazy val taskLauncher: TaskLauncher = new TaskLauncherImpl(
    metrics,
    marathonSchedulerDriverHolder)

  lazy val taskOpFactory: InstanceOpFactory = new InstanceOpFactoryImpl(config, pluginManager)
}

object LauncherModule {
  def apply(conf: LauncherConf)(implicit
    clock: Clock,
    metrics: Metrics,
    taskCreationHandler: InstanceCreationHandler,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    offerMatcher: OfferMatcher,
    pluginManager: PluginManager): LauncherModule =
    LauncherModule(LauncherConfig(conf))

  def apply(conf: Config)(implicit
    clock: Clock,
    metrics: Metrics,
    taskCreationHandler: InstanceCreationHandler,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    offerMatcher: OfferMatcher,
    pluginManager: PluginManager): LauncherModule =
    LauncherModule(LauncherConfig(conf))
}