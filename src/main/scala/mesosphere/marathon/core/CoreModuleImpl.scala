package mesosphere.marathon
package core

import java.time.Clock
import java.util.concurrent.Executors
import javax.inject.Named

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.EventStream
import com.google.inject.{ Inject, Provider }
import mesosphere.marathon.core.async.ExecutionContexts
import mesosphere.marathon.core.auth.AuthModule
import mesosphere.marathon.core.base.{ ActorsModule, JvmExitsCrashStrategy, LifecycleState }
import mesosphere.marathon.core.deployment.DeploymentModule
import mesosphere.marathon.core.election._
import mesosphere.marathon.core.event.EventModule
import mesosphere.marathon.core.flow.FlowModule
import mesosphere.marathon.core.group.GroupManagerModule
import mesosphere.marathon.core.health.HealthModule
import mesosphere.marathon.core.heartbeat.MesosHeartbeatMonitor
import mesosphere.marathon.core.history.HistoryModule
import mesosphere.marathon.core.instance.update.InstanceChangeHandler
import mesosphere.marathon.core.launcher.LauncherModule
import mesosphere.marathon.core.launcher.impl.UnreachableReservedOfferMonitor
import mesosphere.marathon.core.launchqueue.LaunchQueueModule
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.base.util.StopOnFirstMatchingOfferMatcher
import mesosphere.marathon.core.matcher.manager.OfferMatcherManagerModule
import mesosphere.marathon.core.matcher.reconcile.OfferMatcherReconciliationModule
import mesosphere.marathon.core.plugin.PluginModule
import mesosphere.marathon.core.pod.PodModule
import mesosphere.marathon.core.readiness.ReadinessModule
import mesosphere.marathon.core.task.jobs.TaskJobsModule
import mesosphere.marathon.core.task.termination.TaskTerminationModule
import mesosphere.marathon.core.task.tracker.InstanceTrackerModule
import mesosphere.marathon.core.task.update.TaskStatusUpdateProcessor
import mesosphere.marathon.storage.StorageModule
import mesosphere.util.state.MesosLeaderInfo
import scala.concurrent.ExecutionContext

import scala.util.Random

/**
  * Provides the wiring for the core module.
  *
  * Its parameters represent guice wired dependencies.
  * [[CoreGuiceModule]] exports some dependencies back to guice.
  */
class CoreModuleImpl @Inject() (
    // external dependencies still wired by guice
    marathonConf: MarathonConf,
    eventStream: EventStream,
    @Named(ModuleNames.HOST_PORT) hostPort: String,
    actorSystem: ActorSystem,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    clock: Clock,
    scheduler: Provider[DeploymentService],
    instanceUpdateSteps: Seq[InstanceChangeHandler],
    taskStatusUpdateProcessor: TaskStatusUpdateProcessor,
    mesosLeaderInfo: MesosLeaderInfo,
    @Named(ModuleNames.MESOS_HEARTBEAT_ACTOR) heartbeatActor: ActorRef
)
  extends CoreModule {

  // INFRASTRUCTURE LAYER

  private[this] lazy val random = Random
  private[this] lazy val lifecycleState = LifecycleState.WatchingJVM
  override lazy val actorsModule = new ActorsModule(actorSystem)
  private[this] lazy val crashStrategy = JvmExitsCrashStrategy
  private[this] val electionExecutor = Executors.newSingleThreadExecutor()

  override lazy val leadershipModule = LeadershipModule(actorsModule.actorRefFactory)
  override lazy val electionModule = new ElectionModule(
    marathonConf,
    actorSystem,
    eventStream,
    hostPort,
    crashStrategy,
    ExecutionContext.fromExecutor(electionExecutor)
  )

  // TASKS

  override lazy val taskTrackerModule =
    new InstanceTrackerModule(clock, marathonConf, leadershipModule,
      storageModule.instanceRepository, instanceUpdateSteps)(actorsModule.materializer)
  override lazy val taskJobsModule = new TaskJobsModule(marathonConf, leadershipModule, clock)
  override lazy val storageModule = StorageModule(
    marathonConf,
    lifecycleState)(
    actorsModule.materializer,
    ExecutionContexts.global,
    actorSystem.scheduler,
    actorSystem)

  // READINESS CHECKS
  override lazy val readinessModule = new ReadinessModule(actorSystem, actorsModule.materializer)

  // this one can't be lazy right now because it wouldn't be instantiated soon enough ...
  override val taskTerminationModule = new TaskTerminationModule(
    taskTrackerModule, leadershipModule, marathonSchedulerDriverHolder, marathonConf, clock)

  // OFFER MATCHING AND LAUNCHING TASKS

  private[this] lazy val offerMatcherManagerModule = new OfferMatcherManagerModule(
    // infrastructure
    clock, random, marathonConf,
    leadershipModule,
    () => marathonScheduler.getLocalRegion
  )

  private[this] lazy val offerMatcherReconcilerModule =
    new OfferMatcherReconciliationModule(
      marathonConf,
      clock,
      actorSystem.eventStream,
      taskTrackerModule.instanceTracker,
      storageModule.groupRepository,
      leadershipModule
    )

  override lazy val launcherModule = new LauncherModule(
    // infrastructure
    marathonConf,

    // external guicedependencies
    taskTrackerModule.stateOpProcessor,
    marathonSchedulerDriverHolder,

    // internal core dependencies
    StopOnFirstMatchingOfferMatcher(
      offerMatcherReconcilerModule.offerMatcherReconciler,
      offerMatcherManagerModule.globalOfferMatcher
    ),
    pluginModule.pluginManager,
    offerStreamInput
  )(clock)

  lazy val offerStreamInput = UnreachableReservedOfferMonitor.run(
    lookupInstance = taskTrackerModule.instanceTracker.instance(_),
    taskStatusPublisher = taskStatusUpdateProcessor.publish(_)
  )(actorsModule.materializer)

  override lazy val appOfferMatcherModule = new LaunchQueueModule(
    marathonConf,
    leadershipModule, clock,

    // internal core dependencies
    offerMatcherManagerModule.subOfferMatcherManager,
    maybeOfferReviver,

    // external guice dependencies
    taskTrackerModule.instanceTracker,
    launcherModule.taskOpFactory,
    () => marathonScheduler.getLocalRegion
  )

  // PLUGINS
  override lazy val pluginModule = new PluginModule(marathonConf, crashStrategy)

  override lazy val authModule: AuthModule = new AuthModule(pluginModule.pluginManager)

  // FLOW CONTROL GLUE

  private[this] lazy val flowActors = new FlowModule(leadershipModule)

  flowActors.refillOfferMatcherManagerLaunchTokens(
    marathonConf, offerMatcherManagerModule.subOfferMatcherManager)

  /** Combine offersWanted state from multiple sources. */
  private[this] lazy val offersWanted =
    offerMatcherManagerModule.globalOfferMatcherWantsOffers
      .combineLatest(offerMatcherReconcilerModule.offersWantedObservable)
      .map { case (managerWantsOffers, reconciliationWantsOffers) => managerWantsOffers || reconciliationWantsOffers }

  lazy val maybeOfferReviver = flowActors.maybeOfferReviver(
    clock, marathonConf,
    actorSystem.eventStream,
    offersWanted,
    marathonSchedulerDriverHolder)

  // EVENT

  override lazy val eventModule: EventModule = new EventModule(
    eventStream, actorSystem, marathonConf,
    electionModule.service, authModule.authenticator, authModule.authorizer)(actorsModule.materializer)

  // HISTORY

  override lazy val historyModule: HistoryModule =
    new HistoryModule(eventStream, storageModule.taskFailureRepository)

  // HEALTH CHECKS

  override lazy val healthModule: HealthModule = new HealthModule(
    actorSystem, taskTerminationModule.taskKillService, eventStream,
    taskTrackerModule.instanceTracker, groupManagerModule.groupManager)(actorsModule.materializer)

  // GROUP MANAGER

  override lazy val groupManagerModule: GroupManagerModule = new GroupManagerModule(
    marathonConf,
    scheduler,
    storageModule.groupRepository)(ExecutionContexts.global, eventStream, authModule.authorizer)

  // PODS

  override lazy val podModule: PodModule = PodModule(groupManagerModule.groupManager)

  // DEPLOYMENT MANAGER

  override lazy val deploymentModule: DeploymentModule = new DeploymentModule(
    marathonConf,
    leadershipModule,
    taskTrackerModule.instanceTracker,
    taskTerminationModule.taskKillService,
    appOfferMatcherModule.launchQueue,
    schedulerActions, // alternatively schedulerActionsProvider.get()
    healthModule.healthCheckManager,
    eventStream,
    readinessModule.readinessCheckExecutor,
    storageModule.deploymentRepository
  )(actorsModule.materializer)

  // GREEDY INSTANTIATION
  //
  // Greedily instantiate everything.
  //
  // lazy val allows us to write down object instantiations in any order.
  //
  // The LeadershipModule requires that all actors have been registered when the controller
  // is created. Changing the wiring order for this feels wrong since it is nicer if it
  // follows architectural logic. Therefore we instantiate them here explicitly.

  taskJobsModule.handleOverdueTasks(
    taskTrackerModule.instanceTracker,
    taskTrackerModule.stateOpProcessor,
    taskTerminationModule.taskKillService
  )
  taskJobsModule.expungeOverdueLostTasks(taskTrackerModule.instanceTracker, taskTrackerModule.stateOpProcessor)
  maybeOfferReviver
  offerMatcherManagerModule
  launcherModule
  offerMatcherReconcilerModule.start()
  eventModule
  historyModule
  healthModule
  podModule

  // The core (!) of the problem is that SchedulerActions are needed by MarathonModule::provideSchedulerActor
  // and CoreModule::deploymentModule. So until MarathonSchedulerActor is also a core component
  // and moved to CoreModules we can either:
  //
  // 1. Provide it in MarathonModule, inject as a constructor parameter here, in CoreModuleImpl and deal
  //    with Guice's "circular references involving constructors" e.g. by making it a Provider[SchedulerActions]
  //    to defer it's creation or:
  // 2. Create it here though it's not a core module and export it back via @Provider for MarathonModule
  //    to inject it in provideSchedulerActor(...) method.
  //
  // TODO: this can be removed when MarathonSchedulerActor becomes a core component
  override lazy val schedulerActions: SchedulerActions = new SchedulerActions(
    storageModule.groupRepository,
    healthModule.healthCheckManager,
    taskTrackerModule.instanceTracker,
    appOfferMatcherModule.launchQueue,
    eventStream,
    taskTerminationModule.taskKillService)(ExecutionContexts.global)

  override lazy val marathonScheduler: MarathonScheduler = new MarathonScheduler(eventStream, launcherModule.offerProcessor, taskStatusUpdateProcessor, storageModule.frameworkIdRepository, mesosLeaderInfo, marathonConf)

  // MesosHeartbeatMonitor decorates MarathonScheduler
  override def mesosHeartbeatMonitor = new MesosHeartbeatMonitor(marathonScheduler, heartbeatActor)
}
