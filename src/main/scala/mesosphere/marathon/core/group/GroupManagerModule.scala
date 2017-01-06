package mesosphere.marathon
package core.group

import javax.inject.Provider

import akka.actor.ActorRef
import akka.event.EventStream
import akka.stream.Materializer
import kamon.Kamon
import kamon.metric.instrument.Time
import mesosphere.marathon.core.group.impl.{ GroupManagerActor, GroupManagerDelegate }
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.io.storage.StorageProvider
import mesosphere.marathon.storage.repository.{ GroupRepository, ReadOnlyAppRepository, ReadOnlyPodRepository }
import mesosphere.marathon.util.WorkQueue

import scala.concurrent.Await

/**
  * Provides a [[GroupManager]] implementation.
  */
class GroupManagerModule(
    config: MarathonConf,
    leadershipModule: LeadershipModule,
    serializeUpdates: WorkQueue,
    scheduler: Provider[DeploymentService],
    groupRepo: GroupRepository,
    appRepo: ReadOnlyAppRepository,
    podRepo: ReadOnlyPodRepository,
    storage: StorageProvider,
    eventBus: EventStream)(implicit mat: Materializer) {

  private[this] val groupManagerActorRef: ActorRef = {
    val props = GroupManagerActor.props(
      serializeUpdates,
      scheduler,
      groupRepo,
      storage,
      config,
      eventBus)
    leadershipModule.startWhenLeader(props, "groupManager")
  }

  val groupManager: GroupManager = {
    val groupManager = new GroupManagerDelegate(config, appRepo, podRepo, groupManagerActorRef)

    // We've already released metrics using these names, so we can't use the Metrics.* methods
    Kamon.metrics.gauge("service.mesosphere.marathon.app.count")(
      Await.result(groupRepo.root(), config.zkTimeoutDuration).transitiveApps.size.toLong
    )

    Kamon.metrics.gauge("service.mesosphere.marathon.group.count")(
      // Accessing rootGroup from the repository because getting it from groupManager will fail
      // on non-leader marathon instance.
      Await.result(groupRepo.root(), config.zkTimeoutDuration).transitiveGroupsById.size.toLong
    )

    val startedAt = System.currentTimeMillis()
    Kamon.metrics.gauge("service.mesosphere.marathon.uptime", Time.Milliseconds)(
      System.currentTimeMillis() - startedAt
    )

    groupManager
  }
}
