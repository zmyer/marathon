package mesosphere.marathon
package core.group

import javax.inject.Provider

import akka.actor.ActorRef
import akka.event.EventStream
import akka.stream.Materializer
import com.codahale.metrics.Gauge
import com.typesafe.config.Config
import mesosphere.marathon.core.group.impl.{ GroupManagerActor, GroupManagerDelegate }
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.io.storage.StorageProvider
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.storage.repository.{ GroupRepository, ReadOnlyAppRepository, ReadOnlyPodRepository }

import scala.concurrent.Await

/**
  * Provides a [[GroupManager]] implementation.
  */
case class GroupManagerModule(config: GroupManagerConfig)(implicit
  leadershipModule: LeadershipModule,
    scheduler: Provider[DeploymentService],
    groupRepo: GroupRepository,
    appRepo: ReadOnlyAppRepository,
    podRepo: ReadOnlyPodRepository,
    storage: StorageProvider,
    eventBus: EventStream,
    mat: Materializer,
    metrics: Metrics) {

  private[this] val groupManagerActorRef: ActorRef = {
    val props = GroupManagerActor.props(config.maxQueuedUpdates, config.availableFeatures, config.localPortMin, config.localPortMax)
    leadershipModule.startWhenLeader(props, "groupManager")
  }

  val groupManager: GroupManager = {
    val groupManager = new GroupManagerDelegate(config.requestTimeout, appRepo, podRepo, groupManagerActorRef)

    metrics.gauge("service.mesosphere.marathon.app.count", new Gauge[Int] {
      override def getValue: Int = {
        // Accessing rootGroup from the repository because getting it from groupManager will fail
        // on non-leader marathon instance.
        Await.result(groupRepo.root(), config.requestTimeout).transitiveApps.size
      }
    })

    metrics.gauge("service.mesosphere.marathon.group.count", new Gauge[Int] {
      override def getValue: Int = {
        // Accessing rootGroup from the repository because getting it from groupManager will fail
        // on non-leader marathon instance.
        Await.result(groupRepo.root(), config.requestTimeout).transitiveGroupsById.size
      }
    })

    metrics.gauge("service.mesosphere.marathon.uptime", new Gauge[Long] {
      val startedAt = System.currentTimeMillis()

      override def getValue: Long = {
        System.currentTimeMillis() - startedAt
      }
    })

    groupManager
  }
}

object GroupManagerModule {
  def apply(config: Config)(implicit
    leadershipModule: LeadershipModule,
    scheduler: Provider[DeploymentService],
    groupRepo: GroupRepository,
    appRepo: ReadOnlyAppRepository,
    podRepo: ReadOnlyPodRepository,
    storage: StorageProvider,
    eventBus: EventStream,
    mat: Materializer,
    metrics: Metrics): GroupManagerModule =
    GroupManagerModule(GroupManagerConfig(config))

  def apply(conf: GroupManagerConf)(implicit
    leadershipModule: LeadershipModule,
    scheduler: Provider[DeploymentService],
    groupRepo: GroupRepository,
    appRepo: ReadOnlyAppRepository,
    podRepo: ReadOnlyPodRepository,
    storage: StorageProvider,
    eventBus: EventStream,
    mat: Materializer,
    metrics: Metrics): GroupManagerModule =
    GroupManagerModule(GroupManagerConfig(conf))
}