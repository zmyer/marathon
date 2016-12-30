package mesosphere.marathon
package core.health

import akka.actor.ActorRefFactory
import akka.event.EventStream
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.health.impl.MarathonHealthCheckManager
import mesosphere.marathon.core.task.termination.KillService
import mesosphere.marathon.core.task.tracker.InstanceTracker

/**
  * Exposes everything related to a task health, including the health check manager.
  */
case class HealthModule()(implicit
  actorRefFactory: ActorRefFactory,
    killService: KillService,
    eventBus: EventStream,
    taskTracker: InstanceTracker,
    groupManager: GroupManager) {
  lazy val healthCheckManager = new MarathonHealthCheckManager()
}
