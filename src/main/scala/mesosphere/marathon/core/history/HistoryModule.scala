package mesosphere.marathon
package core.history

import akka.actor.Props
import akka.event.EventStream
import mesosphere.marathon.core.history.impl.HistoryActor
import mesosphere.marathon.storage.repository.TaskFailureRepository

/**
  * Exposes the history actor, in charge of keeping track of the task failures.
  */
class HistoryModule(
    eventBus: EventStream,
    taskFailureRepository: TaskFailureRepository) {
  lazy val historyActorProps: Props = Props(new HistoryActor(eventBus, taskFailureRepository))
}
