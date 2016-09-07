package mesosphere.marathon.core.task.tracker.impl

import akka.actor.ActorRef
import akka.util.Timeout
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.InstanceStateOp.ReservationTimeout
import mesosphere.marathon.core.task.{ TaskStateChange, InstanceStateOp }
import mesosphere.marathon.core.task.tracker.impl.InstanceTrackerActor.ForwardTaskOp
import mesosphere.marathon.core.task.tracker.{
  TaskReservationTimeoutHandler,
  TaskStateOpProcessor,
  TaskCreationHandler,
  InstanceTrackerConfig
}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * Implements the [[TaskStateOpProcessor]] trait by sending messages to the TaskTracker actor.
  */
private[tracker] class TaskCreationHandlerAndUpdaterDelegate(
  clock: Clock,
  conf: InstanceTrackerConfig,
  taskTrackerRef: ActorRef)
    extends TaskCreationHandler with TaskStateOpProcessor with TaskReservationTimeoutHandler {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[impl] implicit val timeout: Timeout = conf.internalTaskUpdateRequestTimeout().milliseconds

  override def process(stateOp: InstanceStateOp): Future[TaskStateChange] = {
    taskUpdate(stateOp.instanceId, stateOp)
  }

  override def created(taskStateOp: InstanceStateOp): Future[Unit] = {
    process(taskStateOp).map(_ => ())
  }
  override def terminated(stateOp: InstanceStateOp.ForceExpunge): Future[_] = {
    process(stateOp)
  }
  override def timeout(stateOp: ReservationTimeout): Future[_] = {
    process(stateOp)
  }

  private[this] def taskUpdate(taskId: Instance.Id, taskStateOp: InstanceStateOp): Future[TaskStateChange] = {
    import akka.pattern.ask
    val deadline = clock.now + timeout.duration
    val op: ForwardTaskOp = InstanceTrackerActor.ForwardTaskOp(deadline, taskId, taskStateOp)
    (taskTrackerRef ? op).mapTo[TaskStateChange].recover {
      case NonFatal(e) =>
        throw new RuntimeException(s"while asking for $taskStateOp on app [${taskId.runSpecId}] and $taskId", e)
    }
  }
}
