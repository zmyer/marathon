package mesosphere.marathon.core.condition

import org.apache.mesos
import play.api.libs.json.Json
import mesos.Protos.TaskState.{ TASK_DROPPED, TASK_ERROR, TASK_FAILED, TASK_FINISHED, TASK_GONE, TASK_KILLED, TASK_KILLING, TASK_RUNNING, TASK_STAGING, TASK_STARTING, TASK_UNKNOWN, TASK_UNREACHABLE }

/**
  * To define the status of an Instance, this trait is used and stored for each Task in Task.Status.
  * The existing case objects are:
  * - marathon exclusive status
  * - representations of the mesos.Protos.TaskStatus
  * - mapping of existing (soon-to-be deprecated) mesos.Protos.TaskStatus.TASK_LOST to the new representations
  */
sealed trait Condition extends Product with Serializable {
  val mesosTaskState: Option[mesos.Protos.TaskState]

  /**
    * @return whether condition is considered a lost state.
    */
  def isLost: Boolean = this match {
    case _: Condition.Lost => true
    case _ => false
  }

  /**
    * @return whether condition is a terminal state.
    */
  def isTerminal: Boolean = this match {
    case _: Condition.Terminal => true
    case _ => false
  }

  /**
    * @return whether considered is considered active.
    */
  def isActive: Boolean = this match {
    case _: Condition.Active => true
    case _ => false
  }
}

object Condition {

  sealed trait Terminal extends Condition
  sealed trait Failure extends Condition with Terminal
  sealed trait Active extends Condition
  sealed trait Lost extends Condition

  // Reserved: Task with persistent volume has reservation, but is not launched yet
  case object Reserved extends Condition {
    val mesosTaskState = None // does not map to mesos task state
  }

  // Created: Task is known in marathon and sent to mesos, but not staged yet
  case object Created extends Condition with Active {
    val mesosTaskState = None // does not map to mesos task state
  }

  // Error: indicates that a task launch attempt failed because of an error in the task specification
  case object Error extends Condition with Failure {
    val mesosTaskState = Some(TASK_ERROR)
  }

  // Failed: task aborted with an error
  case object Failed extends Condition with Failure {
    val mesosTaskState = Some(TASK_FAILED)
  }

  // Finished: task completes successfully
  case object Finished extends Condition with Terminal {
    val mesosTaskState = Some(TASK_FINISHED)
  }

  // Killed: task was killed
  case object Killed extends Condition with Terminal {
    val mesosTaskState = Some(TASK_KILLED)
  }

  // Killing: the request to kill the task has been received, but the task has not yet been killed
  case object Killing extends Condition with Active {
    val mesosTaskState = Some(TASK_KILLING)
  }

  // Running: the state after the task has begun running successfully
  case object Running extends Condition with Active {
    val mesosTaskState = Some(TASK_RUNNING)
  }

  // Staging: the master has received the framework’s request to launch the task but the task has not yet started to run
  case object Staging extends Condition with Active {
    val mesosTaskState = Some(TASK_STAGING)
  }

  // Starting: task is currently starting
  case object Starting extends Condition with Active {
    val mesosTaskState = Some(TASK_STARTING)
  }

  // Unreachable: the master has not heard from the agent running the task for a configurable period of time
  case object Unreachable extends Condition with Active with Lost {
    val mesosTaskState = Some(TASK_UNREACHABLE)
  }

  // The task has been unreachable for a configurable time. A replacement task is started but this one won't be killed
  // yet.
  case object UnreachableInactive extends Condition {
    val mesosTaskState = Some(TASK_UNREACHABLE)
  }

  // Gone: the task was running on an agent that has been terminated
  case object Gone extends Condition with Failure with Lost {
    val mesosTaskState = Some(TASK_GONE)
  }

  // Dropped: the task failed to launch because of a transient error (e.g., spontaneously disconnected agent)
  case object Dropped extends Condition with Failure with Lost {
    val mesosTaskState = Some(TASK_DROPPED)
  }

  // Unknown: the master has no knowledge of the task
  case object Unknown extends Condition with Failure with Lost {
    val mesosTaskState = Some(TASK_UNKNOWN)
  }

  // scalastyle:off
  def apply(str: String): Condition = str.toLowerCase match {
    case "reserved" => Reserved
    case "created" => Created
    case "error" => Error
    case "failed" => Failed
    case "killed" => Killed
    case "killing" => Killing
    case "running" => Running
    case "staging" => Staging
    case "starting" => Starting
    case "unreachable" => Unreachable
    case "gone" => Gone
    case "dropped" => Dropped
    case _ => Unknown
  }
  // scalastyle:on

  def unapply(condition: Condition): Option[String] = Some(condition.toString.toLowerCase)

  implicit val conditionFormat = Json.format[Condition]
}
