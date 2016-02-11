package mesosphere.marathon.tasks

import com.google.inject.Inject
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.AppDefinition
import mesosphere.marathon.tasks.TaskFactory.CreatedTask
import mesosphere.mesos.TaskBuilder
import org.apache.mesos.Protos.Offer
import org.slf4j.LoggerFactory

class DefaultTaskFactory @Inject() (
  config: MarathonConf,
  clock: Clock)
    extends TaskFactory {

  private[this] val log = LoggerFactory.getLogger(getClass)

  override def newTask(
    app: AppDefinition,
    offer: Offer,
    runningTasks: Iterable[Task],
    existingTask: Option[Task] = None
  ): Option[CreatedTask] = {
    log.debug("newTask")

    new TaskBuilder(app, Task.Id.forApp, config).buildIfMatches(offer, runningTasks).map {
      case (taskInfo, ports) =>
        val agentInfo = Task.AgentInfo.forOffer(offer)
        val baseTask = existingTask match {
          case Some(oldTask) => oldTask.copy(agentInfo = agentInfo)
          case None          => Task.minimalTask(app.id, agentInfo)
        }
        val launched = Task.Launched(
          appVersion = app.version,
          status = Task.Status(
            stagedAt = clock.now()
          ),
          networking = Task.HostPorts(ports)
        )
        val task = baseTask.copy(launched = Some(launched))
        CreatedTask(taskInfo, task)
    }
  }
}
