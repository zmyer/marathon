package mesosphere.marathon

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task
import org.scalatest.{ BeforeAndAfter, FunSuiteLike, OptionValues }
import org.scalatest.mockito.MockitoSugar

trait MarathonSpec extends FunSuiteLike with BeforeAndAfter with MockitoSugar with OptionValues {
  implicit def tasksToInstances(tasks: Iterable[Task]): Iterable[Instance] = tasks.map(task => Instance(task))

  implicit def taskToInstance(task: Task): Instance = Instance(task)
}
