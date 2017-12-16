package mesosphere.marathon
package instance

import mesosphere.UnitTest
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.pod.MesosContainer
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.raml.Resources
import mesosphere.marathon.state.PathId._

class InstanceIdTest extends UnitTest {

  "InstanceId" should {
    "AppIds can be converted to InstanceIds and back to AppIds" in {
      val appId = "/test/foo/bla/rest".toPath
      val instanceId = Instance.Id.forRunSpec(appId)
      instanceId.runSpecId should equal(appId)
    }

    "InstanceIds can be converted to TaskIds without container name" in {
      val appId = "/test/foo/bla/rest".toPath
      val instanceId = Instance.Id.forRunSpec(appId)
      val taskId = Task.Id.forInstanceId(instanceId, container = None)
      taskId.idString should be(instanceId.idString + ".$anon")
    }

    "InstanceIds can be converted to TaskIds with container name" in {
      val appId = "/test/foo/bla/rest".toPath
      val instanceId = Instance.Id.forRunSpec(appId)
      val container = MesosContainer("firstOne", resources = Resources())
      val taskId = Task.Id.forInstanceId(instanceId, Some(container))
      taskId.idString should be(instanceId.idString + ".firstOne")
    }

    "InstanceIds can be converted from TaskIds with container name" in {
      val appId = "/test/foo/bla/rest".toPath
      val parsedTaskId = Task.Id("test_foo_bla_rest.instance-myinstance.someContainerName")
      parsedTaskId.runSpecId should be(appId)
      parsedTaskId.instanceId should be(Instance.Id("test_foo_bla_rest.instance-myinstance"))
      parsedTaskId.containerName should be('nonEmpty)
      parsedTaskId.containerName should be(Some("someContainerName"))
    }

    "InstanceIds can be converted from TaskIds without a container name" in {
      val appId = "/test/foo/bla/rest".toPath
      val parsedTaskId = Task.Id("test_foo_bla_rest.instance-myinstance.$anon")
      parsedTaskId.runSpecId should be(appId)
      parsedTaskId.instanceId should be(Instance.Id("test_foo_bla_rest.instance-myinstance"))
      parsedTaskId.containerName should be('empty)
    }

    "InstanceIds should be created by static string" in {
      val idString = "app.marathon-task"
      val instanceId = Instance.Id(idString)
      instanceId.idString should be(idString)
      instanceId.runSpecId.safePath should be("app")
      val taskId = Task.Id(idString + ".app")
      taskId.instanceId should be(instanceId)
    }
  }
}
