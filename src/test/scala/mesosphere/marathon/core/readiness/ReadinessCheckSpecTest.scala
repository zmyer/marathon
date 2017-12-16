package mesosphere.marathon
package core.readiness

import mesosphere.UnitTest
import mesosphere.marathon.core.instance.TestTaskBuilder
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.state.NetworkInfo
import mesosphere.marathon.state.{ AppDefinition, PathId, PortDefinition }

class ReadinessCheckSpecTest extends UnitTest {
  "ReadinessCheckSpec" should {
    "readiness check specs for one task with dynamic ports and one readiness check" in {
      val f = new Fixture

      Given("an app with a readiness check and a randomly assigned port")
      val app = f.appWithOneReadinessCheck
      And("a task with two host port")
      val task = f.taskWithPorts

      When("calculating the ReadinessCheckSpec")
      val specs = ReadinessCheckExecutor.ReadinessCheckSpec.readinessCheckSpecsForTask(app, task)

      Then("we get one spec")
      specs should have size 1
      val spec = specs.head
      And("it has the correct url")
      spec.url should equal(s"http://${f.hostName}:80/")
      And("the rest of the fields are correct, too")
      spec should equal(
        ReadinessCheckExecutor.ReadinessCheckSpec(
          url = s"http://${f.hostName}:80/",
          taskId = task.taskId,
          checkName = app.readinessChecks.head.name,
          interval = app.readinessChecks.head.interval,
          timeout = app.readinessChecks.head.timeout,
          httpStatusCodesForReady = app.readinessChecks.head.httpStatusCodesForReady,
          preserveLastResponse = app.readinessChecks.head.preserveLastResponse
        )
      )
    }

    "readiness check specs for one task with a required port and one readiness check" in {
      val f = new Fixture

      Given("an app with a readiness check and a fixed port assignment")
      val app = f.appWithOneReadinessCheckWithRequiredPorts
      And("a task with two host ports")
      val task = f.taskWithPorts

      When("calculating the ReadinessCheckSpec")
      val specs = ReadinessCheckExecutor.ReadinessCheckSpec.readinessCheckSpecsForTask(app, task)

      Then("we get one spec")
      specs should have size 1
      val spec = specs.head
      And("it has the correct url")
      spec.url should equal(s"http://${f.hostName}:80/")
      And("the rest of the fields are correct, too")
      spec should equal(
        ReadinessCheckExecutor.ReadinessCheckSpec(
          url = s"http://${f.hostName}:80/",
          taskId = task.taskId,
          checkName = app.readinessChecks.head.name,
          interval = app.readinessChecks.head.interval,
          timeout = app.readinessChecks.head.timeout,
          httpStatusCodesForReady = app.readinessChecks.head.httpStatusCodesForReady,
          preserveLastResponse = app.readinessChecks.head.preserveLastResponse
        )
      )
    }

    "multiple readiness check specs" in {
      val f = new Fixture

      Given("an app with two readiness checks and randomly assigned ports")
      val app = f.appWithMultipleReadinessChecks
      And("a task with two host port")
      val task = f.taskWithPorts

      When("calculating the ReadinessCheckSpec")
      val specs = ReadinessCheckExecutor.ReadinessCheckSpec.readinessCheckSpecsForTask(app, task)

      Then("we get two specs in the right order")
      specs should have size 2
      val specDefaultHttp = specs.head
      val specAlternative = specs(1)
      specDefaultHttp.checkName should equal(app.readinessChecks.head.name)
      specAlternative.checkName should equal(app.readinessChecks(1).name)

      And("the default http spec has the right url")
      specDefaultHttp.url should equal(s"http://${f.hostName}:81/")

      And("the alternative https spec has the right url")
      specAlternative.url should equal(s"https://${f.hostName}:80/v1/plan")
    }
  }

  class Fixture {
    val appId: PathId = PathId("/test")
    val hostName = "some.host"

    val appWithOneReadinessCheck = AppDefinition(
      id = appId,
      readinessChecks = Seq(ReadinessCheckTestHelper.defaultHttp),
      portDefinitions = Seq(
        PortDefinition(
          port = AppDefinition.RandomPortValue,
          name = Some("http-api")
        )
      )
    )

    val appWithOneReadinessCheckWithRequiredPorts = AppDefinition(
      id = appId,
      readinessChecks = Seq(ReadinessCheckTestHelper.defaultHttp),
      requirePorts = true,
      portDefinitions = Seq(
        PortDefinition(
          port = 80,
          name = Some(ReadinessCheckTestHelper.defaultHttp.portName)
        ),
        PortDefinition(
          port = 81,
          name = Some("foo")
        )
      )
    )

    val appWithMultipleReadinessChecks = AppDefinition(
      id = appId,
      readinessChecks = Seq(
        ReadinessCheckTestHelper.defaultHttp,
        ReadinessCheckTestHelper.alternativeHttps
      ),
      portDefinitions = Seq(
        PortDefinition(
          port = 1, // service ports, ignore
          name = Some(ReadinessCheckTestHelper.alternativeHttps.portName)
        ),
        PortDefinition(
          port = 2, // service ports, ignore
          name = Some(ReadinessCheckTestHelper.defaultHttp.portName)
        )
      )
    )

    def taskWithPorts = {
      val task: Task.LaunchedEphemeral = TestTaskBuilder.Helper.runningTaskForApp(appId)
      task.copy(status = task.status.copy(networkInfo = NetworkInfo(hostName, hostPorts = Seq(80, 81), ipAddresses = Nil)))
    }
  }
}
