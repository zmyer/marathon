package mesosphere.marathon
package integration

import mesosphere.AkkaIntegrationTest
import mesosphere.marathon.integration.setup._
import mesosphere.marathon.raml.App
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.Timestamp

import scala.concurrent.duration._

@IntegrationTest
class GracefulTaskKillIntegrationTest extends AkkaIntegrationTest with EmbeddedMarathonTest {

  before {
    cleanUp()
  }

  // this command simulates a 'long terminating' application
  // note: Integration test does not interpret symbolic names (SIGTERM=15), therefore signal 15 is used.
  val taskKillGraceDuration = 10
  val taskKillGracePeriod = taskKillGraceDuration.seconds
  val appCommand: String = s"""trap \"sleep ${taskKillGraceDuration + 1}\" 15 && sleep 100000"""

  "GracefulTaskKilling" should {
    "create a 'long terminating' app with custom taskKillGracePeriod duration" in {
      Given("a new 'long terminating' app with taskKillGracePeriod set to 10 seconds")
      val app = App(
        (testBasePath / "app-stopped-with-sigkill").toString,
        cmd = Some(appCommand),
        taskKillGracePeriodSeconds = Some(taskKillGracePeriod.toSeconds.toInt))

      When("The app is deployed")
      val result = marathon.createAppV2(app)

      Then("The app is created")
      result should be(Created)
      waitForDeployment(result)
      waitForTasks(app.id.toPath, 1)
      //make sure, the app has really started
      val taskId = marathon.tasks(app.id.toPath).value.head.id

      When("a task of an app is killed")
      marathon.killTask(app.id.toPath, taskId, scale = true) should be(OK)
      val taskKillSentTimestamp = Timestamp.now()

      val taskKilledEvent = waitForEventWith(
        "status_update_event",
        _.info("taskStatus") == "TASK_KILLED")

      val taskKilledReceivedTimestamp = Timestamp(taskKilledEvent.info("timestamp").toString)
      val waitedForTaskKilledEvent = taskKillSentTimestamp until taskKilledReceivedTimestamp

      // the task_killed event should occur at least 10 seconds after sending it
      waitedForTaskKilledEvent.toMillis should be >= taskKillGracePeriod.toMillis
    }

    "create a 'short terminating' app with custom taskKillGracePeriod duration" in {
      Given("a new 'short terminating' app with taskKillGracePeriod set to 10 seconds")
      val app = App(
        (testBasePath / "app-stopped-with-sigterm").toString,
        cmd = Some("sleep 100000"),
        taskKillGracePeriodSeconds = Some(taskKillGracePeriod.toSeconds.toInt))

      When("The app is deployed")
      val result = marathon.createAppV2(app)

      Then("The app is created")
      result should be(Created)
      waitForDeployment(result)
      waitForTasks(app.id.toPath, 1)
      //make sure, the app has really started
      val taskId = marathon.tasks(app.id.toPath).value.head.id

      When("a task of an app is killed")
      marathon.killTask(app.id.toPath, taskId, scale = true) should be(OK)
      val taskKillSentTimestamp = Timestamp.now()

      val taskKilledEvent = waitForEventWith(
        "status_update_event",
        _.info("taskStatus") == "TASK_KILLED")

      val taskKilledReceivedTimestamp = Timestamp(taskKilledEvent.info("timestamp").toString)
      val waitedForTaskKilledEvent = taskKillSentTimestamp until taskKilledReceivedTimestamp

      // the task_killed event should occur instantly or at least smaller as taskKillGracePeriod,
      // because the app terminates shortly
      waitedForTaskKilledEvent.toMillis should be < taskKillGracePeriod.toMillis withClue "the task kill event took longer than the task kill grace period"
    }
  }
}
