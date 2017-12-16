package mesosphere.marathon
package integration

import mesosphere.AkkaIntegrationTest
import mesosphere.marathon.integration.facades.ITEnrichedTask
import mesosphere.marathon.integration.facades.MarathonFacade._
import mesosphere.marathon.integration.facades.MesosFacade.{ ITMesosState, ITResources }
import mesosphere.marathon.integration.setup.{ EmbeddedMarathonTest, RestResult }
import mesosphere.marathon.raml.{ App, AppPersistentVolume, AppResidency, AppUpdate, AppVolume, Container, EngineType, PersistentVolume, PortDefinition, ReadMode, UnreachableDisabled, UpgradeStrategy }
import mesosphere.marathon.state.PathId

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util.Try

@IntegrationTest
class ResidentTaskIntegrationTest extends AkkaIntegrationTest with EmbeddedMarathonTest {

  import Fixture._

  "ResidentTaskIntegrationTest" should {
    "resident task can be deployed and write to persistent volume" in new Fixture {
      Given("An app that writes into a persistent volume")
      val containerPath = "persistent-volume"
      val app = residentApp(
        id = appId("resident-task-can-be-deployed-and-write-to-persistent-volume"),
        containerPath = containerPath,
        cmd = s"""echo "data" > $containerPath/data""")

      When("A task is launched")
      val result = createAsynchronously(app)

      Then("It writes successfully to the persistent volume and finishes")
      waitForStatusUpdates(StatusUpdate.TASK_RUNNING)
      waitForDeployment(result)
      waitForStatusUpdates(StatusUpdate.TASK_FINISHED)
    }

    "resident task can be deployed along with constraints" in new Fixture {
      // background: Reserved tasks may not be considered while making sure constraints are met, because they
      // would prevent launching a task because there `is` already a task (although not launched)
      Given("A resident app that uses a hostname:UNIQUE constraints")
      val containerPath = "persistent-volume"
      val unique = raml.Constraints("hostname" -> "UNIQUE")

      val app = residentApp(
        id = appId("resident-task-that-uses-hostname-unique"),
        containerPath = containerPath,
        cmd = """sleep 1""",
        constraints = unique)

      When("A task is launched")
      val result = createAsynchronously(app)

      Then("It it successfully launched")
      waitForStatusUpdates(StatusUpdate.TASK_RUNNING)
      waitForDeployment(result)
    }

    "persistent volume will be re-attached and keep state" in new Fixture {
      Given("An app that writes into a persistent volume")
      val containerPath = "persistent-volume"
      val app = residentApp(
        id = appId("resident-task-with-persistent-volumen-will-be-reattached-and-keep-state"),
        containerPath = containerPath,
        cmd = s"""echo data > $containerPath/data && sleep 1000""")

      When("a task is launched")
      val result = createAsynchronously(app)

      Then("it successfully writes to the persistent volume and then finishes")
      waitForStatusUpdates(StatusUpdate.TASK_RUNNING)
      waitForDeployment(result)

      When("the app is suspended")
      suspendSuccessfully(PathId(app.id))

      And("a new task is started that checks for the previously written file")
      // deploy a new version that checks for the data written the above step
      val update = marathon.updateApp(
        PathId(app.id),
        AppUpdate(
          instances = Some(1),
          cmd = Some(s"""test -e $containerPath/data && sleep 2""")
        )
      )
      update.code shouldBe 200
      // we do not wait for the deployment to finish here to get the task events

      waitForStatusUpdates(StatusUpdate.TASK_RUNNING)
      waitForDeployment(update)
      waitForStatusUpdates(StatusUpdate.TASK_FINISHED)
    }

    "resident task is launched completely on reserved resources" in new Fixture {
      Given("A clean state of the cluster since we check reserved resources")
      cleanUp()

      And("A resident app")
      val app = residentApp(
        id = appId("resident-task-is-launched-completely-on-reserved-resources"))

      When("A task is launched")
      createSuccessfully(app)

      Then("used and reserved resources correspond to the app")
      val state: RestResult[ITMesosState] = mesos.state

      withClue("used_resources") {
        state.value.agents.head.usedResources should equal(itMesosResources)
      }
      withClue("reserved_resources") {
        state.value.agents.head.reservedResourcesByRole.get("foo") should equal(Some(itMesosResources))
      }

      When("the app is suspended")
      suspendSuccessfully(PathId(app.id))

      Then("there are no used resources anymore but there are the same reserved resources")
      val state2: RestResult[ITMesosState] = mesos.state

      withClue("used_resources") {
        state2.value.agents.head.usedResources should be(empty)
      }
      withClue("reserved_resources") {
        state2.value.agents.head.reservedResourcesByRole.get("foo") should equal(Some(itMesosResources))
      }

      // we check for a blank slate of mesos reservations after each test
      // TODO: Once we wait for the unreserves before finishing the StopApplication deployment step,
      // we should test that here
    }

    "Scale Up" in new Fixture {
      Given("A resident app with 0 instances")
      val app = createSuccessfully(residentApp(
        id = appId("scale-up-resident-app-with-zero-instances"),
        instances = 0))

      When("We scale up to 5 instances")
      scaleToSuccessfully(PathId(app.id), 5)

      Then("exactly 5 tasks have been created")
      launchedTasks(PathId(app.id)).size shouldBe 5
    }

    "Scale Down" in new Fixture {
      Given("a resident app with 5 instances")
      val app = createSuccessfully(residentApp(
        id = appId("scale-down-resident-app-with-five-instances"),
        instances = 5))

      When("we scale down to 0 instances")
      suspendSuccessfully(PathId(app.id))

      Then("all tasks are suspended")
      val all = allTasks(PathId(app.id))
      all.size shouldBe 5
      all.count(_.launched) shouldBe 0
      all.count(_.suspended) shouldBe 5
    }

    "Restart" in new Fixture {
      Given("a resident app with 5 instances")
      val app = createSuccessfully(
        residentApp(
          id = appId("restart-resident-app-with-five-instances"),
          instances = 5
        )
      )

      When("we restart the app")
      val newVersion = restartSuccessfully(app) withClue ("The app did not restart.")
      val all = allTasks(PathId(app.id))

      logger.info("tasks after relaunch: {}", all.mkString(";"))

      Then("no extra task was created")
      all.size shouldBe 5

      And("exactly 5 instances are running")
      all.count(_.launched) shouldBe 5

      And("all 5 tasks are restarted and of the new version")
      all.map(_.version).forall(_.contains(newVersion)) shouldBe true
    }

    "Config Change" in new Fixture {
      Given("a resident app with 5 instances")
      val app = createSuccessfully(
        residentApp(
          id = appId("config-change-resident-app-with-five-instances"),
          instances = 5
        )
      )

      When("we change the config")
      val newVersion = updateSuccessfully(PathId(app.id), AppUpdate(cmd = Some("sleep 1234"))).toString
      val all = allTasks(PathId(app.id))

      logger.info("tasks after config change: {}", all.mkString(";"))

      Then("no extra task was created")
      all should have size 5

      And("exactly 5 instances are running")
      all.filter(_.launched) should have size 5

      And("all 5 tasks are of the new version")
      all.map(_.version).forall(_.contains(newVersion)) shouldBe true
    }
  }

  class Fixture {

    val cpus: Double = 0.001
    val mem: Double = 1.0
    val disk: Double = 1.0
    val gpus: Double = 0.0
    val persistentVolumeSize = 2L

    val itMesosResources = ITResources(
      "mem" -> mem,
      "cpus" -> cpus,
      "disk" -> (disk + persistentVolumeSize),
      "gpus" -> gpus
    )

    def appId(suffix: String): PathId = PathId(s"/$testBasePath/app-$suffix")

    def residentApp(
      id: PathId = PathId(s"/$testBasePath/app-${IdGenerator.generate()}"),
      containerPath: String = "persistent-volume",
      cmd: String = "sleep 1000",
      instances: Int = 1,
      backoffDuration: FiniteDuration = 1.hour,
      portDefinitions: Seq[PortDefinition] = Seq.empty, /* prevent problems by randomized port assignment */
      constraints: Set[Seq[String]] = Set.empty): App = {

      val persistentVolume: AppVolume = AppPersistentVolume(
        containerPath = containerPath,
        persistent = PersistentVolume(size = persistentVolumeSize),
        mode = ReadMode.Rw
      )

      val app = App(
        id.toString,
        instances = instances,
        residency = Some(AppResidency()),
        constraints = constraints,
        container = Some(Container(
          `type` = EngineType.Mesos,
          volumes = Seq(persistentVolume)
        )),
        cmd = Some(cmd),
        // cpus, mem and disk are really small because otherwise we'll soon run out of reservable resources
        cpus = cpus,
        mem = mem,
        disk = disk,
        portDefinitions = Some(portDefinitions),
        backoffSeconds = backoffDuration.toSeconds.toInt,
        upgradeStrategy = Some(UpgradeStrategy(minimumHealthCapacity = 0.5, maximumOverCapacity = 0.0)),
        unreachableStrategy = Some(UnreachableDisabled.DefaultValue)
      )

      app
    }

    def createSuccessfully(app: App): App = {
      waitForDeployment(createAsynchronously(app))
      app
    }

    def createAsynchronously(app: App): RestResult[App] = {
      val result = marathon.createAppV2(app)
      result should be(Created)
      extractDeploymentIds(result) should have size 1
      result
    }

    def scaleToSuccessfully(appId: PathId, instances: Int): Seq[ITEnrichedTask] = {
      val result = marathon.updateApp(appId, AppUpdate(instances = Some(instances)))
      result should be(OK)
      waitForDeployment(result)
      waitForTasks(appId, instances)
    }

    def suspendSuccessfully(appId: PathId): Seq[ITEnrichedTask] = scaleToSuccessfully(appId, 0)

    def updateSuccessfully(appId: PathId, update: AppUpdate): VersionString = {
      val result = marathon.updateApp(appId, update)
      result should be(OK)
      waitForDeployment(result)
      result.value.version.toString
    }

    def restartSuccessfully(app: App): VersionString = {
      val result = marathon.restartApp(PathId(app.id))
      result should be(OK)
      waitForDeployment(result)
      result.value.version.toString
    }

    def allTasks(appId: PathId): Seq[ITEnrichedTask] = {
      Try(marathon.tasks(appId)).map(_.value).getOrElse(Nil)
    }

    def launchedTasks(appId: PathId): Seq[ITEnrichedTask] = allTasks(appId).filter(_.launched)

    def suspendedTasks(appId: PathId): Seq[ITEnrichedTask] = allTasks(appId).filter(_.suspended)
  }

  object Fixture {
    type VersionString = String

    object StatusUpdate {
      val TASK_FINISHED = "TASK_FINISHED"
      val TASK_RUNNING = "TASK_RUNNING"
      val TASK_FAILED = "TASK_FAILED"
    }

    /**
      * Resident Tasks reside in the TaskTracker even after they terminate and after the associated app is deleted.
      * To prevent spurious state in the above test cases, each test case should use a unique appId.
      */
    object IdGenerator {
      private[this] var index: Int = 0
      def generate(): String = {
        index += 1
        index.toString
      }
    }
  }
}
