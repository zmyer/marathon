package mesosphere.marathon.core.matcher.base

import mesosphere.marathon.MarathonTestHelper
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.{PersistentVolumeInfo, PersistentVolume, PathId}
import mesosphere.marathon.tasks.ResourceUtil
import mesosphere.marathon.test.Mockito
import org.apache.mesos.{Protos => MesosProtos}
import org.scalatest.{FunSuite, GivenWhenThen, Matchers}

class ReserveAndCreateVolumesTest extends FunSuite with Mockito with GivenWhenThen with Matchers
{
  import scala.collection.JavaConverters._

  test("applyToOffer without volumes") {
    val f = new Fixture

    Given("a big offer")
    When("we apply an op without volumes")
    val op = OfferMatcher.ReserveAndCreateVolumes(
      taskResources = f.taskResources,
      localVolumes = Iterable.empty,
      oldTask = None,
      newTask = f.minimalTask.copy(reservationWithVolumes = Some(Task.ReservationWithVolumes(Iterable.empty)))
    )
    val resultingOffer = op.applyToOffer(f.bigOffer)

    Then("we deduct the task resource only")
    resultingOffer should equal(ResourceUtil.consumeResourcesFromOffer(f.bigOffer, f.taskResources))
  }

  test("applyToOffer with volumes") {
    val f = new Fixture

    Given("a big offer")
    When("we apply an op with volumes")
    val volume1 = f.volumeForSize(1*f.GB)
    val volume2 = f.volumeForSize(2*f.GB)
    val volumesForOp = Iterable(volume1, volume2)

    val op = OfferMatcher.ReserveAndCreateVolumes(
      taskResources = f.taskResources,
      localVolumes = volumesForOp,
      oldTask = None,
      newTask = f.minimalTask.copy(
        reservationWithVolumes = Some(Task.ReservationWithVolumes(volumesForOp.map(_.volumeId)))
      )
    )
    val resultingOffer = op.applyToOffer(f.bigOffer)

    Then("we deduct the task resource and volume resources")
    val offer1 = ResourceUtil.consumeResourcesFromOffer(f.bigOffer, f.taskResources)
    val offer2 = ResourceUtil.consumeResourcesFromOffer(offer1, volume1.resources)
    val offer3 = ResourceUtil.consumeResourcesFromOffer(offer2, volume2.resources)
    resultingOffer should equal(offer3)
  }

  // FIXME: Test construct operations...
  // FIXME: Test OfferOperations code

  class Fixture {
    final val GB = 1024L * 1024L * 1024L

    val appId = PathId("/test")
    val minimalTask = MarathonTestHelper.mininimalTask(appId)

    val bigOffer = MarathonTestHelper.makeBasicOffer(
      cpus = 16,
      mem = 8192,
      disk = 10*GB.toDouble,
      beginPort = 1000, endPort = 2000,
      role = "*"
    ).build()

    val taskResources = MarathonTestHelper.makeBasicOffer(
      cpus = 4, mem = 1024, disk = 1*GB,
      beginPort = 1000, endPort = 1001
    ).getResourcesList.asScala

    def volumeForSize(size: Long): OfferMatcher.ReserveAndCreateVolumes.LocalVolumeInfo = {
      OfferMatcher.ReserveAndCreateVolumes.LocalVolumeInfo(
        "principal",
        role = "*",
        volumeId = Task.LocalVolumeId("volumeId"),
        volumeConfig = PersistentVolume(
          containerPath = "/some/path",
          persistent = PersistentVolumeInfo(size),
          mode = MesosProtos.Volume.Mode.RW
        )
      )
    }

    // FIXME, remove?
    private[this] def scalar(name: String, value: Double, role: String = "*"): MesosProtos.Resource = {
      val scalar = MesosProtos.Value.Scalar.newBuilder().setValue(value)
      MesosProtos.Resource
        .newBuilder()
        .setName(name)
        .setType(MesosProtos.Value.Type.SCALAR)
        .setScalar(scalar)
        .build()
    }
  }
}
