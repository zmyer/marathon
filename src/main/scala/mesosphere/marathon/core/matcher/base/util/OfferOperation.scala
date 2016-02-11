package mesosphere.marathon.core.matcher.base.util

import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.task.Task.LocalVolumeId
import mesosphere.marathon.state.PersistentVolume
import org.apache.mesos.Protos
import org.apache.mesos.Protos.Offer

/**
  * Helper methods for creating operations on offers.
  */
private[base] object OfferOperation {

  /** Create a launch operation for the given taskInfo. */
  def launch(taskInfo: Protos.TaskInfo): Offer.Operation = {
    val launch = Offer.Operation.Launch.newBuilder()
      .addTaskInfos(taskInfo)
      .build()

    Offer.Operation.newBuilder()
      .setType(Protos.Offer.Operation.Type.LAUNCH)
      .setLaunch(launch)
      .build()
  }

  def reserve(resources: Iterable[Protos.Resource]): Offer.Operation = {
    import scala.collection.JavaConverters._
    val reserve = Offer.Operation.Reserve.newBuilder()
      .addAllResources(resources.asJava)
      .build()

    Offer.Operation.newBuilder()
      .setType(Protos.Offer.Operation.Type.RESERVE)
      .setReserve(reserve)
      .build()
  }

  def createVolume(localVolume: OfferMatcher.ReserveAndCreateVolumes.LocalVolumeInfo): Offer.Operation = {
    val disk = {
      val persistence = Protos.Resource.DiskInfo.Persistence.newBuilder()
        .setId(localVolume.volumeId.idString)
        .build()

      val volume = Protos.Volume.newBuilder()
      .setContainerPath(localVolume.volumeConfig.containerPath)
      .setMode(localVolume.volumeConfig.mode)
      .build()

      Protos.Resource.DiskInfo.newBuilder()
      .setPersistence(persistence)
      .setVolume(volume)
      .build()
    }

    val resource = Protos.Resource.newBuilder()
    .setName("disk")
    .setType(Protos.Value.Type.SCALAR)
    .setScalar(Protos.Value.Scalar.newBuilder().setValue(localVolume.volumeConfig.persistent.size.toDouble).build())
    .setRole(localVolume.role)
    .setReservation(Protos.Resource.ReservationInfo.newBuilder().setPrincipal(localVolume.principal).build())
    .setDisk(disk)

    val create = Offer.Operation.Create.newBuilder().addVolumes(resource).build()

    Offer.Operation.newBuilder()
    .setType(Protos.Offer.Operation.Type.CREATE)
    .setCreate(create)
    .build()
  }
}
