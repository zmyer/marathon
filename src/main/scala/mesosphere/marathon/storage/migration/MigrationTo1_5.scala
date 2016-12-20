package mesosphere.marathon
package storage.migration

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.google.protobuf.MessageOrBuilder
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.Protos._
import mesosphere.marathon.stream._
import mesosphere.marathon.api.serialization.ContainerSerializer
import mesosphere.marathon.state.{ AppDefinition, PathId, Timestamp }
import mesosphere.marathon.storage.repository.GroupRepository

import scala.async.Async.{ async, await }
import scala.concurrent.{ ExecutionContext, Future }
import org.apache.mesos.{ Protos => mesos }

case class MigrationTo1_5(
    migration: Migration)(implicit
  executionContext: ExecutionContext,
    materializer: Materializer) extends StrictLogging {

  import MigrationTo1_5._

  def migrate(): Future[Done] = async {
    val summary = await(migrateGroups(migration.serviceDefinitionRepo, migration.groupRepository))
    logger.info(s"Migrated $summary to 1.5")
    Done
  }

  def migrateGroups(serviceRepository: ServiceDefinitionRepository, groupRepository: GroupRepository): Future[(String, Int)] = async {
    val result: Future[(String, Int)] = groupRepository.rootVersions().mapAsync(Int.MaxValue) { version =>
      groupRepository.rootVersion(version)
    }.collect {
      case Some(root) => root
    }.concat { Source.fromFuture(groupRepository.root()) }.mapAsync(1) { root =>
      // store roots one at a time with current root last
      val appIds = root.transitiveApps.map(app => app.id -> app.version)
      migrateApps(appIds.to[Seq], serviceRepository).flatMap { apps =>
        groupRepository.storeRoot(root, apps, Nil, Nil, Nil).map(_ => apps.size)
      }
    }.runFold(0) { case (acc, apps) => acc + apps + 1 }.map("root + app versions" -> _)
    await(result)
  }

  def migrateApps(ids: Seq[(PathId, Timestamp)], serviceRepository: ServiceDefinitionRepository): Future[Seq[AppDefinition]] =
    async {
      // 1. read raw app protobuf
      // 2. convert/migrate from old proto fields to new
      // 3. convert app proto to AppDefinition
      val rawApps: Seq[Option[ServiceDefinition]] = await(Future.sequence(ids.map {
        case (id, version) =>
          serviceRepository.getVersion(id, version.toOffsetDateTime)
      }))
      rawApps.flatten.map { service =>
        AppDefinition.fromProto(migrateApp(service))
      }
    }

  def migrateApp(service: ServiceDefinition): ServiceDefinition = {
    val network = migrateNetworks(service, migration.defaultNetworkName)
    val prototypeContainer = service.map(_.hasContainer)(_.getContainer).getOrElse(EmptyMesosContainer)
    val containerWithPortMappings =
      migrateDockerPortMappings(
        migrateIpDiscovery(
          prototypeContainer,
          service.flat(_.hasOBSOLETEIpAddress)(_.getOBSOLETEIpAddress.map(_.hasDiscoveryInfo)(_.getDiscoveryInfo))
        )
      )

    val builder = service.toBuilder.addNetworks(network)

    // kill portDefinitions for non-host networking
    if (network.getMode != NetworkDefinition.Mode.HOST) {
      builder.clearPortDefinitions()
    }

    if (containerWithPortMappings != prototypeContainer) {
      builder.setContainer(containerWithPortMappings)
    }

    builder.build
  }

  /**
    * see related normalization code in [[mesosphere.marathon.api.v2.AppNormalization]]
    */
  def migrateDockerPortMappings(container: ExtendedContainerInfo): ExtendedContainerInfo = {
    import mesos.ContainerInfo.Type._
    require(container.getPortMappingsCount == 0, "port mappings are new in 1.5, they shouldn't exist here yet")
    container.map(c => c.hasDocker && c.getType == DOCKER)(_.getDocker.getOBSOLETEPortMappingsList).map { ports =>
      container.toBuilder.addAllPortMappings(ports.map { port =>
        val builder = ExtendedContainerInfo.PortMapping.newBuilder
          .setContainerPort(port.getContainerPort)
          .addAllLabels(port.getLabelsList)
        port.map(_.hasHostPort)(_.getHostPort).foreach(builder.setHostPort)
        port.map(_.hasServicePort)(_.getServicePort).foreach(builder.setServicePort)
        port.map(_.hasName)(_.getName).foreach(builder.setName)
        port.map(_.hasProtocol)(_.getProtocol).foreach(builder.setProtocol)
        builder.build
      }).build
    }.getOrElse(container)
  }

  /**
    * see related normalization code in [[mesosphere.marathon.api.v2.AppNormalization]]
    */
  def migrateIpDiscovery(container: ExtendedContainerInfo, maybeDiscovery: Option[DiscoveryInfo]): ExtendedContainerInfo = {
    import mesos.ContainerInfo.Type._
    require(container.getPortMappingsCount == 0, "port mappings are new in 1.5, they shouldn't exist here yet")
    val containerType = container.map(_.hasType)(_.getType).getOrElse(MESOS)
    (containerType, maybeDiscovery) match {
      case (MESOS, Some(discovery)) =>
        val portMappings = discovery.getPortsList.map { port =>
          val builder = ExtendedContainerInfo.PortMapping.newBuilder()
            .setContainerPort(port.getNumber)
          port.map(_.hasName)(_.getName).foreach(builder.setName)
          port.map(_.hasProtocol)(_.getProtocol).foreach(builder.setProtocol)
          // the old IP/CT api didn't let users map discovery ports to host ports
          builder.build
        }
        container.toBuilder.addAllPortMappings(portMappings).build
      case (t, Some(discovery)) if discovery.getPortsCount > 0 =>
        throw SerializationFailedException(s"ipAddress.discovery.ports do not apply for container type $t")
      case _ =>
        container
    }
  }

  /**
    * see related normalization code in [[mesosphere.marathon.api.v2.AppNormalization]]
    */
  def migrateNetworks(service: ServiceDefinition, defaultNetworkName: Option[String]): NetworkDefinition = {
    def migrateUnnamedContainerNetworkName: String =
      defaultNetworkName.orElse(sys.env.get(DefaultNetworkNameForMigratedApps)).getOrElse(
        throw SerializationFailedException(
          s"failed to migrate service ${service.getId} because no default-network-name has been configured and" +
            s" environment variable $DefaultNetworkNameForMigratedApps is not set")
      )

    def containerNetworking(ipaddr: Protos.IpAddress) =
      NetworkDefinition.newBuilder
        .setMode(NetworkDefinition.Mode.BRIDGE)
        .addAllLabels(ipaddr.getLabelsList)
        .setName(ipaddr.map(_.hasNetworkName)(_.getNetworkName).getOrElse(migrateUnnamedContainerNetworkName))
        .build

    def bridgeNetworking(ipaddr: Protos.IpAddress) =
      NetworkDefinition.newBuilder
        .setMode(NetworkDefinition.Mode.BRIDGE)
        .addAllLabels(ipaddr.getLabelsList)
        .build

    def hostNetworking =
      NetworkDefinition.newBuilder.setMode(NetworkDefinition.Mode.HOST).build

    val ipAddress = service.map(_.hasOBSOLETEIpAddress)(_.getOBSOLETEIpAddress)
    val dockerNetwork = service.flat(_.hasContainer)(_.getContainer.flat(_.hasDocker)(_.getDocker.map(_.hasOBSOLETENetwork)(_.getOBSOLETENetwork)))
    (ipAddress, dockerNetwork) match {
      // wants ip/ct with a specific network mode
      case (Some(ipaddr), Some(network)) =>
        import org.apache.mesos.Protos.ContainerInfo.DockerInfo.Network._
        network match {
          case HOST => hostNetworking
          case BRIDGE => bridgeNetworking(ipaddr)
          case USER => containerNetworking(ipaddr)
          case unsupported =>
            throw SerializationFailedException(s"unsupported docker network type $unsupported")
        }
      // wants ip/ct with some network mode.
      // if the user gave us a name try to figure out what they want.
      case (Some(ipaddr), None) =>
        ipaddr.map(_.hasNetworkName)(_.getNetworkName) match {
          case Some(name) if name == ContainerSerializer.MesosBridgeName => // users shouldn't do this, but we're tolerant
            bridgeNetworking(ipaddr)
          case _ =>
            containerNetworking(ipaddr)
        }
      // user didn't ask for IP-per-CT, but specified a network type anyway
      case (None, Some(network)) =>
        import org.apache.mesos.Protos.ContainerInfo.DockerInfo.Network._
        network match {
          case HOST => hostNetworking
          case BRIDGE => bridgeNetworking(Protos.IpAddress.getDefaultInstance)
          case USER => containerNetworking(Protos.IpAddress.getDefaultInstance)
          case unsupported =>
            throw SerializationFailedException(s"unsupported docker network type $unsupported")
        }
      // no deprecated APIs used! awesome, so default to host networking
      case (None, None) =>
        hostNetworking
    }
  }
}

object MigrationTo1_5 {
  private[migration] val DefaultNetworkNameForMigratedApps = "MIGRATION_1_5_0_MARATHON_DEFAULT_NETWORK_NAME"
  private[migration] val EmptyMesosContainer = ExtendedContainerInfo.newBuilder().setType(mesos.ContainerInfo.Type.MESOS).build

  // stupid helpers because dealing with protobufs is tedious
  implicit class ProtoMappers[T <: MessageOrBuilder](t: T) {
    def map[A](b: T => Boolean)(f: T => A): Option[A] = if (b(t)) Some(f(t)) else None
    def flat[A](b: T => Boolean)(f: T => Option[A]): Option[A] = if (b(t)) f(t) else None
  }
}
