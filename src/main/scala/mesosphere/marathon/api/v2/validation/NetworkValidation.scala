package mesosphere.marathon
package api.v2.validation

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation
import mesosphere.marathon.core.pod
import mesosphere.marathon.raml.{ Network, NetworkMode }

@SuppressWarnings(Array("all")) // wix breaks stuff
trait NetworkValidation {
  import Validation._

  /** changes here should be reflected in [[modelNetworksValidator]] */
  implicit val ramlNetworksValidator: Validator[Seq[Network]] =
    isTrue[Seq[Network]]("Host networks may not have names or labels") { nets =>
      !nets.filter(_.mode == NetworkMode.Host).exists { n =>
        val hasName = n.name.fold(false){ _.nonEmpty }
        val hasLabels = n.labels.nonEmpty
        hasName || hasLabels
      }
    } and isTrue[Seq[Network]]("Bridge networks may not have names") { nets =>
      !nets.filter(_.mode == NetworkMode.ContainerBridge).exists { n =>
        val hasName = n.name.fold(false){ _.nonEmpty }
        hasName
      }
    } and isTrue[Seq[Network]]("Duplicate networks are not allowed") { nets =>
      // unnamed CT nets pick up the default virtual net name
      val unnamedAtMostOnce = nets.count { n => n.name.isEmpty && n.mode == NetworkMode.Container } < 2
      val realNamesAtMostOnce: Boolean = !nets.flatMap(_.name).groupBy(name => name).exists(_._2.size > 1)
      unnamedAtMostOnce && realNamesAtMostOnce
    } and isTrue[Seq[Network]]("May specify a single host network, single bridge network, or else 1-to-n container networks") { nets =>
      val countsByMode = nets.groupBy { net => net.mode }.map { case (mode, networks) => mode -> networks.size }
      val hostNetworks = countsByMode.getOrElse(NetworkMode.Host, 0)
      val bridgeNetworks = countsByMode.getOrElse(NetworkMode.ContainerBridge, 0)
      val containerNetworks = countsByMode.getOrElse(NetworkMode.Container, 0)
      (hostNetworks <= 1 && bridgeNetworks == 0 && containerNetworks == 0) ||
        (hostNetworks == 0 && bridgeNetworks <= 1 && containerNetworks == 0) ||
        (hostNetworks == 0 && bridgeNetworks == 0 && containerNetworks > 0)
    }

  /** changes here should be reflected in [[ramlNetworksValidator]], except where the results are expected to have already been normalized for the model */
  implicit val modelNetworksValidator: Validator[Seq[pod.Network]] =
    isTrue[Seq[pod.Network]]("Duplicate networks are not allowed") { nets =>
      // unnamed CT nets should have already picked up the default virtual net name
      val realNamesAtMostOnce: Boolean = !nets.collect {
        case ct: pod.ContainerNetwork => ct.name
      }.groupBy(name => name).exists(_._2.size > 1)
      realNamesAtMostOnce
    } and isTrue[Seq[pod.Network]]("Must specify either a single host network, single bridge network, or else 1-to-n container networks") { nets =>
      val countsByMode = nets.groupBy {
        case pod.HostNetwork => "h"
        case _: pod.BridgeNetwork => "b"
        case _: pod.ContainerNetwork => "c"
      }.mapValues(_.size)
      val hostNetworks = countsByMode.getOrElse("h", 0)
      val bridgeNetworks = countsByMode.getOrElse("b", 0)
      val containerNetworks = countsByMode.getOrElse("c", 0)
      (hostNetworks == 1 && bridgeNetworks == 0 && containerNetworks == 0) ||
        (hostNetworks == 0 && bridgeNetworks == 1 && containerNetworks == 0) ||
        (hostNetworks == 0 && bridgeNetworks == 0 && containerNetworks > 0)
    }

  def defaultNetworkNameValidator(defaultNetworkName: () => Option[String]): Validator[Seq[Network]] =
    isTrue(NetworkValidationMessages.NetworkNameMustBeSpecified) { n =>
      n.forall(c => c.mode != NetworkMode.Container || c.name.isDefined || defaultNetworkName().isDefined)
    }
}

object NetworkValidation extends NetworkValidation

object NetworkValidationMessages {
  val NetworkNameMustBeSpecified = "Network name must be specified when container network is selected."
}
