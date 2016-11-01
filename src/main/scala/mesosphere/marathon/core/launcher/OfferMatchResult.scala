package mesosphere.marathon.core.launcher

import mesosphere.marathon.state.RunSpec
import mesosphere.mesos.NoOfferMatchReason
import org.apache.mesos.{ Protos => Mesos }

/**
  * Defines the offer match result based on the related offer for given runSpec.
  */
sealed trait OfferMatchResult {

  /**
    * Related RunSpec.
    */
  def runSpec: RunSpec

  /**
    * Related Mesos Offer.
    */
  def offer: Mesos.Offer
}

object OfferMatchResult {

  case class Match(runSpec: RunSpec, offer: Mesos.Offer, instanceOp: InstanceOp) extends OfferMatchResult

  case class NoMatch(runSpec: RunSpec, offer: Mesos.Offer, reasons: Seq[NoOfferMatchReason]) extends OfferMatchResult

}

