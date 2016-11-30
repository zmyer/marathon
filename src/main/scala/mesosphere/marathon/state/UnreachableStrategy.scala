package mesosphere.marathon
package state

import com.wix.accord._
import com.wix.accord.dsl._

import scala.concurrent.duration._
import mesosphere.marathon.Protos
import FiniteDuration._ // HACK: work around for "diverting implicit" compilation errors in IDEA

/**
  * Defines the time outs for unreachable tasks.
  */
case class UnreachableStrategy(
    inactiveAfter: FiniteDuration = UnreachableStrategy.DefaultInactiveAfter,
    expungeAfter: FiniteDuration = UnreachableStrategy.DefaultExpungeAfter) {

  def toProto: Protos.UnreachableStrategy =
    Protos.UnreachableStrategy.newBuilder.
      setExpungeAfterSeconds(expungeAfter.toSeconds).
      setInactiveAfterSeconds(inactiveAfter.toSeconds).
      build
}

object UnreachableStrategy {
  val DefaultInactiveAfter: FiniteDuration = 15.minutes
  val DefaultExpungeAfter: FiniteDuration = 7.days
  val MinInactiveAfter: FiniteDuration = 1.second
  val default = UnreachableStrategy()

  implicit val unreachableStrategyValidator: Validator[UnreachableStrategy] = validator[UnreachableStrategy] { strategy =>
    strategy.inactiveAfter should be >= MinInactiveAfter
    strategy.inactiveAfter should be < strategy.expungeAfter
  }

  def fromProto(unreachableStrategyProto: Protos.UnreachableStrategy): UnreachableStrategy = {
    UnreachableStrategy(
      inactiveAfter = unreachableStrategyProto.getInactiveAfterSeconds.seconds,
      expungeAfter = unreachableStrategyProto.getExpungeAfterSeconds.seconds)
  }
}
