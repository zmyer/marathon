package mesosphere.marathon
package state

import mesosphere.UnitTest
import com.wix.accord.scalatest.ResultMatchers

import scala.concurrent.duration._

class UnreachableStrategyTest extends UnitTest with ResultMatchers {

  def validate = UnreachableStrategy.unreachableStrategyValidator

  "UnreachableStrategy.unreachableStrategyValidator" should {
    "validate default strategy" in {
      val strategy = UnreachableStrategy()
      validate(strategy) shouldBe aSuccess
    }

    "validate with other parameters successfully" in {
      val strategy = UnreachableStrategy(13.minutes, 37.minutes)
      validate(strategy) shouldBe aSuccess
    }

    "fail with invalid time until inactive" in {
      val strategy = UnreachableStrategy(inactiveAfter = 0.second)
      validate(strategy) should failWith("inactiveAfter" -> "got 0 seconds, expected 1 second or more")
    }

    "fail when time until expunge is smaller" in {
      val strategy = UnreachableStrategy(inactiveAfter = 2.seconds, expungeAfter = 1.second)
      validate(strategy) should failWith("inactiveAfter" -> "got 2 seconds, expected less than 1 second")
    }

    "fail when time until expunge is equal to time until inactive" in {
      val strategy = UnreachableStrategy(inactiveAfter = 2.seconds, expungeAfter = 2.seconds)
      validate(strategy) should failWith("inactiveAfter" -> "got 2 seconds, expected less than 2 seconds")
    }
  }
}
