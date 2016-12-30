package mesosphere.marathon
package core.election.impl

import akka.event.EventStream
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.base.{ RichRuntime, ShutdownHooks }
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.test.{ ExitDisabledTest, Mockito }
import mesosphere.marathon.{ UnstableTest, ZookeeperConfig }
import org.rogach.scallop.ScallopOption

import scala.concurrent.duration._

@UnstableTest
class CuratorElectionServiceTest extends AkkaUnitTest with Mockito with ExitDisabledTest {

  def scallopOption[A](a: Option[A]): ScallopOption[A] = {
    new ScallopOption[A]("") {
      override def get = a
      override def apply() = a.get
    }
  }

  "The CuratorElectionService" when {
    implicit val eventStream: EventStream = mock[EventStream]
    implicit val metrics: Metrics = mock[Metrics]
    val hostPort = "80"
    implicit val backoff: ExponentialBackoff = new ExponentialBackoff(0.01.seconds, 0.1.seconds)
    implicit val shutdownHooks: ShutdownHooks = mock[ShutdownHooks]

    val service = new CuratorElectionService(ZookeeperConfig(
      url = "zk://unresolvable:8080/marathon",
      10.seconds, 10.milliseconds), hostPort)

    "given an unresolvable hostname" should {
      "shut Marathon down on a NonFatal" in {
        service.offerLeadershipImpl()

        exitCalled(RichRuntime.FatalErrorSignal).futureValue should be(true)
      }
    }
  }
}
