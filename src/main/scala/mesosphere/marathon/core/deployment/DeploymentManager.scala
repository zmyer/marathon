package mesosphere.marathon.core.deployment

import akka.Done
import akka.actor.ActorRef
import mesosphere.marathon.Seq

import scala.concurrent.Future

trait DeploymentManager {

  def start(plan: DeploymentPlan, force: Boolean = false, origSender: ActorRef): Future[Done]

  def cancel(id: String): Future[Done]

  def list(): Future[Seq[DeploymentStepInfo]]
}
