package mesosphere.marathon.core.deployment.impl

import akka.Done
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.MarathonSchedulerActor.RetrieveRunningDeployments
import mesosphere.marathon.Seq
import mesosphere.marathon.core.deployment.impl.DeploymentManagerActor.{ CancelDeployment, StartDeployment }
import mesosphere.marathon.core.deployment.{ DeploymentConfig, DeploymentManager, DeploymentPlan, DeploymentStepInfo }

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

private[deployment] class DeploymentManagerDelegate(
    config: DeploymentConfig,
    deploymentManagerActor: ActorRef) extends DeploymentManager with StrictLogging {

  val requestTimeout: Timeout = config.deploymentManagerRequestTimeout().milliseconds

  override def start(plan: DeploymentPlan, force: Boolean, origSender: ActorRef): Future[Done] =
    askActorFuture[StartDeployment, Done]("start")(StartDeployment(plan, origSender, force))

  override def cancel(id: String): Future[Done] =
    askActorFuture[CancelDeployment, Done]("cancel")(CancelDeployment(id))

  override def list(): Future[Seq[DeploymentStepInfo]] =
    askActorFuture[RetrieveRunningDeployments.type, Seq[DeploymentStepInfo]]("list")(RetrieveRunningDeployments)

  private[this] def askActorFuture[T, R: ClassTag](
    method: String,
    timeout: Timeout = requestTimeout)(message: T): Future[R] = {

    implicit val timeoutImplicit: Timeout = timeout
    val answerFuture = (deploymentManagerActor ? message).mapTo[Future[R]]

    import scala.concurrent.ExecutionContext.Implicits.global
    answerFuture.recover {
      case NonFatal(e) => throw new RuntimeException(s"in $method", e)
    }
    answerFuture.flatMap(f => f)
  }
}
