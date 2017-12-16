package mesosphere.marathon
package api.v2

import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.Response.Status._
import javax.ws.rs.core.{ Context, MediaType, Response }

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.{ AuthResource, MarathonMediaType }
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.state.PathId
import mesosphere.marathon.{ MarathonConf, MarathonSchedulerService }

@Path("v2/deployments")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class DeploymentsResource @Inject() (
    service: MarathonSchedulerService,
    groupManager: GroupManager,
    val authenticator: Authenticator,
    val authorizer: Authorizer,
    val config: MarathonConf)
  extends AuthResource
  with StrictLogging {

  @GET
  def running(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val infos = result(service.listRunningDeployments())
      .filter(_.plan.affectedRunSpecs.exists(isAuthorized(ViewRunSpec, _)))
    ok(infos)
  }

  @DELETE
  @Path("{id}")
  def cancel(
    @PathParam("id") id: String,
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val plan = result(service.listRunningDeployments()).find(_.plan.id == id).map(_.plan)
    plan.fold(notFound(s"DeploymentPlan $id does not exist")) { deployment =>
      deployment.affectedRunSpecs.foreach(checkAuthorization(UpdateRunSpec, _))

      if (force) {
        // do not create a new deployment to return to the previous state
        logger.info(s"Canceling deployment [$id]")
        service.cancelDeployment(deployment)
        status(ACCEPTED) // 202: Accepted
      } else {
        // create a new deployment to return to the previous state
        deploymentResult(result(groupManager.updateRoot(
          PathId.empty,
          deployment.revert,
          force = true
        )))
      }
    }
  }
}
