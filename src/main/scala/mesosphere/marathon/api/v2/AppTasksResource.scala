package mesosphere.marathon
package api.v2

import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, MediaType, Response }

import mesosphere.marathon.api.EndpointsHelper.ListTasks
import mesosphere.marathon.api._
import mesosphere.marathon.core.appinfo.EnrichedTask
import mesosphere.marathon.core.async.ExecutionContexts.global
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.health.HealthCheckManager
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.core.task.tracker.InstanceTracker.InstancesBySpec
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.raml.AnyToRaml
import mesosphere.marathon.raml.Task._
import mesosphere.marathon.raml.TaskConversion._
import mesosphere.marathon.state.PathId
import mesosphere.marathon.state.PathId._
import org.slf4j.LoggerFactory

import scala.async.Async._
import scala.concurrent.Future

@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class AppTasksResource @Inject() (
    instanceTracker: InstanceTracker,
    taskKiller: TaskKiller,
    healthCheckManager: HealthCheckManager,
    val config: MarathonConf,
    groupManager: GroupManager,
    val authorizer: Authorizer,
    val authenticator: Authenticator) extends AuthResource {

  val log = LoggerFactory.getLogger(getClass.getName)
  val GroupTasks = """^((?:.+/)|)\*$""".r

  @GET
  @SuppressWarnings(Array("all")) /* async/await */
  def indexJson(
    @PathParam("appId") id: String,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val tasksResponse = async {
      val instancesBySpec = await(instanceTracker.instancesBySpec)
      id match {
        case GroupTasks(gid) =>
          val groupPath = gid.toRootPath
          val maybeGroup = groupManager.group(groupPath)
          withAuthorization(ViewGroup, maybeGroup, unknownGroup(groupPath)) { group =>
            ok(jsonObjString("tasks" -> runningTasks(group.transitiveAppIds, instancesBySpec).toRaml))
          }
        case _ =>
          val appId = id.toRootPath
          val maybeApp = groupManager.app(appId)
          withAuthorization(ViewRunSpec, maybeApp, unknownApp(appId)) { _ =>
            ok(jsonObjString("tasks" -> runningTasks(Set(appId), instancesBySpec).toRaml))
          }
      }
    }
    result(tasksResponse)
  }

  def runningTasks(appIds: Iterable[PathId], instancesBySpec: InstancesBySpec): Vector[EnrichedTask] = {
    appIds.withFilter(instancesBySpec.hasSpecInstances).flatMap { id =>
      val health = result(healthCheckManager.statuses(id))
      instancesBySpec.specInstances(id).flatMap { instance =>
        instance.tasksMap.values.map { task =>
          EnrichedTask(id, task, instance.agentInfo, health.getOrElse(instance.instanceId, Nil))
        }
      }
    }(collection.breakOut)
  }

  @GET
  @Produces(Array(MediaType.TEXT_PLAIN))
  @SuppressWarnings(Array("all")) /* async/await */
  def indexTxt(
    @PathParam("appId") appId: String,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val id = appId.toRootPath
    result(async {
      val instancesBySpec = await(instanceTracker.instancesBySpec)
      withAuthorization(ViewRunSpec, groupManager.app(id), unknownApp(id)) { app =>
        ok(EndpointsHelper.appsToEndpointString(ListTasks(instancesBySpec, Seq(app))))
      }
    })
  }

  @DELETE
  @SuppressWarnings(Array("all")) // async/await
  def deleteMany(
    @PathParam("appId") appId: String,
    @QueryParam("host") host: String,
    @QueryParam("scale")@DefaultValue("false") scale: Boolean = false,
    @QueryParam("force")@DefaultValue("false") force: Boolean = false,
    @QueryParam("wipe")@DefaultValue("false") wipe: Boolean = false,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val pathId = appId.toRootPath

    def findToKill(appTasks: Seq[Instance]): Seq[Instance] = {
      Option(host).fold(appTasks) { hostname =>
        appTasks.filter(_.agentInfo.host == hostname || hostname == "*")
      }
    }

    if (scale && wipe) throw new BadRequestException("You cannot use scale and wipe at the same time.")

    if (scale) {
      val deploymentF = taskKiller.killAndScale(pathId, findToKill, force)
      deploymentResult(result(deploymentF))
    } else {
      val response: Future[Response] = async {
        val instances = await(taskKiller.kill(pathId, findToKill, wipe))
        val healthStatuses = await(healthCheckManager.statuses(pathId))
        val enrichedTasks: Seq[EnrichedTask] = instances.map { instance =>
          val killedTask = instance.appTask
          val enrichedTask = EnrichedTask(pathId, killedTask, instance.agentInfo, healthStatuses.getOrElse(instance.instanceId, Nil))
          enrichedTask
        }
        ok(jsonObjString("tasks" -> enrichedTasks.toRaml))
      }.recover {
        case PathNotFoundException(appId, version) => unknownApp(appId, version)
      }

      result(response)
    }
  }

  @DELETE
  @Path("{taskId}")
  @SuppressWarnings(Array("all")) // async/await
  def deleteOne(
    @PathParam("appId") appId: String,
    @PathParam("taskId") id: String,
    @QueryParam("scale")@DefaultValue("false") scale: Boolean = false,
    @QueryParam("force")@DefaultValue("false") force: Boolean = false,
    @QueryParam("wipe")@DefaultValue("false") wipe: Boolean = false,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val pathId = appId.toRootPath
    def findToKill(appTasks: Seq[Instance]): Seq[Instance] = {
      try {
        val instanceId = Task.Id(id).instanceId
        appTasks.filter(_.instanceId == instanceId)
      } catch {
        // the id can not be translated to an instanceId
        case _: MatchError => Seq.empty
      }
    }

    if (scale && wipe) throw new BadRequestException("You cannot use scale and wipe at the same time.")

    if (scale) {
      val deploymentF = taskKiller.killAndScale(pathId, findToKill, force)
      deploymentResult(result(deploymentF))
    } else {
      val response: Future[Response] = async {
        val instances = await(taskKiller.kill(pathId, findToKill, wipe))
        val healthStatuses = await(healthCheckManager.statuses(pathId))
        instances.headOption match {
          case None =>
            unknownTask(id)
          case Some(instance) =>
            val killedTask = instance.appTask
            val enrichedTask = EnrichedTask(pathId, killedTask, instance.agentInfo, healthStatuses.getOrElse(instance.instanceId, Nil))
            ok(jsonObjString("task" -> enrichedTask.toRaml))
        }
      }.recover {
        case PathNotFoundException(appId, version) => unknownApp(appId, version)
      }

      result(response)
    }
  }
}
