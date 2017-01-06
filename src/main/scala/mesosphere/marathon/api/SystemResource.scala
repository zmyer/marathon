package mesosphere.marathon
package api

import java.io.StringWriter
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, MediaType, Response }

import akka.actor.{ Actor, ActorRefFactory, Props }
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.inject.Inject
import kamon.Kamon
import kamon.metric.{ Entity, SubscriptionFilter }
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot
import kamon.metric.instrument.CollectionContext
import kamon.util.{ MapMerge, MilliTimestamp }
import mesosphere.marathon.io.IO
import mesosphere.marathon.plugin.auth.AuthorizedResource.SystemConfig
import mesosphere.marathon.plugin.auth.{ Authenticator, Authorizer, ViewResource }

/**
  * System Resource gives access to system level functionality.
  * All system resources can be protected via ACLs.
  */
@Path("")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class SystemResource @Inject() (val config: MarathonConf)(implicit
  val authenticator: Authenticator,
    val authorizer: Authorizer,
    actorRefFactory: ActorRefFactory) extends RestResource with AuthResource {

  private val mapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  private[this] var metrics: TickMetricSnapshot = {
    val now = MilliTimestamp.now
    TickMetricSnapshot(now, now, Map.empty)
  }

  class SubscriberActor(metricSnapshot: TickMetricSnapshot) extends Actor {
    val collectionContext: CollectionContext = Kamon.metrics.buildDefaultCollectionContext

    override def receive: Actor.Receive = {
      case TickMetricSnapshot(_, to, tickMetrics) =>
        val combined = MapMerge.Syntax(metrics.metrics).merge(tickMetrics, (l, r) => l.merge(r, collectionContext))
        val combinedSnapshot = TickMetricSnapshot(metrics.from, to, combined)
        metrics = combinedSnapshot
    }
  }

  Kamon.metrics.subscribe(new SubscriptionFilter {
    def accept(entity: Entity): Boolean = true
  }, actorRefFactory.actorOf(Props(new SubscriberActor(metrics))))

  @GET
  @Path("ping")
  def ping(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withAuthorization(ViewResource, SystemConfig){
      ok("pong")
    }
  }

  @GET
  @Path("metrics")
  def metrics(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withAuthorization(ViewResource, SystemConfig){
      IO.using(new StringWriter()) { writer =>
        mapper.writer().writeValue(writer, metrics)
        ok(writer.toString)
      }
    }
  }
}
