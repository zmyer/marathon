package mesosphere.marathon
package api

import java.time.Instant
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, MediaType, Response }

import akka.actor.{ Actor, ActorRefFactory, Props }
import com.google.inject.Inject
import kamon.Kamon
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot
import kamon.metric.instrument.Histogram.Snapshot
import kamon.metric.instrument.{ CollectionContext, Counter, Histogram }
import kamon.metric.{ Entity, SubscriptionFilter }
import kamon.util.{ MapMerge, MilliTimestamp }
import mesosphere.marathon.metrics.{ ApiMetric, Metrics }
import mesosphere.marathon.plugin.auth.AuthorizedResource.SystemConfig
import mesosphere.marathon.plugin.auth.{ Authenticator, Authorizer, ViewResource }
import play.api.libs.json.{ JsObject, _ }

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

  private[this] val pingTimer = Metrics.timer(ApiMetric, classOf[SystemResource], "ping")

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

  implicit val snapshotWrites: Writes[(Map[String, String], Snapshot)] = Writes[(Map[String, String], Histogram.Snapshot)] {
    case (tags, histogram) =>
      JsObject(Seq(
        "count" -> JsNumber(histogram.numberOfMeasurements),
        "min" -> JsNumber(histogram.min),
        "max" -> JsNumber(histogram.max),
        "p50" -> JsNumber(histogram.percentile(0.5)),
        "p75" -> JsNumber(histogram.percentile(0.75)),
        "p98" -> JsNumber(histogram.percentile(0.98)),
        "p99" -> JsNumber(histogram.percentile(0.99)),
        "p999" -> JsNumber(histogram.percentile(0.999)),
        "mean" -> JsNumber(if (histogram.numberOfMeasurements != 0) histogram.sum / histogram.numberOfMeasurements else 0),
        "tags" -> JsObject(tags.map { case (k, v) => k -> JsString(v) })
      ))
  }

  implicit val tickMetricWrites: Writes[TickMetricSnapshot] = Writes[TickMetricSnapshot] { snapshot =>
    val metrics = snapshot.metrics.map {
      case (entity, entitySnapshot) =>
        val entityMetrics = entitySnapshot.metrics.map {
          case (metricKey, metricSnapshot) =>
            val metricName = if (entity.category == metricKey.name) entity.name else s"${entity.name}.${metricKey.name}"
            metricSnapshot match {
              case histogram: Histogram.Snapshot =>
                metricName -> JsObject(Seq(
                  "count" -> JsNumber(histogram.numberOfMeasurements),
                  "min" -> JsNumber(histogram.min),
                  "max" -> JsNumber(histogram.max),
                  "p50" -> JsNumber(histogram.percentile(0.5)),
                  "p75" -> JsNumber(histogram.percentile(0.75)),
                  "p98" -> JsNumber(histogram.percentile(0.98)),
                  "p99" -> JsNumber(histogram.percentile(0.99)),
                  "p999" -> JsNumber(histogram.percentile(0.999)),
                  "mean" -> JsNumber(if (histogram.numberOfMeasurements != 0) histogram.sum / histogram.numberOfMeasurements else 0),
                  "tags" -> JsObject(entity.tags.map { case (k, v) => k -> JsString(v) }),
                  "unit" -> JsString(metricKey.unitOfMeasurement.label)
                ))
              case cs: Counter.Snapshot =>
                metricName -> JsObject(Seq(
                  "count" -> JsNumber(cs.count),
                  "tags" -> JsObject(entity.tags.map { case (k, v) => k -> JsString(v) }),
                  "unit" -> JsString(metricKey.unitOfMeasurement.label)
                ))
            }
        }
        entity.category -> entityMetrics
    }

    JsObject(Map(
      "start" -> JsString(Instant.ofEpochMilli(snapshot.from.millis).toString),
      "end" -> JsString(Instant.ofEpochMilli(snapshot.to.millis).toString)
    ) ++ metrics)
  }

  @GET
  @Path("ping")
  def ping(@Context req: HttpServletRequest): Response = pingTimer.blocking {
    authenticated(req) { implicit identity =>
      withAuthorization(ViewResource, SystemConfig){
        ok("pong")
      }
    }
  }

  @GET
  @Path("metrics")
  def metrics(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withAuthorization(ViewResource, SystemConfig){
      ok(jsonString(metrics))
    }
  }
}
