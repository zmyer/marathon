package mesosphere.marathon
package api.akkahttp

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route
import ch.qos.logback.classic.{ Level, Logger, LoggerContext }
import com.typesafe.config.{ Config, ConfigRenderOptions }
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.election.ElectionService
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.plugin.auth.AuthorizedResource.{ SystemConfig, SystemMetrics }
import mesosphere.marathon.plugin.auth.{ Authenticator, Authorizer, UpdateResource, ViewResource }
import mesosphere.marathon.raml.{ AnyToRaml, MetricsConversion }
import mesosphere.marathon.stream.Implicits._
import org.slf4j.LoggerFactory
import play.api.libs.json.JsString

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * The SystemController handles system level functionality like configuration, metrics and logging.
  */
class SystemController(val marathonConfig: MarathonConf, val cfg: Config, val electionService: ElectionService)(
    implicit
    val actorSystem: ActorSystem,
    val executionContext: ExecutionContext,
    val authenticator: Authenticator,
    val authorizer: Authorizer
) extends Controller with MetricsConversion with StrictLogging {

  import Directives._
  import EntityMarshallers._

  def pingPlainText: Route = complete("pong")
  def pingJson: Route = complete(JsString("ping"))

  /**
    * GET /metrics
    * @return a snapshot of all system level metrics.
    */
  def metrics: Route = {
    authenticated.apply { implicit identity =>
      authorized(ViewResource, SystemMetrics).apply {
        complete(Metrics.snapshot().toRaml)
      }
    }
  }

  /**
    * GET /config
    * @return the complete application config (typesafe config)
    */
  def config: Route = {
    authenticated.apply { implicit identity =>
      authorized(ViewResource, SystemMetrics).apply {
        complete(cfg.root().render(ConfigRenderOptions.defaults().setJson(true)))
      }
    }
  }

  /**
    * GET /logging
    * @return a map of all loggers with related log level
    */
  def showLoggers: Route = {
    authenticated.apply { implicit identity =>
      authorized(ViewResource, SystemConfig).apply {
        LoggerFactory.getILoggerFactory match {
          case lc: LoggerContext =>
            complete(lc.getLoggerList.map { logger =>
              logger.getName -> Option(logger.getLevel).map(_.levelStr).getOrElse(logger.getEffectiveLevel.levelStr + " (inherited)")
            }.toMap[String, String])
        }
      }
    }
  }

  /**
    * POST /logging
    * @return the log change.
    */
  def changeLoggers: Route = {
    authenticated.apply { implicit identity =>
      authorized(UpdateResource, SystemConfig).apply {
        entity(as[raml.LoggerChange]) { change: raml.LoggerChange =>
          LoggerFactory.getILoggerFactory.getLogger(change.logger) match {
            case log: Logger =>
              val level = Level.valueOf(change.level.value.toUpperCase)

              // current level can be null, which means: use the parent level
              // the current level should be preserved, no matter what the effective level is
              val currentLevel = log.getLevel
              val currentEffectiveLevel = log.getEffectiveLevel
              logger.info(s"Set logger ${log.getName} to $level current: $currentEffectiveLevel")
              log.setLevel(level)

              // if a duration is given, we schedule a timer to reset to the current level
              change.durationSeconds.foreach(duration => actorSystem.scheduler.scheduleOnce(duration.seconds, new Runnable {
                override def run(): Unit = {
                  logger.info(s"Duration expired. Reset Logger ${log.getName} back to $currentEffectiveLevel")
                  log.setLevel(currentLevel)
                }
              }))
              complete(change)
          }
        }
      }
    }
  }
  // format: OFF
  override val route: Route = {
    // To maintain the functionality of the original API, this endpoint is leader aware.
    // It would make sense to allow the functionality of this controller on every instance.
    asLeader(electionService) {
      path("ping") {
        (get & acceptsAnything) { pingPlainText } ~
        (get & accepts(MediaTypes.`text/plain`)) { pingPlainText } ~
        (get & accepts(MediaTypes.`application/json`)) { pingJson } ~
         get { complete(HttpResponse(StatusCodes.NoContent)) }
      } ~
        path("metrics") {
          get { metrics }
        } ~
        path("config") {
          get { config }
        } ~
        path("logging") {
          get { showLoggers } ~ post { changeLoggers }
        }
    }
  }
  // format: On
}
