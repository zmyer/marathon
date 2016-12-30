package mesosphere.marathon
package core.base

import akka.actor.{ ActorRefFactory, ActorSystem, Scheduler }
import akka.event.EventStream
import akka.stream.{ ActorMaterializer, Materializer }
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Await
import scala.concurrent.duration._

case class BaseModule(config: Config, systemName: String = "marathon",
  shutdownHooks: ShutdownHooks = ShutdownHooks())
    extends StrictLogging {

  protected[this] implicit lazy val actorSystem: ActorSystem = ActorSystem(systemName, config)
  implicit lazy val actorRefFactory: ActorRefFactory = actorSystem
  implicit lazy val materializer: Materializer = ActorMaterializer()(actorSystem)
  implicit lazy val scheduler: Scheduler = actorSystem.scheduler
  implicit lazy val eventStream: EventStream = actorSystem.eventStream

  shutdownHooks.onShutdown {
    logger.info(s"Shutting down actor system $actorSystem")
    Await.result(actorSystem.terminate(), 10.seconds)
  }
}
