package mesosphere.marathon
package core.event

import akka.actor.{ ActorRef, ActorRefFactory, Props }
import akka.event.EventStream
import com.typesafe.config.Config
import mesosphere.marathon.core.election.ElectionService
import mesosphere.marathon.core.event.impl.stream._
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.plugin.auth.{ Authenticator, Authorizer }
import mesosphere.marathon.util.toRichConfig
import org.eclipse.jetty.servlets.EventSourceServlet
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

trait EventConf extends ScallopConf {
  lazy val eventStreamMaxOutstandingMessages = opt[Int](
    "event_stream_max_outstanding_messages",
    descr = "The event stream buffers events, that are not already consumed by clients. " +
      "This number defines the number of events that get buffered on the server side, before messages are dropped.",
    noshort = true,
    default = Some(50)
  )
}

case class EventConfig(maxOutstandingMessages: Int)

object EventConfig {
  def apply(config: Config): EventConfig =
    EventConfig(maxOutstandingMessages = config.int("max-outstanding-messages"))

  def apply(conf: EventConf): EventConfig =
    EventConfig(maxOutstandingMessages = conf.eventStreamMaxOutstandingMessages())
}

/**
  * Exposes everything necessary to provide an internal event stream, an HTTP events stream and HTTP event callbacks.
  */
case class EventModule(config: EventConfig)(implicit
  eventBus: EventStream,
    actorRefFactory: ActorRefFactory,
    metrics: Metrics,
    electionService: ElectionService,
    authenticator: Authenticator,
    authorizer: Authorizer,
    ctx: ExecutionContext) {
  val log = LoggerFactory.getLogger(getClass.getName)

  lazy val httpEventStreamActor: ActorRef = {
    val outstanding = config.maxOutstandingMessages
    def handleStreamProps(handle: HttpEventStreamHandle): Props =
      Props(new HttpEventStreamHandleActor(handle, eventBus, outstanding))

    actorRefFactory.actorOf(
      Props(
        new HttpEventStreamActor(
          electionService,
          new HttpEventStreamActorMetrics(metrics),
          handleStreamProps)
      ),
      "HttpEventStream"
    )
  }

  lazy val httpEventStreamServlet: EventSourceServlet = {
    new HttpEventStreamServlet(httpEventStreamActor, authenticator, authorizer)
  }
}

object EventModule {
  def apply(conf: EventConf)(implicit
    eventBus: EventStream,
    actorRefFactory: ActorRefFactory,
    metrics: Metrics,
    electionService: ElectionService,
    authenticator: Authenticator,
    authorizer: Authorizer,
    ctx: ExecutionContext): EventModule = EventModule(EventConfig(conf))

  def apply(conf: Config)(implicit
    eventBus: EventStream,
    actorRefFactory: ActorRefFactory,
    metrics: Metrics,
    electionService: ElectionService,
    authenticator: Authenticator,
    authorizer: Authorizer,
    ctx: ExecutionContext): EventModule = EventModule(EventConfig(conf))
}