package mesosphere.marathon
package test

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.{ Context, AppenderBase }
import org.slf4j.LoggerFactory

object CaptureLogEvents {
  def forBlock(block: => Unit): Vector[ILoggingEvent] = {

    val capturingAppender = new CapturingAppender
    capturingAppender.appendToRootLogger()
    try block finally capturingAppender.detachFromRootLogger()
    capturingAppender.getEvents
  }

  private class CapturingAppender extends AppenderBase[ILoggingEvent] {
    setName("capture")

    private[this] var events = Vector.empty[ILoggingEvent]
    private[this] def rootLogger: Logger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]

    def appendToRootLogger(): Unit = {
      // Waiting for Logback to be ready.
      // See https://gist.github.com/mbknor/34944ea4589a5fc6974c
      // Should actually have been fixed with http://jira.qos.ch/browse/SLF4J-167
      while (LoggerFactory.getILoggerFactory.isInstanceOf[org.slf4j.helpers.SubstituteLoggerFactory]) {
        Thread.sleep(50)
      }
      setContext(LoggerFactory.getILoggerFactory.asInstanceOf[Context])
      start()
      rootLogger.addAppender(this)
    }

    def detachFromRootLogger(): Unit = rootLogger.detachAppender(this)

    def clearEvents(): Unit = synchronized { events = Vector.empty }
    def getEvents: Vector[ILoggingEvent] = synchronized { events }

    override def append(eventObject: ILoggingEvent): Unit = synchronized { events :+= eventObject }
  }
}
