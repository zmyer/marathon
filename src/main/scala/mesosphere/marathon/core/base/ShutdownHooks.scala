package mesosphere.marathon.core.base

import java.util.{ Timer, TimerTask }
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.Done
import com.typesafe.scalalogging.StrictLogging

/**
  * Manages the Marathon Lifecycle shutdown process, either handling a more graceful shutdown, or a forceful abortAsync
  *
  * TODO - rename (MarathonLifecycle?)
  */
trait ShutdownHooks {
  /**
    * Add an additional hook to the shutdown process
    *
    * Note, if shutdown is already in process, the hook will not be called.
    */
  def onShutdown(block: => Unit): Unit

  /**
    * Shutdown Marathon gracefull
    */
  def shutdown(): Unit

  /**
    * Is Marathon currently trying to shut down?
    */
  def isShuttingDown: Boolean

  /**
    * Try to stop Marathon as quickly as possible. Do not apply hooks.
    */
  def abortAsync(
    exitCode: Int = ShutdownHooks.FatalErrorSignal,
    waitForExit: FiniteDuration = ShutdownHooks.DefaultExitDelay)(implicit ec: ExecutionContext): Future[Done]
}

object ShutdownHooks {
  def apply(): ShutdownHooks = new DefaultShutdownHooks

  val FatalErrorSignal = 137
  val DefaultExitDelay = 10.seconds
}

private[base] class BaseShutdownHooks extends ShutdownHooks with StrictLogging {
  private[this] var shutdownHooks = List.empty[() => Unit]
  private[this] val shuttingDown = new AtomicBoolean(false)

  override def onShutdown(block: => Unit): Unit = {
    shutdownHooks +:= { () => block }
  }

  override def shutdown(): Unit = {
    if (shuttingDown.compareAndSet(false, true)) {
      shutdownHooks.foreach { hook =>
        try hook()
        catch {
          case NonFatal(e) => logger.error("while executing shutdown hook", e)
        }
      }
      shutdownHooks = Nil
    } else {
      logger.info("ignored shutdown call; already shutting down")
    }

  }

  /**
    * Exit this process in an async fashion.
    * First try exit regularly in the given timeout. If this does not exit in time, we halt the system.
    *
    * @param exitCode    the exit code to signal.
    * @param waitForExit the time to wait for a normal exit.
    * @return the Future of this operation.
    */
  override def abortAsync(
    exitCode: Int = ShutdownHooks.FatalErrorSignal,
    waitForExit: FiniteDuration = ShutdownHooks.DefaultExitDelay)(implicit ec: ExecutionContext): Future[Done] = {
    shuttingDown.set(true)
    val timer = new Timer()
    val promise = Promise[Done]()
    logger.info("aborting Marathon")
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        logger.info("Halting JVM")
        promise.success(Done)
        // do nothing in tests: we can't guarantee we can block the exit
        if (!sys.props.get("java.class.path").exists(_.contains("test-classes"))) {
          Runtime.getRuntime.halt(exitCode)
        }
      }
    }, waitForExit.toMillis)
    Future(sys.exit(exitCode))
    promise.future
  }

  override def isShuttingDown: Boolean = shuttingDown.get
}

/**
  * Extends BaseShutdownHooks by ensuring that the hooks are run when the VM shuts down.
  */
private class DefaultShutdownHooks extends BaseShutdownHooks {
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      shutdown()
    }
  })
}
