package mesosphere.marathon
package metrics

import kamon.Kamon
import kamon.metric.instrument.Time
import kamon.metric.instrument
import mesosphere.util.CallerThreadExecutionContext

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

case class Timer(name: String, tags: Map[String, String], unit: Time) {
  private[this] val histogram: instrument.Histogram = Kamon.metrics.histogram(name, tags, unit)

  def apply[T](f: => Future[T]): Future[T] = {
    val start = System.nanoTime()
    val future = try {
      f
    } catch {
      case NonFatal(e) =>
        histogram.record(System.nanoTime() - start)
        throw e
    }
    future.onComplete(_ => histogram.record(System.nanoTime() - start))(CallerThreadExecutionContext.callerThreadExecutionContext)
    future
  }

  def blocking[T](f: => T): T = {
    val start = System.nanoTime()
    try {
      f
    } finally {
      histogram.record(System.nanoTime() - start)
    }
  }

  def update(value: Long): this.type = {
    histogram.record(value)
    this
  }

  def update(duration: FiniteDuration): this.type = {
    val value = unit match {
      case Time.Nanoseconds => duration.toNanos
      case Time.Milliseconds => duration.toMillis
      case Time.Microseconds => duration.toMicros
      case Time.Seconds => duration.toSeconds
    }
    update(value)
  }
}
