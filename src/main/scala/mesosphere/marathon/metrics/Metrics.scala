package mesosphere.marathon
package metrics

import akka.stream.scaladsl.Source
import kamon.Kamon
import kamon.metric.instrument
import kamon.metric.instrument.Histogram.DynamicRange
import kamon.metric.instrument.{ Time, UnitOfMeasurement }
import org.aopalliance.intercept.MethodInvocation

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Counter {
  def increment(): Unit
  def increment(times: Long): Unit
}

trait Gauge {
  def value(): Long
  def increment(by: Long = 1): this.type
  def decrement(by: Long = 1): this.type
}

trait SettableGauge extends Gauge {
  def setValue(value: Long): this.type
}

trait Histogram {
  def record(value: Long): Unit
  def record(value: Long, count: Long): Unit
}

trait MinMaxCounter {
  def increment(): Unit
  def increment(times: Long): Unit
  def decrement(): Unit
  def decrement(times: Long): Unit
  def refreshValues(): Unit
}

trait Timer {
  def apply[T](f: => Future[T]): Future[T]
  def forSource[T, M](f: => Source[T, M]): Source[T, M]
  def blocking[T](f: => T): T
  def update(value: Long): this.type
  def update(duration: FiniteDuration): this.type
}

object Metrics {
  private implicit class KamonCounter(val counter: instrument.Counter) extends Counter {
    override def increment(): Unit = counter.increment()
    override def increment(times: Long): Unit = counter.increment(times)
  }

  def counter(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): Counter = {
    Kamon.metrics.counter(name(prefix, `class`, metricName), tags, unit)
  }

  private implicit class KamonGauge(val gauge: instrument.Gauge) extends Gauge {
    override def value(): Long = gauge.value()
    override def increment(by: Long): this.type = {
      gauge.increment(by)
      this
    }
    override def decrement(by: Long): this.type = {
      gauge.decrement(by)
      this
    }
  }

  def gauge(prefix: MetricPrefix, `class`: Class[_], metricName: String, currentValue: () => Long,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): Gauge = {
    Kamon.metrics.gauge(name(prefix, `class`, metricName), tags, unit)(currentValue)
  }

  def atomicGauge(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): SettableGauge = {
    AtomicGauge(name(prefix, `class`, metricName), unit, tags)
  }

  private implicit class KamonHistogram(val histogram: instrument.Histogram) extends Histogram {
    override def record(value: Long): Unit = histogram.record(value)
    override def record(value: Long, count: Long): Unit = histogram.record(value, count)
  }

  def histogram(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown,
    dynamicRange: DynamicRange): Histogram = {
    Kamon.metrics.histogram(name(prefix, `class`, metricName), tags, unit, dynamicRange)
  }

  private implicit class KamonMinMaxCounter(val counter: instrument.MinMaxCounter) extends MinMaxCounter {
    override def increment(): Unit = counter.increment()
    override def increment(times: Long): Unit = counter.increment(times)
    override def decrement(): Unit = counter.decrement()
    override def decrement(times: Long): Unit = counter.decrement(times)
    override def refreshValues(): Unit = counter.refreshValues()
  }

  def minMaxCounter(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): MinMaxCounter = {
    Kamon.metrics.minMaxCounter(name(prefix, `class`, metricName), tags, unit)
  }

  def timer(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: Time = Time.Nanoseconds): Timer = {
    HistogramTimer(name(prefix, `class`, metricName), tags, unit)
  }

  def timer(prefix: MetricPrefix, methodInvocation: MethodInvocation): Timer = {
    HistogramTimer(name(prefix, methodInvocation), Map.empty, Time.Nanoseconds)
  }
}
