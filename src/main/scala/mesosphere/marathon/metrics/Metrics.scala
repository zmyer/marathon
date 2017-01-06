package mesosphere.marathon
package metrics

import kamon.Kamon
import kamon.metric.instrument
import kamon.metric.instrument.Histogram.DynamicRange
import kamon.metric.instrument.{Histogram, Instrument, Time, UnitOfMeasurement}

trait Counter {
  def increment(): Unit
  def increment(times: Long): Unit
}

trait Gauge {
  def value(): Long
  def setValue(value: Long): this.type
  def increment(by: Long = 1): this.type
  def decrement(by: Long = 1): this.type
}

trait Histogram {
  def record(value: Long)
  def record(value: Long, count: Long)
}

trait MinMaxCounter {
  def increment(): Unit
  def increment(times: Long): Unit
  def decrement()
  def decrement(times: Long)
  def refreshValues(): Unit
}

object Metrics {
  implicit class KamonCounter(val counter: instrument.Counter) extends AnyVal with Counter {
    override def increment(): Unit = counter.increment()
    override def increment(times: Long): Unit = counter.increment(times)
  }
  def counter(prefix: MetricPrefix, `class`: Class[_], metricName: String,
              tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown) : Counter = {
    Kamon.metrics.counter(name(prefix, `class`, metricName), tags, unit)
  }

  def gauge(prefix: MetricPrefix, `class`: Class[_], metricName: String,
            tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): Gauge = {
    AtomicGauge(name(prefix, `class`, metricName), unit, tags)
  }

  implicit class KamonHistogram(val histogram: instrument.Histogram) extends AnyVal with Histogram {
    override def record(value: Long): Unit = histogram.record(value)
    override def record(value: Long, count: Long): Unit = histogram.record(value, count)
  }

  def histogram(prefix: MetricPrefix, `class`: Class[_], metricName: String,
                tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown,
                dynamicRange: DynamicRange): Histogram = {
    Kamon.metrics.histogram(name(prefix, `class`, metricName), tags, unit, dynamicRange)
  }

  implicit class KamonMinMaxCounter(val counter: instrument.MinMaxCounter) extends MinMaxCounter {
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
    Timer(name(prefix, `class`, metricName), tags, unit)
  }
}
