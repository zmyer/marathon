package mesosphere.marathon
package metrics

import com.codahale.metrics.MetricRegistry

case class MetricsModule(metricsReporterConf: MetricsReporterConf) {
  lazy val metricRegistry = new MetricRegistry()
  lazy val metrics = new Metrics(metricRegistry)
  lazy val metricsReporterService = new MetricsReporterService(metricsReporterConf, metricRegistry)
}
