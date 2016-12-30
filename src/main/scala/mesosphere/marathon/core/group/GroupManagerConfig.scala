package mesosphere.marathon
package core.group

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import org.rogach.scallop.{ ScallopConf, ScallopOption }

import scala.concurrent.duration._
import mesosphere.marathon.util.toRichConfig

trait GroupManagerConf extends ScallopConf {
  lazy val groupManagerRequestTimeout = opt[Int](
    "group_manager_request_timeout",
    descr = "INTERNAL TUNING PARAMETER: Timeout (in ms) for requests to the group manager actor.",
    hidden = true,
    default = Some(10.seconds.toMillis.toInt))

  lazy val internalMaxQueuedRootGroupUpdates = opt[Int](
    "max_queued_root_group_updates",
    descr = "INTERNAL TUNING PARAMETER: " +
      "The maximum number of root group updates that we queue before rejecting updates.",
    noshort = true,
    hidden = true,
    default = Some(500)
  )

  def availableFeatures: Set[String]

  def localPortMin: ScallopOption[Int]
  def localPortMax: ScallopOption[Int]
}

case class GroupManagerConfig(requestTimeout: Duration, maxQueuedUpdates: Int, availableFeatures: Set[String],
  localPortMin: Int, localPortMax: Int)

object GroupManagerConfig {
  def apply(config: Config): GroupManagerConfig =
    GroupManagerConfig(
      requestTimeout = config.duration("request-timeout"),
      maxQueuedUpdates = config.int("max-queued-updates"),
      availableFeatures = config.stringList("available-features").to[Set],
      localPortMin = config.int("local-port-min"),
      localPortMax = config.int("local-port-max"))

  def apply(conf: GroupManagerConf): GroupManagerConfig =
    GroupManagerConfig(
      requestTimeout = Duration(conf.groupManagerRequestTimeout().toLong, TimeUnit.MILLISECONDS),
      maxQueuedUpdates = conf.internalMaxQueuedRootGroupUpdates(),
      availableFeatures = conf.availableFeatures,
      localPortMin = conf.localPortMin(),
      localPortMax = conf.localPortMax())
}
