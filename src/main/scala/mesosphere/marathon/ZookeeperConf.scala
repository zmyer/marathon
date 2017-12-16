package mesosphere.marathon

import java.net.InetSocketAddress

import org.apache.zookeeper.ZooDefs
import org.rogach.scallop.ScallopConf

import scala.concurrent.duration._

trait ZookeeperConf extends ScallopConf {
  import ZookeeperConf._

  lazy val zooKeeperTimeout = opt[Long](
    "zk_timeout",
    descr = "The timeout for ZooKeeper operations in milliseconds.",
    default = Some(Duration(10, SECONDS).toMillis))

  lazy val zooKeeperSessionTimeout = opt[Long](
    "zk_session_timeout",
    descr = "The timeout for ZooKeeper sessions in milliseconds",
    default = Some(Duration(10, SECONDS).toMillis)
  )

  lazy val zooKeeperConnectionTimeout = opt[Long](
    "zk_connection_timeout",
    descr = "The timeout to connect to ZooKeeper in milliseconds",
    default = Some(Duration(10, SECONDS).toMillis)
  )

  lazy val zooKeeperUrl = opt[String](
    "zk",
    descr = "ZooKeeper URL for storing state. Format: zk://host1:port1,host2:port2,.../path",
    validate = (in) => ZKUrlPattern.pattern.matcher(in).matches(),
    default = Some("zk://localhost:2181/marathon")
  )

  lazy val zooKeeperCompressionEnabled = toggle(
    "zk_compression",
    descrYes =
      "(Default) Enable compression of zk nodes, if the size of the node is bigger than the configured threshold.",
    descrNo = "Disable compression of zk nodes",
    noshort = true,
    prefix = "disable_",
    default = Some(true)
  )

  lazy val zooKeeperCompressionThreshold = opt[Long](
    "zk_compression_threshold",
    descr = "(Default: 64 KB) Threshold in bytes, when compression is applied to the ZooKeeper node.",
    noshort = true,
    validate = _ >= 0,
    default = Some(64 * 1024)
  )

  lazy val zooKeeperMaxNodeSize = opt[Long](
    "zk_max_node_size",
    descr = "(Default: 1 MiB) Maximum allowed ZooKeeper node size (in bytes).",
    noshort = true,
    validate = _ >= 0,
    default = Some(1024 * 1000)
  )

  def zooKeeperStatePath: String = "%s/state".format(zkPath)
  def zooKeeperLeaderPath: String = "%s/leader".format(zkPath)
  def zooKeeperServerSetPath: String = "%s/apps".format(zkPath)

  def zooKeeperHostAddresses: Seq[InetSocketAddress] =
    zkHosts.split(",").map { s =>
      val splits = s.split(":")
      require(splits.length == 2, "expected host:port for zk servers")
      new InetSocketAddress(splits(0), splits(1).toInt)
    }(collection.breakOut)

  @SuppressWarnings(Array("OptionGet"))
  def zkURL: String = zooKeeperUrl.get.get

  lazy val zkHosts = zkURL match { case ZKUrlPattern(_, _, server, _) => server }
  lazy val zkPath = zkURL match { case ZKUrlPattern(_, _, _, path) => path }
  lazy val zkUsername = zkURL match { case ZKUrlPattern(u, _, _, _) => Option(u) }
  lazy val zkPassword = zkURL match { case ZKUrlPattern(_, p, _, _) => Option(p) }

  lazy val zkDefaultCreationACL = (zkUsername, zkPassword) match {
    case (Some(_), Some(_)) => ZooDefs.Ids.CREATOR_ALL_ACL
    case _ => ZooDefs.Ids.OPEN_ACL_UNSAFE
  }

  lazy val zkTimeoutDuration = Duration(zooKeeperTimeout(), MILLISECONDS)
  lazy val zkSessionTimeoutDuration = Duration(zooKeeperSessionTimeout(), MILLISECONDS)
  lazy val zkConnectionTimeoutDuration = Duration(zooKeeperConnectionTimeout(), MILLISECONDS)
}

object ZookeeperConf {
  private val user = """[^/:]+"""
  private val pass = """[^@]+"""
  private val hostAndPort = """[A-z0-9-.]+(?::\d+)?"""
  private val zkNode = """[^/]+"""
  val ZKUrlPattern = s"""^zk://(?:($user):($pass)@)?($hostAndPort(?:,$hostAndPort)*)(/$zkNode(?:/$zkNode)*)$$""".r
}
