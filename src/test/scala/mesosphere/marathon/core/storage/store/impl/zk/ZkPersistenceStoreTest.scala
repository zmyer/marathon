package mesosphere.marathon
package core.storage.store.impl.zk

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.time.{ Instant, OffsetDateTime, ZoneOffset }
import java.util.UUID

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.util.ByteString
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.storage.store.{ IdResolver, PersistenceStoreTest, TestClass1 }
import mesosphere.marathon.integration.setup.ZookeeperServerTest

import scala.concurrent.duration._

trait ZkTestClass1Serialization {
  implicit object ZkTestClass1Resolver extends IdResolver[String, TestClass1, String, ZkId] {
    override def fromStorageId(path: ZkId): String = path.id.replaceAll("_", "/")
    override def toStorageId(id: String, version: Option[OffsetDateTime]): ZkId = {
      ZkId(category = "test-class", id.replaceAll("/", "_"), version)
    }
    override val category: String = "test-class"
    override val hasVersions = true
    override def version(tc: TestClass1): OffsetDateTime = tc.version
  }

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  implicit val tc1ZkMarshal: Marshaller[TestClass1, ZkSerialized] =
    Marshaller.opaque { (a: TestClass1) =>
      val builder = ByteString.newBuilder
      val id = a.str.getBytes(StandardCharsets.UTF_8)
      builder.putInt(id.length)
      builder.putBytes(id)
      builder.putInt(a.int)
      builder.putLong(a.version.toInstant.toEpochMilli)
      builder.putInt(a.version.getOffset.getTotalSeconds)
      ZkSerialized(builder.result())
    }

  implicit val tc1ZkUnmarshal: Unmarshaller[ZkSerialized, TestClass1] =
    Unmarshaller.strict { (a: ZkSerialized) =>
      val it = a.bytes.iterator
      val len = it.getInt
      val str = it.getBytes(len)
      val int = it.getInt
      val time = it.getLong
      val offset = it.getInt
      val version = OffsetDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.ofTotalSeconds(offset))
      TestClass1(new String(str, StandardCharsets.UTF_8), int, version)
    }
}

class ZkPersistenceStoreTest extends AkkaUnitTest
  with PersistenceStoreTest with ZookeeperServerTest with ZkTestClass1Serialization {

  lazy val rootClient = zkClient()

  def defaultStore: ZkPersistenceStore = {
    val root = UUID.randomUUID().toString
    val client = zkClient(namespace = Some(root))
    val store = new ZkPersistenceStore(client, Duration.Inf)
    store.markOpen()
    store
  }

  behave like basicPersistenceStore("ZookeeperPersistenceStore", defaultStore)
  behave like backupRestoreStore("ZookeeperPersistenceStore", defaultStore)
}

