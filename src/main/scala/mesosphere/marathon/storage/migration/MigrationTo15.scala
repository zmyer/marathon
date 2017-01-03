package mesosphere.marathon
package storage.migration

import akka.Done
import mesosphere.marathon.core.storage.store.PersistenceStore
import mesosphere.marathon.core.storage.store.impl.zk.ZkPersistenceStore

import scala.concurrent.{ ExecutionContext, Future }
import scala.async.Async._

case class MigrationTo15[K, C, S](store: PersistenceStore[K, C, S])(implicit ctx: ExecutionContext) {
  @SuppressWarnings(Array("all"))
  def migrate(): Future[Done] = async {
    store match {
      case zk: ZkPersistenceStore =>
        await(zk.client.delete("/event-subscribers"))
      case _ =>
    }
    Done
  }
}
