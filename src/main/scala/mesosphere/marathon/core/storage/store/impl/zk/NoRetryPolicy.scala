package mesosphere.marathon
package core.storage.store.impl.zk

import org.apache.curator.{ RetryPolicy, RetrySleeper }

object NoRetryPolicy extends RetryPolicy {
  override def allowRetry(retryCount: Int, elapsedTimeMs: Long, sleeper: RetrySleeper): Boolean = false
}
