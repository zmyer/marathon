package mesosphere.marathon
package util

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import mesosphere.UnitTest

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class WorkQueueTest extends UnitTest {

  "WorkQueue" should {
    "cap the maximum number of concurrent operations" in {
      val counter = new AtomicInteger(0)
      val waitToExit1 = new Semaphore(0)
      val exited1 = new Semaphore(0)
      val waitToExit2 = new Semaphore(0)
      val exited2 = new Semaphore(0)

      val queue = WorkQueue("test", maxConcurrent = 1, maxQueueLength = Int.MaxValue)
      queue {
        Future {
          waitToExit1.acquire()
          exited1.release()
        }
      }

      queue {
        Future {
          counter.incrementAndGet()
          waitToExit2.acquire()
          exited2.release()
        }
      }

      waitToExit1.release()
      counter.get() should equal(0)
      exited1.acquire()

      waitToExit2.release()
      exited2.acquire()
      counter.get() should equal(1)
    }
  }
}
