package mesosphere.marathon
package util

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ ExecutionContext, Future, Promise }
import mesosphere.marathon.functional._

import scala.collection.mutable

case class WorkQueue(name: String, maxConcurrent: Int, maxQueueLength: Int, parent: Option[WorkQueue] = None) {
  private case class WorkItem[T](f: () => Future[T], ctx: ExecutionContext, promise: Promise[T])
  private val queue = new ConcurrentLinkedQueue[WorkItem[_]]()
  private val totalOutstanding = new AtomicInteger(0)

  private def run[T](workItem: WorkItem[T]): Future[T] = {
    parent.fold {
      workItem.ctx.execute(new Runnable {
        override def run(): Unit = {
          val future = workItem.f()
          future.onComplete(_ => executeNextIfPossible())(workItem.ctx)
          workItem.promise.completeWith(future)
        }
      })
      workItem.promise.future
    } { p =>
      p(workItem.f())(workItem.ctx)
    }
  }

  private def executeNextIfPossible(): Unit = {
    Option(queue.poll()).fold[Unit] {
      totalOutstanding.decrementAndGet()
    } { run(_) }
  }

  def apply[T](f: => Future[T])(implicit ctx: ExecutionContext): Future[T] = {
    val previouslyOutstanding = totalOutstanding.getAndUpdate((total: Int) => if (total <= maxConcurrent) total + 1 else total)
    if (previouslyOutstanding < maxConcurrent) {
      val promise = Promise[T]()
      run(WorkItem(() => f, ctx, promise))
    } else {
      val promise = Promise[T]()
      queue.add(WorkItem(() => f, ctx, promise))
      promise.future
    }
  }
}

case class KeyedLock[T](name: String, maxQueueLength: Int, parent: Option[WorkQueue] = None) {
  private val queues = Lock(mutable.HashMap.empty[T, WorkQueue].withDefault(hash => WorkQueue(s"$name-$hash", maxConcurrent = 1, maxQueueLength, parent)))

  def apply(key: T)(f: => Future[T])(implicit ctx: ExecutionContext): Future[T] = {
    queues(_(key)(f))
  }
}
