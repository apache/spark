/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.util.Collections
import java.util.concurrent.TimeUnit

import org.apache.spark.api.java.JavaFutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobFailed, JobSucceeded, JobWaiter}

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

/**
 * A future for the result of an action to support cancellation. This is an extension of the
 * Scala Future interface to support cancellation.
 */
trait FutureAction[T] extends Future[T] {
  // Note that we redefine methods of the Future trait here explicitly so we can specify a different
  // documentation (with reference to the word "action").

  /**
   * Cancels the execution of this action.
   */
  def cancel()

  /**
   * Blocks until this action completes.
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @return this FutureAction
   */
  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureAction.this.type

  /**
   * Awaits and returns the result (of type T) of this action.
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @throws Exception exception during action execution
   * @return the result value if the action is completed within the specific maximum wait time
   */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T

  /**
   * When this action is completed, either through an exception, or a value, applies the provided
   * function.
   */
  def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext)

  /**
   * Returns whether the action has already been completed with a value or an exception.
   */
  override def isCompleted: Boolean

  /**
   * Returns whether the action has been cancelled.
   */
  def isCancelled: Boolean

  /**
   * The value of this Future.
   *
   * If the future is not completed the returned value will be None. If the future is completed
   * the value will be Some(Success(t)) if it contains a valid result, or Some(Failure(error)) if
   * it contains an exception.
   */
  override def value: Option[Try[T]]

  /**
   * Blocks and returns the result of this job.
   */
  @throws(classOf[Exception])
  def get(): T = Await.result(this, Duration.Inf)

  /**
   * Returns the job IDs run by the underlying async operation.
   *
   * This returns the current snapshot of the job list. Certain operations may run multiple
   * jobs, so multiple calls to this method may return different lists.
   */
  def jobIds: Seq[Int]

}


/**
 * A [[FutureAction]] holding the result of an action that triggers a single job. Examples include
 * count, collect, reduce.
 */
class SimpleFutureAction[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
  extends FutureAction[T] {

  @volatile private var _cancelled: Boolean = false

  override def cancel() {
    _cancelled = true
    jobWaiter.cancel()
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): SimpleFutureAction.this.type = {
    if (!atMost.isFinite()) {
      awaitResult()
    } else jobWaiter.synchronized {
      val finishTime = System.currentTimeMillis() + atMost.toMillis
      while (!isCompleted) {
        val time = System.currentTimeMillis()
        if (time >= finishTime) {
          throw new TimeoutException
        } else {
          jobWaiter.wait(finishTime - time)
        }
      }
    }
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    ready(atMost)(permit)
    awaitResult() match {
      case scala.util.Success(res) => res
      case scala.util.Failure(e) => throw e
    }
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) {
    executor.execute(new Runnable {
      override def run() {
        func(awaitResult())
      }
    })
  }

  override def isCompleted: Boolean = jobWaiter.jobFinished
  
  override def isCancelled: Boolean = _cancelled

  override def value: Option[Try[T]] = {
    if (jobWaiter.jobFinished) {
      Some(awaitResult())
    } else {
      None
    }
  }

  private def awaitResult(): Try[T] = {
    jobWaiter.awaitResult() match {
      case JobSucceeded => scala.util.Success(resultFunc)
      case JobFailed(e: Exception) => scala.util.Failure(e)
    }
  }

  def jobIds: Seq[Int] = Seq(jobWaiter.jobId)
}


/**
 * A [[FutureAction]] for actions that could trigger multiple Spark jobs. Examples include take,
 * takeSample. Cancellation works by setting the cancelled flag to true and interrupting the
 * action thread if it is being blocked by a job.
 */
class ComplexFutureAction[T] extends FutureAction[T] {

  // Pointer to the thread that is executing the action. It is set when the action is run.
  @volatile private var thread: Thread = _

  // A flag indicating whether the future has been cancelled. This is used in case the future
  // is cancelled before the action was even run (and thus we have no thread to interrupt).
  @volatile private var _cancelled: Boolean = false

  @volatile private var jobs: Seq[Int] = Nil

  // A promise used to signal the future.
  private val p = promise[T]()

  override def cancel(): Unit = this.synchronized {
    _cancelled = true
    if (thread != null) {
      thread.interrupt()
    }
  }

  /**
   * Executes some action enclosed in the closure. To properly enable cancellation, the closure
   * should use runJob implementation in this promise. See takeAsync for example.
   */
  def run(func: => T)(implicit executor: ExecutionContext): this.type = {
    scala.concurrent.future {
      thread = Thread.currentThread
      try {
        p.success(func)
      } catch {
        case e: Exception => p.failure(e)
      } finally {
        // This lock guarantees when calling `thread.interrupt()` in `cancel`,
        // thread won't be set to null.
        ComplexFutureAction.this.synchronized {
          thread = null
        }
      }
    }
    this
  }

  /**
   * Runs a Spark job. This is a wrapper around the same functionality provided by SparkContext
   * to enable cancellation.
   */
  def runJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R) {
    // If the action hasn't been cancelled yet, submit the job. The check and the submitJob
    // command need to be in an atomic block.
    val job = this.synchronized {
      if (!isCancelled) {
        rdd.context.submitJob(rdd, processPartition, partitions, resultHandler, resultFunc)
      } else {
        throw new SparkException("Action has been cancelled")
      }
    }

    this.jobs = jobs ++ job.jobIds

    // Wait for the job to complete. If the action is cancelled (with an interrupt),
    // cancel the job and stop the execution. This is not in a synchronized block because
    // Await.ready eventually waits on the monitor in FutureJob.jobWaiter.
    try {
      Await.ready(job, Duration.Inf)
    } catch {
      case e: InterruptedException =>
        job.cancel()
        throw new SparkException("Action has been cancelled")
    }
  }

  override def isCancelled: Boolean = _cancelled

  @throws(classOf[InterruptedException])
  @throws(classOf[scala.concurrent.TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    p.future.ready(atMost)(permit)
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    p.future.result(atMost)(permit)
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    p.future.onComplete(func)(executor)
  }

  override def isCompleted: Boolean = p.isCompleted

  override def value: Option[Try[T]] = p.future.value

  def jobIds: Seq[Int] = jobs

}

private[spark]
class JavaFutureActionWrapper[S, T](futureAction: FutureAction[S], converter: S => T)
  extends JavaFutureAction[T] {

  import scala.collection.JavaConverters._

  override def isCancelled: Boolean = futureAction.isCancelled

  override def isDone: Boolean = {
    // According to java.util.Future's Javadoc, this returns True if the task was completed,
    // whether that completion was due to successful execution, an exception, or a cancellation.
    futureAction.isCancelled || futureAction.isCompleted
  }

  override def jobIds(): java.util.List[java.lang.Integer] = {
    Collections.unmodifiableList(futureAction.jobIds.map(Integer.valueOf).asJava)
  }

  private def getImpl(timeout: Duration): T = {
    // This will throw TimeoutException on timeout:
    Await.ready(futureAction, timeout)
    futureAction.value.get match {
      case scala.util.Success(value) => converter(value)
      case Failure(exception) =>
        if (isCancelled) {
          throw new CancellationException("Job cancelled").initCause(exception)
        } else {
          // java.util.Future.get() wraps exceptions in ExecutionException
          throw new ExecutionException("Exception thrown by job", exception)
        }
    }
  }

  override def get(): T = getImpl(Duration.Inf)

  override def get(timeout: Long, unit: TimeUnit): T =
    getImpl(Duration.fromNanos(unit.toNanos(timeout)))

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = synchronized {
    if (isDone) {
      // According to java.util.Future's Javadoc, this should return false if the task is completed.
      false
    } else {
      // We're limited in terms of the semantics we can provide here; our cancellation is
      // asynchronous and doesn't provide a mechanism to not cancel if the job is running.
      futureAction.cancel()
      true
    }
  }

}
