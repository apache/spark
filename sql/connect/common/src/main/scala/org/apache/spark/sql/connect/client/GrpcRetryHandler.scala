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

package org.apache.spark.sql.connect.client

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{ELAPSED_TIME, ERROR, NUM_RETRY, POLICY, RETRY_WAIT_TIME}
import org.apache.spark.sql.util.{CloseableIterator, WrappedCloseableIterator}

private[sql] class GrpcRetryHandler(
    private val policies: Seq[RetryPolicy],
    private val sleep: Long => Unit = Thread.sleep,
    private val maxRetryExceptionElapsedTime: FiniteDuration =
      GrpcRetryHandler.DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
    private val nowNanos: () => Long = () => System.nanoTime()) {

  def this(policy: RetryPolicy, sleep: Long => Unit) = this(List(policy), sleep)
  def this(policy: RetryPolicy) = this(policy, Thread.sleep)

  /**
   * Retries the given function with exponential backoff according to the client's retryPolicy.
   */
  def retry[T](fn: => T): T =
    new GrpcRetryHandler.Retrying(policies, sleep, fn, maxRetryExceptionElapsedTime, nowNanos)
      .retry()

  /**
   * Generalizes the retry logic for RPC calls that return an iterator.
   * @param request
   *   The request to send to the server.
   * @param call
   *   The function that calls the RPC.
   * @tparam T
   *   The type of the request.
   * @tparam U
   *   The type of the response.
   */
  class RetryIterator[T, U](request: T, call: T => CloseableIterator[U])
      extends WrappedCloseableIterator[U] {

    private var opened = false // we only retry if it fails on first call when using the iterator
    private var iter = call(request)

    override def innerIterator: Iterator[U] = iter

    private def retryIter[V](f: Iterator[U] => V) = {
      if (!opened) {
        opened = true
        var firstTry = true
        retry {
          if (firstTry) {
            // on first try, we use the initial iterator.
            firstTry = false
          } else {
            // on retry, we need to call the RPC again.
            iter = call(request)
          }
          f(iter)
        }
      } else {
        f(iter)
      }
    }

    override def next(): U = {
      retryIter(_.next())
    }

    override def hasNext: Boolean = {
      retryIter(_.hasNext)
    }

    override def close(): Unit = {
      iter.close()
    }
  }

  object RetryIterator {
    def apply[T, U](request: T, call: T => CloseableIterator[U]): RetryIterator[T, U] =
      new RetryIterator(request, call)
  }

  /**
   * Generalizes the retry logic for RPC calls that return a StreamObserver.
   * @param request
   *   The request to send to the server.
   * @param call
   *   The function that calls the RPC.
   * @tparam T
   *   The type of the request.
   * @tparam U
   *   The type of the response.
   */
  class RetryStreamObserver[T, U](request: T, call: T => StreamObserver[U])
      extends StreamObserver[U] {

    private var opened = false // only retries on first call

    // Note: This is not retried, because no error would ever be thrown here, and GRPC will only
    // throw error on first iterator.hasNext() or iterator.next()
    private var streamObserver = call(request)

    override def onNext(v: U): Unit = {
      if (!opened) {
        opened = true
        var firstTry = true
        retry {
          if (firstTry) {
            // on first try, we use the initial streamObserver.
            firstTry = false
          } else {
            // on retry, we need to call the RPC again.
            streamObserver = call(request)
          }
          streamObserver.onNext(v)
        }
      } else {
        streamObserver.onNext(v)
      }
    }

    override def onError(throwable: Throwable): Unit = {
      opened = true
      streamObserver.onError(throwable)
    }

    override def onCompleted(): Unit = {
      opened = true
      streamObserver.onCompleted()
    }
  }

  object RetryStreamObserver {
    def apply[T, U](request: T, call: T => StreamObserver[U]): RetryStreamObserver[T, U] =
      new RetryStreamObserver(request, call)
  }
}

private[sql] object GrpcRetryHandler extends Logging {

  // Default cumulative elapsed-time bound for RetryException-driven retries (see
  // Retrying.waitAfterAttempt below). Single source of truth for this default; please keep the
  // Python side (retries.py's DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME) in sync with this value.
  val DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME: FiniteDuration = FiniteDuration(1, "hour")

  /**
   * Class managing the state of the retrying logic during a single retryable block.
   * @param retryPolicies
   *   list of policies to apply (in order)
   * @param sleep
   *   typically Thread.sleep
   * @param fn
   *   the function to compute
   * @param maxRetryExceptionElapsedTime
   *   maximum cumulative wall-clock time to keep retrying a [[RetryException]] (which is
   *   otherwise retried unconditionally, without consulting any [[RetryPolicy]]) before giving
   *   up. Measured from the first [[RetryException]] seen by this instance.
   * @param nowNanos
   *   typically System.nanoTime, used to measure maxRetryExceptionElapsedTime
   * @tparam T
   *   result of function fn
   */
  class Retrying[T](
      retryPolicies: Seq[RetryPolicy],
      sleep: Long => Unit,
      fn: => T,
      maxRetryExceptionElapsedTime: FiniteDuration = DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
      nowNanos: () => Long = () => System.nanoTime()) {
    private var currentRetryNum: Int = 0
    private var exceptionList: Seq[Throwable] = Seq.empty
    private val policies: Seq[RetryPolicy.RetryPolicyState] = retryPolicies.map(_.toState)

    // Set to the result of nowNanos() when the first RetryException is observed; used to bound
    // the total time spent in RetryException-driven retries (see waitAfterAttempt below).
    private var retryExceptionStartNanos: Option[Long] = None

    def canRetry(throwable: Throwable): Boolean = {
      throwable.isInstanceOf[RetryException] || policies.exists(p => p.canRetry(throwable))
    }

    def makeAttempt(): Option[T] = {
      try {
        Some(fn)
      } catch {
        case NonFatal(e) if canRetry(e) =>
          currentRetryNum += 1
          exceptionList = e +: exceptionList
          None
      }
    }

    def waitAfterAttempt(): Unit = {
      // find policy which will accept this exception
      val lastException = exceptionList.head

      if (lastException.isInstanceOf[RetryException]) {
        val startNanos = retryExceptionStartNanos.getOrElse {
          val now = nowNanos()
          retryExceptionStartNanos = Some(now)
          now
        }
        val elapsedNanos = nowNanos() - startNanos
        if (elapsedNanos >= maxRetryExceptionElapsedTime.toNanos) {
          logWarning(
            log"Non-Fatal error during RPC execution: ${MDC(ERROR, lastException)}, " +
              log"exceeded maxRetryExceptionElapsedTime " +
              log"(elapsed=${MDC(ELAPSED_TIME, elapsedNanos / 1000000)} ms, " +
              log"currentRetryNum=${MDC(NUM_RETRY, currentRetryNum)})")
          logWarning(log"[RETRIES_EXCEEDED] The maximum number of retries has been exceeded.")
          // Unwrap the underlying cause (both throw sites in
          // ExecutePlanResponseReattachableIterator attach it via addSuppressed) so the
          // caller sees a real, actionable error instead of the bare RetryException marker.
          throw lastException.getSuppressed.headOption.getOrElse(lastException)
        }
        // retry exception is considered immediately retriable without any policies, as long
        // as the cumulative elapsed time above has not been exceeded.
        logWarning(
          log"Non-Fatal error during RPC execution: ${MDC(ERROR, lastException)}, " +
            log"retrying (currentRetryNum=${MDC(NUM_RETRY, currentRetryNum)})")
        return
      }

      // find a policy to wait with
      val matchedPolicyOpt = policies.find(_.canRetry(lastException))
      if (matchedPolicyOpt.isDefined) {
        val matchedPolicy = matchedPolicyOpt.get
        val time = matchedPolicy.nextAttempt(lastException)
        if (time.isDefined) {
          logWarning(
            log"Non-Fatal error during RPC execution: ${MDC(ERROR, lastException)}, " +
              log"retrying (wait=${MDC(RETRY_WAIT_TIME, time.get.toMillis)} ms, " +
              log"currentRetryNum=${MDC(NUM_RETRY, currentRetryNum)}, " +
              log"policy=${MDC(POLICY, matchedPolicy.getName)}).")
          sleep(time.get.toMillis)
          return
        }
      }

      logWarning(
        log"Non-Fatal error during RPC execution: ${MDC(ERROR, lastException)}, " +
          log"exceeded retries (currentRetryNum=${MDC(NUM_RETRY, currentRetryNum)})")

      logWarning(log"[RETRIES_EXCEEDED] The maximum number of retries has been exceeded.")
      throw lastException
    }

    def retry(): T = {
      var result = makeAttempt()

      while (result.isEmpty) {
        waitAfterAttempt()
        result = makeAttempt()
      }

      result.get
    }
  }

  /**
   * An exception that can be thrown upstream when inside retry and which will be always retryable
   * without any policies, bounded by Retrying's maxRetryExceptionElapsedTime.
   */
  class RetryException extends Throwable
}
