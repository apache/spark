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

import scala.util.control.NonFatal

import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKey.{ERROR, POLICY, RETRY_COUNT, WAIT_TIME}
import org.apache.spark.internal.MDC

private[sql] class GrpcRetryHandler(
    private val policies: Seq[RetryPolicy],
    private val sleep: Long => Unit = Thread.sleep) {

  def this(policy: RetryPolicy, sleep: Long => Unit) = this(List(policy), sleep)
  def this(policy: RetryPolicy) = this(policy, Thread.sleep)

  /**
   * Retries the given function with exponential backoff according to the client's retryPolicy.
   */
  def retry[T](fn: => T): T = new GrpcRetryHandler.Retrying(policies, sleep, fn).retry()

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

  /**
   * Class managing the state of the retrying logic during a single retryable block.
   * @param retryPolicies
   *   list of policies to apply (in order)
   * @param sleep
   *   typically Thread.sleep
   * @param fn
   *   the function to compute
   * @tparam T
   *   result of function fn
   */
  class Retrying[T](retryPolicies: Seq[RetryPolicy], sleep: Long => Unit, fn: => T) {
    private var currentRetryNum: Int = 0
    private var exceptionList: Seq[Throwable] = Seq.empty
    private val policies: Seq[RetryPolicy.RetryPolicyState] = retryPolicies.map(_.toState)

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
        // retry exception is considered immediately retriable without any policies.
        logWarning(
          log"Non-Fatal error during RPC execution: ${MDC(ERROR, lastException)}, " +
            log"retrying (currentRetryNum=${MDC(RETRY_COUNT, currentRetryNum)})")
        return
      }

      for (policy <- policies if policy.canRetry(lastException)) {
        val time = policy.nextAttempt()

        if (time.isDefined) {
          logWarning(
            log"Non-Fatal error during RPC execution: ${MDC(ERROR, lastException)}, " +
              log"retrying (wait=${MDC(WAIT_TIME, time.get.toMillis)} ms, " +
              log"currentRetryNum=${MDC(RETRY_COUNT, currentRetryNum)}, " +
              log"policy=${MDC(POLICY, policy.getName)}).")
          sleep(time.get.toMillis)
          return
        }
      }

      logWarning(
        log"Non-Fatal error during RPC execution: ${MDC(ERROR, lastException)}, " +
          log"exceeded retries (currentRetryNum=${MDC(RETRY_COUNT, currentRetryNum)})")

      val error = new RetriesExceeded()
      exceptionList.foreach(error.addSuppressed)
      throw error
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
   * without any policies.
   */
  class RetryException extends Throwable
}
