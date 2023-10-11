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

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random
import scala.util.control.NonFatal

import io.grpc.{Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging

private[sql] class GrpcRetryHandler(
    private val retryPolicy: GrpcRetryHandler.RetryPolicy,
    private val sleep: Long => Unit = Thread.sleep) {

  /**
   * Retries the given function with exponential backoff according to the client's retryPolicy.
   */
  def retry[T](fn: => T): T =
    GrpcRetryHandler.retry(retryPolicy, sleep)(fn)

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

    override def next: U = {
      retryIter(_.next)
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
   * Retries the given function with exponential backoff according to the client's retryPolicy.
   *
   * @param retryPolicy
   *   The retry policy
   * @param sleep
   *   The function which sleeps (takes number of milliseconds to sleep)
   * @param fn
   *   The function to retry.
   * @tparam T
   *   The return type of the function.
   * @return
   *   The result of the function.
   */
  final def retry[T](retryPolicy: RetryPolicy, sleep: Long => Unit = Thread.sleep)(
      fn: => T): T = {
    var currentRetryNum = 0
    var exceptionList: Seq[Throwable] = Seq.empty
    var nextBackoff: Duration = retryPolicy.initialBackoff

    if (retryPolicy.maxRetries < 0) {
      throw new IllegalArgumentException("Can't have negative number of retries")
    }

    while (currentRetryNum <= retryPolicy.maxRetries) {
      if (currentRetryNum != 0) {
        var currentBackoff = nextBackoff
        nextBackoff = nextBackoff * retryPolicy.backoffMultiplier min retryPolicy.maxBackoff

        if (currentBackoff >= retryPolicy.minJitterThreshold) {
          currentBackoff += Random.nextDouble() * retryPolicy.jitter
        }

        sleep(currentBackoff.toMillis)
      }

      try {
        return fn
      } catch {
        case NonFatal(e) if retryPolicy.canRetry(e) && currentRetryNum < retryPolicy.maxRetries =>
          currentRetryNum += 1
          exceptionList = e +: exceptionList

          if (currentRetryNum <= retryPolicy.maxRetries) {
            logWarning(
              s"Non-Fatal error during RPC execution: $e, " +
                s"retrying (currentRetryNum=$currentRetryNum)")
          } else {
            logWarning(
              s"Non-Fatal error during RPC execution: $e, " +
                s"exceeded retries (currentRetryNum=$currentRetryNum)")
          }
      }
    }

    val exception = exceptionList.head
    exceptionList.tail.foreach(exception.addSuppressed(_))
    throw exception
  }

  /**
   * Default canRetry in [[RetryPolicy]].
   * @param e
   *   The exception to check.
   * @return
   *   true if the exception is a [[StatusRuntimeException]] with code UNAVAILABLE.
   */
  private[client] def retryException(e: Throwable): Boolean = {
    e match {
      case e: StatusRuntimeException =>
        val statusCode: Status.Code = e.getStatus.getCode

        if (statusCode == Status.Code.INTERNAL) {
          val msg: String = e.toString

          // This error happens if another RPC preempts this RPC.
          if (msg.contains("INVALID_CURSOR.DISCONNECTED")) {
            return true
          }
        }

        if (statusCode == Status.Code.UNAVAILABLE) {
          return true
        }
        false
      case _ => false
    }
  }

  /**
   * [[RetryPolicy]] configure the retry mechanism in [[GrpcRetryHandler]]
   *
   * @param maxRetries
   *   Maximum number of retries.
   * @param initialBackoff
   *   Start value of the exponential backoff (ms).
   * @param maxBackoff
   *   Maximal value of the exponential backoff (ms).
   * @param backoffMultiplier
   *   Multiplicative base of the exponential backoff.
   * @param canRetry
   *   Function that determines whether a retry is to be performed in the event of an error.
   */
  case class RetryPolicy(
      // Please synchronize changes here with Python side:
      // pyspark/sql/connect/client/core.py
      //
      // Note: these constants are selected so that the maximum tolerated wait is guaranteed
      // to be at least 10 minutes
      maxRetries: Int = 15,
      initialBackoff: FiniteDuration = FiniteDuration(50, "ms"),
      maxBackoff: FiniteDuration = FiniteDuration(1, "min"),
      backoffMultiplier: Double = 4.0,
      jitter: FiniteDuration = FiniteDuration(500, "ms"),
      minJitterThreshold: FiniteDuration = FiniteDuration(2, "s"),
      canRetry: Throwable => Boolean = retryException) {}

  /**
   * An exception that can be thrown upstream when inside retry and which will be retryable
   * regardless of policy.
   */
  class RetryException extends Throwable
}
