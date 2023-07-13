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

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import io.grpc.{Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver

private[client] class GrpcRetryHandler(private val retryPolicy: GrpcRetryHandler.RetryPolicy) {

  /**
   * Retries the given function with exponential backoff according to the client's retryPolicy.
   * @param fn
   *   The function to retry.
   * @param currentRetryNum
   *   Current number of retries.
   * @tparam T
   *   The return type of the function.
   * @return
   *   The result of the function.
   */
  @tailrec final def retry[T](fn: => T, currentRetryNum: Int = 0): T = {
    if (currentRetryNum > retryPolicy.maxRetries) {
      throw new IllegalArgumentException(
        s"The number of retries ($currentRetryNum) must not exceed " +
          s"the maximum number of retires (${retryPolicy.maxRetries}).")
    }
    try {
      return fn
    } catch {
      case NonFatal(e) if retryPolicy.canRetry(e) && currentRetryNum < retryPolicy.maxRetries =>
        Thread.sleep(
          (retryPolicy.maxBackoff min retryPolicy.initialBackoff * Math
            .pow(retryPolicy.backoffMultiplier, currentRetryNum)).toMillis)
    }
    retry(fn, currentRetryNum + 1)
  }

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
  class RetryIterator[T, U](request: T, call: T => java.util.Iterator[U])
      extends java.util.Iterator[U] {

    private var opened = false // we only retry if it fails on first call when using the iterator
    private var iterator = call(request)

    private def retryIter[V](f: java.util.Iterator[U] => V) = {
      if (!opened) {
        opened = true
        var firstTry = true
        retry {
          if (firstTry) {
            // on first try, we use the initial iterator.
            firstTry = false
          } else {
            // on retry, we need to call the RPC again.
            iterator = call(request)
          }
          f(iterator)
        }
      } else {
        f(iterator)
      }
    }

    override def next: U = {
      retryIter(_.next())
    }

    override def hasNext: Boolean = {
      retryIter(_.hasNext())
    }
  }

  object RetryIterator {
    def apply[T, U](request: T, call: T => java.util.Iterator[U]): RetryIterator[T, U] =
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

private[client] object GrpcRetryHandler {

  /**
   * Default canRetry in [[RetryPolicy]].
   * @param e
   *   The exception to check.
   * @return
   *   true if the exception is a [[StatusRuntimeException]] with code UNAVAILABLE.
   */
  private[client] def retryException(e: Throwable): Boolean = {
    e match {
      case e: StatusRuntimeException => e.getStatus.getCode == Status.Code.UNAVAILABLE
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
      maxRetries: Int = 15,
      initialBackoff: FiniteDuration = FiniteDuration(50, "ms"),
      maxBackoff: FiniteDuration = FiniteDuration(1, "min"),
      backoffMultiplier: Double = 4.0,
      canRetry: Throwable => Boolean = retryException) {}
}
