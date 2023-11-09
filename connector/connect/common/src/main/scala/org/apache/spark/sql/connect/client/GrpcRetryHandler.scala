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
    private val policies: Seq[GrpcRetryHandler.RetryPolicy],
    private val sleep: Long => Unit = Thread.sleep) {

  def this(policy: GrpcRetryHandler.RetryPolicy, sleep: Long => Unit) = this(List(policy), sleep)
  def this(policy: GrpcRetryHandler.RetryPolicy) = this(policy, Thread.sleep)

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

  class Retrying[T](retryPolicies: Seq[RetryPolicy], sleep: Long => Unit, fn: => T) {
    private var currentRetryNum: Int = 0
    private var exceptionList: Seq[Throwable] = Seq.empty
    private val policies: Seq[RetryPolicyState] = retryPolicies.map(_.toState)
    private var result: Option[T] = None

    def canRetry(throwable: Throwable): Boolean = {
      return policies.exists(p => p.canRetry(throwable))
    }

    def makeAttempt(): Unit = {
      try {
        result = Some(fn)
      } catch {
        case NonFatal(e) if canRetry(e) =>
          currentRetryNum += 1
          exceptionList = e +: exceptionList
      }
    }

    def waitAfterAttempt(): Unit = {
      val lastException = exceptionList.head

      for (policy <- policies if policy.canRetry(lastException)) {
        val time = policy.nextAttempt()

        if (time.isDefined) {
          logWarning(s"Non-Fatal error during RPC execution: $lastException, retrying " +
            s"(wait=${time.get.toMillis}, currentRetryNum=$currentRetryNum, " +
            s"policy: ${policy.getName})")

          sleep(time.get.toMillis)
          return
        }
      }

      logWarning(s"Non-Fatal error during RPC execution: $lastException, exceeded retries " +
        s"(currentRetryNum=$currentRetryNum)")

      exceptionList.tail.foreach(lastException.addSuppressed)
      throw lastException
    }

    def retry(): T = {
      makeAttempt()

      while (result.isEmpty) {
        waitAfterAttempt()
        makeAttempt()
      }

      return result.get
    }
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
      maxRetries: Option[Int] = None,
      initialBackoff: FiniteDuration = FiniteDuration(1000, "ms"),
      maxBackoff: Option[FiniteDuration] = None,
      backoffMultiplier: Double = 1.0,
      jitter: FiniteDuration = FiniteDuration(0, "s"),
      minJitterThreshold: FiniteDuration = FiniteDuration(0, "s"),
      canRetry: Throwable => Boolean = (_ => false),
      name: String = this.getClass.getName) {

    def getName: String = name
    def toState: RetryPolicyState = new RetryPolicyState(this)
  }

  def defaultPolicy(): RetryPolicy = RetryPolicy(name = "DefaultPolicy",
    // Please synchronize changes here with Python side:
    // pyspark/sql/connect/client/core.py
    //
    // Note: these constants are selected so that the maximum tolerated wait is guaranteed
    // to be at least 10 minutes
    maxRetries = Some(15),
    initialBackoff = FiniteDuration(50, "ms"),
    maxBackoff = Some(FiniteDuration(1, "min")),
    backoffMultiplier = 4.0,
    jitter = FiniteDuration(500, "ms"),
    minJitterThreshold = FiniteDuration(2, "s"),
    canRetry = retryException
  )

  class RetryPolicyState(val policy: RetryPolicy) {
    private var numberAttempts = 0
    private var nextWait: Duration = policy.initialBackoff

    def nextAttempt(): Option[Duration] = {
      if (policy.maxRetries.isDefined && numberAttempts >= policy.maxRetries.get) {
        return None
      }

      numberAttempts += 1

      var currentWait = nextWait
      nextWait = nextWait * policy.backoffMultiplier
      if (policy.maxBackoff.isDefined) {
        nextWait = nextWait min policy.maxBackoff.get
      }

      if (currentWait >= policy.minJitterThreshold) {
        currentWait += Random.nextDouble() * policy.jitter
      }

      return Some(currentWait)
    }

    def canRetry(throwable: Throwable): Boolean = policy.canRetry(throwable)
    def getName: String = policy.getName
  }

  /**
   * An exception that can be thrown upstream when inside retry and which will be retryable
   * regardless of policy.
   */
  class RetryException extends Throwable
}
