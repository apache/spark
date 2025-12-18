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
import scala.jdk.CollectionConverters._
import scala.util.Random

import com.google.rpc.RetryInfo
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto

import org.apache.spark.internal.Logging

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
 * @param jitter
 *   Sample a random value uniformly from the range [0, jitter] and add it to the backoff.
 * @param minJitterThreshold
 *   Minimal value of the backoff to add random jitter.
 * @param canRetry
 *   Function that determines whether a retry is to be performed in the event of an error.
 * @param name
 *   Name of the policy.
 * @param recognizeServerRetryDelay
 *   Per gRPC standard, the server can send error messages that contain `RetryInfo` message with
 *   `retry_delay` field indicating that the client should wait for at least `retry_delay` amount
 *   of time before retrying again, see:
 *   https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto#L91
 *
 * If this flag is set to true, RetryPolicy will use `RetryInfo.retry_delay` field in the backoff
 * computation. Server's `retry_delay` can override client's `maxBackoff`.
 *
 * This flag does not change which errors are retried, only how the backoff is computed.
 * `DefaultPolicy` additionally has a rule for retrying any error that contains `RetryInfo`.
 * @param maxServerRetryDelay
 *   Limit for the server-provided `retry_delay`.
 */
case class RetryPolicy(
    maxRetries: Option[Int] = None,
    initialBackoff: FiniteDuration = FiniteDuration(1000, "ms"),
    maxBackoff: Option[FiniteDuration] = None,
    backoffMultiplier: Double = 1.0,
    jitter: FiniteDuration = FiniteDuration(0, "s"),
    minJitterThreshold: FiniteDuration = FiniteDuration(0, "s"),
    canRetry: Throwable => Boolean,
    name: String,
    recognizeServerRetryDelay: Boolean = false,
    maxServerRetryDelay: Option[FiniteDuration] = None) {

  def getName: String = name

  def toState: RetryPolicy.RetryPolicyState = new RetryPolicy.RetryPolicyState(this)
}

object RetryPolicy extends Logging {
  def defaultPolicy(): RetryPolicy = RetryPolicy(
    name = "DefaultPolicy",
    // Please synchronize changes here with Python side:
    // pyspark/sql/connect/client/retries.py
    //
    // Note: these constants are selected so that the maximum tolerated wait is guaranteed
    // to be at least 10 minutes
    maxRetries = Some(15),
    initialBackoff = FiniteDuration(50, "ms"),
    maxBackoff = Some(FiniteDuration(1, "min")),
    backoffMultiplier = 4.0,
    jitter = FiniteDuration(500, "ms"),
    minJitterThreshold = FiniteDuration(2, "s"),
    canRetry = defaultPolicyRetryException,
    recognizeServerRetryDelay = true,
    maxServerRetryDelay = Some(FiniteDuration(10, "min")))

  // list of policies to be used by this client
  def defaultPolicies(): Seq[RetryPolicy] = List(defaultPolicy())

  // represents a state of the specific policy
  // (how many retries have happened and how much to wait until next one)
  private[client] class RetryPolicyState(val policy: RetryPolicy) {
    private var numberAttempts = 0
    private var nextWait: Duration = policy.initialBackoff

    // return waiting time until next attempt, or None if has exceeded max retries
    def nextAttempt(e: Throwable): Option[Duration] = {
      if (policy.maxRetries.isDefined && numberAttempts >= policy.maxRetries.get) {
        return None
      }

      numberAttempts += 1

      var currentWait = nextWait
      nextWait = nextWait * policy.backoffMultiplier
      if (policy.maxBackoff.isDefined) {
        nextWait = nextWait min policy.maxBackoff.get
      }

      if (policy.recognizeServerRetryDelay) {
        extractRetryDelay(e).foreach { retryDelay =>
          logDebug(s"The server has sent a retry delay of $retryDelay ms.")
          val retryDelayLimited = retryDelay min policy.maxServerRetryDelay.getOrElse(retryDelay)
          currentWait = currentWait max retryDelayLimited
        }
      }

      if (currentWait >= policy.minJitterThreshold) {
        currentWait += Random.nextDouble() * policy.jitter
      }

      Some(currentWait)
    }

    def canRetry(throwable: Throwable): Boolean = policy.canRetry(throwable)

    def getName: String = policy.getName
  }

  /**
   * Default canRetry in [[RetryPolicy]].
   *
   * @param e
   *   The exception to check.
   * @return
   *   true if the exception is a [[StatusRuntimeException]] with code UNAVAILABLE.
   */
  private[client] def defaultPolicyRetryException(e: Throwable): Boolean = {
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

        // All errors messages containing `RetryInfo` should be retried.
        if (extractRetryInfo(e).isDefined) {
          return true
        }

        false
      case _ => false
    }
  }

  private def extractRetryInfo(e: Throwable): Option[RetryInfo] = {
    e match {
      case e: StatusRuntimeException =>
        Option(StatusProto.fromThrowable(e))
          .flatMap(status =>
            status.getDetailsList.asScala
              .find(_.is(classOf[RetryInfo]))
              .map(_.unpack(classOf[RetryInfo])))
      case _ => None
    }
  }

  private def extractRetryDelay(e: Throwable): Option[FiniteDuration] = {
    extractRetryInfo(e)
      .flatMap(retryInfo => Option(retryInfo.getRetryDelay))
      .map(retryDelay =>
        FiniteDuration(retryDelay.getSeconds, "s") + FiniteDuration(retryDelay.getNanos, "ns"))
  }
}
