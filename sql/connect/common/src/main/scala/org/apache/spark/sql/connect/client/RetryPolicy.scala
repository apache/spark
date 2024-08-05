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

import io.grpc.{Status, StatusRuntimeException}

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
    canRetry: Throwable => Boolean,
    name: String) {

  def getName: String = name

  def toState: RetryPolicy.RetryPolicyState = new RetryPolicy.RetryPolicyState(this)
}

object RetryPolicy {
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
    canRetry = defaultPolicyRetryException)

  // list of policies to be used by this client
  def defaultPolicies(): Seq[RetryPolicy] = List(defaultPolicy())

  // represents a state of the specific policy
  // (how many retries have happened and how much to wait until next one)
  private[client] class RetryPolicyState(val policy: RetryPolicy) {
    private var numberAttempts = 0
    private var nextWait: Duration = policy.initialBackoff

    // return waiting time until next attempt, or None if has exceeded max retries
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
        false
      case _ => false
    }
  }
}
