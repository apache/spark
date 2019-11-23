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

package org.apache.spark.streaming.kinesis

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.streaming.StreamingContext

/**
 * Configurations to pass to the [[KinesisBackedBlockRDD]].
 *
 * @param maxRetries: The maximum number of attempts to be made to Kinesis. Defaults to 3.
 * @param retryWaitTimeMs: The interval between consequent Kinesis retries.
 *                         Defaults to 100ms.
 * @param retryTimeoutMs: The timeout in milliseconds for a Kinesis request.
 *                         Defaults to batch duration provided for streaming,
 *                         else uses 10000 if invoked directly.
 */
private[kinesis] case class KinesisReadConfigurations(
    maxRetries: Int,
    retryWaitTimeMs: Long,
    retryTimeoutMs: Long)

private[kinesis] object KinesisReadConfigurations {
  def apply(): KinesisReadConfigurations = {
    KinesisReadConfigurations(maxRetries = DEFAULT_MAX_RETRIES,
      retryWaitTimeMs = JavaUtils.timeStringAsMs(DEFAULT_RETRY_WAIT_TIME),
      retryTimeoutMs = DEFAULT_RETRY_TIMEOUT)
  }

  def apply(ssc: StreamingContext): KinesisReadConfigurations = {
    KinesisReadConfigurations(
      maxRetries = ssc.sc.getConf.getInt(RETRY_MAX_ATTEMPTS_KEY, DEFAULT_MAX_RETRIES),
      retryWaitTimeMs = JavaUtils.timeStringAsMs(
        ssc.sc.getConf.get(RETRY_WAIT_TIME_KEY, DEFAULT_RETRY_WAIT_TIME)),
      retryTimeoutMs = ssc.graph.batchDuration.milliseconds)
  }

  /**
   * SparkConf key for configuring the maximum number of retries used when attempting a Kinesis
   * request.
   */
  val RETRY_MAX_ATTEMPTS_KEY = "spark.streaming.kinesis.retry.maxAttempts"

  /**
   * SparkConf key for configuring the wait time to use before retrying a Kinesis attempt.
   */
  val RETRY_WAIT_TIME_KEY = "spark.streaming.kinesis.retry.waitTime"

  /**
   * Default value for the RETRY_MAX_ATTEMPTS_KEY
   */
  val DEFAULT_MAX_RETRIES = 3

  /**
   * Default value for the RETRY_WAIT_TIME_KEY
   */
  val DEFAULT_RETRY_WAIT_TIME = "100ms"

  /**
   * Default value for the retry timeout
   */
  val DEFAULT_RETRY_TIMEOUT = 10000
}
