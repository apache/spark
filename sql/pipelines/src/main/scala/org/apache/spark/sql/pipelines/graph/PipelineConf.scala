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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * Configuration for the pipeline system, which is read from the Spark session's SQL configuration.
 */
@deprecated("TODO(SPARK-52410): Remove this class in favor of using SqlConf directly")
class PipelineConf(spark: SparkSession) {
  private val sqlConf: SQLConf = spark.sessionState.conf

  /** Interval in milliseconds to poll the state of streaming flow execution. */
  val streamStatePollingInterval: Long = sqlConf.getConf(
    SQLConf.PIPELINES_STREAM_STATE_POLLING_INTERVAL
  )

  /** Minimum time in seconds between retries for the watchdog. */
  val watchdogMinRetryTimeInSeconds: Long = {
    sqlConf.getConf(SQLConf.PIPELINES_WATCHDOG_MIN_RETRY_TIME_IN_SECONDS)
  }

  /** Maximum time in seconds for the watchdog to retry before giving up. */
  val watchdogMaxRetryTimeInSeconds: Long = {
    val value = sqlConf.getConf(SQLConf.PIPELINES_WATCHDOG_MAX_RETRY_TIME_IN_SECONDS)
    // TODO(SPARK-52410): Remove this check and use `checkValue` when defining the conf
    //                    in `SqlConf`.
    if (value < watchdogMinRetryTimeInSeconds) {
      throw new IllegalArgumentException(
        "Watchdog maximum retry time must be greater than or equal to the watchdog minimum " +
        "retry time."
      )
    }
    value
  }

  /** Maximum number of concurrent flows that can be executed. */
  val maxConcurrentFlows: Int = sqlConf.getConf(SQLConf.PIPELINES_MAX_CONCURRENT_FLOWS)

  /** Timeout in milliseconds for termination join and lock operations. */
  val timeoutMsForTerminationJoinAndLock: Long = {
    sqlConf.getConf(SQLConf.PIPELINES_TIMEOUT_MS_FOR_TERMINATION_JOIN_AND_LOCK)
  }

  /** Maximum number of retry attempts for a flow execution. */
  val maxFlowRetryAttempts: Int = sqlConf.getConf(SQLConf.PIPELINES_MAX_FLOW_RETRY_ATTEMPTS)
}
