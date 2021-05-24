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

package org.apache.spark.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.streaming.util.OpenHashMapBasedStateMap.DELTA_CHAIN_LENGTH_THRESHOLD

object StreamingConf {

  private[streaming] val BACKPRESSURE_ENABLED =
    ConfigBuilder("spark.streaming.backpressure.enabled")
      .version("1.5.0")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val RECEIVER_MAX_RATE =
    ConfigBuilder("spark.streaming.receiver.maxRate")
      .version("1.0.2")
      .longConf
      .createWithDefault(Long.MaxValue)

  private[streaming] val BACKPRESSURE_INITIAL_RATE =
    ConfigBuilder("spark.streaming.backpressure.initialRate")
      .version("2.0.0")
      .fallbackConf(RECEIVER_MAX_RATE)

  private[streaming] val BLOCK_INTERVAL =
    ConfigBuilder("spark.streaming.blockInterval")
      .version("0.8.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("200ms")

  private[streaming] val RECEIVER_WAL_ENABLE_CONF_KEY =
    ConfigBuilder("spark.streaming.receiver.writeAheadLog.enable")
      .version("1.2.1")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val RECEIVER_WAL_CLASS_CONF_KEY =
    ConfigBuilder("spark.streaming.receiver.writeAheadLog.class")
      .version("1.4.0")
      .stringConf
      .createOptional

  private[streaming] val RECEIVER_WAL_ROLLING_INTERVAL_CONF_KEY =
    ConfigBuilder("spark.streaming.receiver.writeAheadLog.rollingIntervalSecs")
      .version("1.4.0")
      .intConf
      .createWithDefault(60)

  private[streaming] val RECEIVER_WAL_MAX_FAILURES_CONF_KEY =
    ConfigBuilder("spark.streaming.receiver.writeAheadLog.maxFailures")
      .version("1.2.0")
      .intConf
      .createWithDefault(3)

  private[streaming] val RECEIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY =
    ConfigBuilder("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val DRIVER_WAL_CLASS_CONF_KEY =
    ConfigBuilder("spark.streaming.driver.writeAheadLog.class")
      .version("1.4.0")
      .stringConf
      .createOptional

  private[streaming] val DRIVER_WAL_ROLLING_INTERVAL_CONF_KEY =
    ConfigBuilder("spark.streaming.driver.writeAheadLog.rollingIntervalSecs")
      .version("1.4.0")
      .intConf
      .createWithDefault(60)

  private[streaming] val DRIVER_WAL_MAX_FAILURES_CONF_KEY =
    ConfigBuilder("spark.streaming.driver.writeAheadLog.maxFailures")
      .version("1.4.0")
      .intConf
      .createWithDefault(3)

  private[streaming] val DRIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY =
    ConfigBuilder("spark.streaming.driver.writeAheadLog.closeFileAfterWrite")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val DRIVER_WAL_BATCHING_CONF_KEY =
    ConfigBuilder("spark.streaming.driver.writeAheadLog.allowBatching")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  private[streaming] val DRIVER_WAL_BATCHING_TIMEOUT_CONF_KEY =
    ConfigBuilder("spark.streaming.driver.writeAheadLog.batchingTimeout")
      .version("1.6.0")
      .longConf
      .createWithDefault(5000)

  private[streaming] val STREAMING_UNPERSIST =
    ConfigBuilder("spark.streaming.unpersist")
      .version("0.9.0")
      .booleanConf
      .createWithDefault(true)

  private[streaming] val STOP_GRACEFULLY_ON_SHUTDOWN =
    ConfigBuilder("spark.streaming.stopGracefullyOnShutdown")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val UI_RETAINED_BATCHES =
    ConfigBuilder("spark.streaming.ui.retainedBatches")
      .version("1.0.0")
      .intConf
      .createWithDefault(1000)

  private[streaming] val SESSION_BY_KEY_DELTA_CHAIN_THRESHOLD =
    ConfigBuilder("spark.streaming.sessionByKey.deltaChainThreshold")
      .version("1.6.0")
      .intConf
      .createWithDefault(DELTA_CHAIN_LENGTH_THRESHOLD)

  private[streaming] val BACKPRESSURE_RATE_ESTIMATOR =
    ConfigBuilder("spark.streaming.backpressure.rateEstimator")
      .version("1.5.0")
      .stringConf
      .createWithDefault("pid")

  private[streaming] val BACKPRESSURE_PID_PROPORTIONAL =
    ConfigBuilder("spark.streaming.backpressure.pid.proportional")
      .version("1.5.0")
      .doubleConf
      .createWithDefault(1.0)

  private[streaming] val BACKPRESSURE_PID_INTEGRAL =
    ConfigBuilder("spark.streaming.backpressure.pid.integral")
      .version("1.5.0")
      .doubleConf
      .createWithDefault(0.2)

  private[streaming] val BACKPRESSURE_PID_DERIVED =
    ConfigBuilder("spark.streaming.backpressure.pid.derived")
      .version("1.5.0")
      .doubleConf
      .createWithDefault(0.0)

  private[streaming] val BACKPRESSURE_PID_MIN_RATE =
    ConfigBuilder("spark.streaming.backpressure.pid.minRate")
      .version("1.5.0")
      .doubleConf
      .createWithDefault(100)

  private[streaming] val CONCURRENT_JOBS =
    ConfigBuilder("spark.streaming.concurrentJobs")
      .version("0.7.0")
      .intConf
      .createWithDefault(1)

  private[streaming] val GRACEFUL_STOP_TIMEOUT =
    ConfigBuilder("spark.streaming.gracefulStopTimeout")
      .version("1.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[streaming] val MANUAL_CLOCK_JUMP =
    ConfigBuilder("spark.streaming.manualClock.jump")
      .version("0.7.0")
      .longConf
      .createWithDefault(0)

}
