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

object StreamingConf {

  private[streaming] val BACKPRESSURE_ENABLED =
    ConfigBuilder("spark.streaming.backpressure.enabled")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val RECEIVER_MAX_RATE =
    ConfigBuilder("spark.streaming.receiver.maxRate")
      .longConf
      .createWithDefault(Long.MaxValue)

  private[streaming] val BACKPRESSURE_INITIAL_RATE =
    ConfigBuilder("spark.streaming.backpressure.initialRate")
      .longConf
      .createOptional

  val BLOCK_INTERVAL =
    ConfigBuilder("spark.streaming.blockInterval")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("200ms")

  private[streaming] val RECEIVER_WAL_ENABLE_CONF_KEY =
    ConfigBuilder("spark.streaming.receiver.writeAheadLog.enable")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val STREAMING_UNPERSIST =
    ConfigBuilder("spark.streaming.unpersist")
      .booleanConf
      .createWithDefault(true)

  private[streaming] val STOP_GRACEFULLY_ON_SHUTDOWN =
    ConfigBuilder("spark.streaming.stopGracefullyOnShutdown")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val UI_RETAINED_BATCHES =
    ConfigBuilder("spark.streaming.ui.retainedBatches")
      .intConf
      .createWithDefault(1000)

  private[streaming] val DRIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY =
    ConfigBuilder("spark.streaming.driver.writeAheadLog.closeFileAfterWrite")
      .booleanConf
      .createWithDefault(false)

  private[streaming] val RECEIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY =
    ConfigBuilder("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite")
      .booleanConf
      .createWithDefault(false)

}
