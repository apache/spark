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

import org.apache.spark.internal.config.ConfigBuilder

/**
 * Spark Integration for Kafka 0.10
 */
package object kafka010 { //scalastyle:ignore

  private[spark] val CONSUMER_CACHE_ENABLED =
    ConfigBuilder("spark.streaming.kafka.consumer.cache.enabled")
      .version("2.2.1")
      .booleanConf
      .createWithDefault(true)

  private[spark] val CONSUMER_POLL_MS =
    ConfigBuilder("spark.streaming.kafka.consumer.poll.ms")
      .version("2.0.1")
      .longConf
      .createOptional

  private[spark] val CONSUMER_CACHE_INITIAL_CAPACITY =
    ConfigBuilder("spark.streaming.kafka.consumer.cache.initialCapacity")
      .version("2.0.1")
      .intConf
      .createWithDefault(16)

  private[spark] val CONSUMER_CACHE_MAX_CAPACITY =
    ConfigBuilder("spark.streaming.kafka.consumer.cache.maxCapacity")
      .version("2.0.1")
      .intConf
      .createWithDefault(64)

  private[spark] val CONSUMER_CACHE_LOAD_FACTOR =
    ConfigBuilder("spark.streaming.kafka.consumer.cache.loadFactor")
      .version("2.0.1")
      .doubleConf
      .createWithDefault(0.75)

  private[spark] val MAX_RATE_PER_PARTITION =
    ConfigBuilder("spark.streaming.kafka.maxRatePerPartition")
      .version("1.3.0")
      .longConf
      .createWithDefault(0)

  private[spark] val MIN_RATE_PER_PARTITION =
    ConfigBuilder("spark.streaming.kafka.minRatePerPartition")
      .version("2.4.0")
      .longConf
      .createWithDefault(1)

  private[spark] val ALLOW_NON_CONSECUTIVE_OFFSETS =
    ConfigBuilder("spark.streaming.kafka.allowNonConsecutiveOffsets")
      .version("2.3.1")
      .booleanConf
      .createWithDefault(false)

}

