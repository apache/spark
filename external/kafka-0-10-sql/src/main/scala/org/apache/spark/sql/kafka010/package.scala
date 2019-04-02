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
package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.config.ConfigBuilder

package object kafka010 {   // scalastyle:ignore
  // ^^ scalastyle:ignore is for ignoring warnings about digits in package name
  type PartitionOffsetMap = Map[TopicPartition, Long]

  private[spark] val PRODUCER_CACHE_TIMEOUT =
    ConfigBuilder("spark.kafka.producer.cache.timeout")
      .doc("The time to remove the producer when the producer is not used.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("600000")

  val CONSUMER_POLL_TIMEOUT =
    ConfigBuilder("kafkaConsumer.pollTimeoutMs")
      .doc("The timeout in milliseconds to poll data from Kafka in executors.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("512")

  val CONSUMER_CACHE_CAPACITY =
    ConfigBuilder("spark.sql.kafkaConsumerCache.capacity")
      .doc("The size of LinkedHashMap for caching kafkaConsumers.")
      .intConf
      .createWithDefaultString("64")

  val NETWORK_TIMEOUT =
    ConfigBuilder("spark.network.timeout")
      .doc("The timeout of network.")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  val MAX_OFFSET_PER_TRIGGER =
    ConfigBuilder("maxOffsetsPerTrigger")
      .doc("Rate limit on maximum number of offsets processed per trigger interval. " +
        "The specified total number of offsets will be " +
        "proportionally split across topicPartitions of different volume.")
      .longConf
      .createOptional

  val FETCH_OFFSET_NUM_RETRY =
    ConfigBuilder("fetchOffset.numRetries")
      .doc("Retry at most `maxOffsetFetchAttempts` times when offsets are incorrect.")
      .intConf
      .createWithDefaultString("3")

  val FETCH_OFFSET_RETRY_INTERVAL_MS =
    ConfigBuilder("fetchOffset.retryIntervalMs")
      .doc("The interval time for retrying to fetch offsets.")
      .longConf
      .createWithDefaultString("1000")

 }
