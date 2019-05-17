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

  private[kafka010] val PRODUCER_CACHE_TIMEOUT =
    ConfigBuilder("spark.kafka.producer.cache.timeout")
      .doc("The expire time to remove the unused producers.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10m")

  private[kafka010] val CONSUMER_CACHE_CAPACITY =
    ConfigBuilder("spark.kafka.consumer.cache.capacity")
      .doc("The maximum number of consumers cached. Please note it's a soft limit" +
        " (check Structured Streaming Kafka integration guide for further details).")
      .intConf
      .createWithDefault(64)
}
