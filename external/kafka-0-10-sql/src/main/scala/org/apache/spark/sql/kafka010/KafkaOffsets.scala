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

package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition

/**
 * Values that can be specified to configure starting,
 * ending, and specific offsets.
 */
private[kafka010] sealed trait KafkaOffsets

/**
 * Bind to the earliest offsets in Kafka
 */
private[kafka010] case object EarliestOffsets extends KafkaOffsets

/**
 * Bind to the latest offsets in Kafka
 */
private[kafka010] case object LatestOffsets extends KafkaOffsets

/**
 * Bind to the specific offsets. A offset == -1 binds to the latest
 * offset, and offset == -2 binds to the earliest offset.
 */
private[kafka010] case class SpecificOffsets(
    partitionOffsets: Map[TopicPartition, Long]) extends KafkaOffsets

private[kafka010] object KafkaOffsets {
  // Used to denote unbounded offset positions
  val LATEST = -1L
  val EARLIEST = -2L
}
