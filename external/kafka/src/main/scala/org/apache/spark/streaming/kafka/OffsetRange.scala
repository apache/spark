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

package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition

/** Represents a range of offsets from a single Kafka TopicAndPartition */
trait OffsetRange {
  /** kafka topic name */
  def topic: String

  /** kafka partition id */
  def partition: Int

  /** inclusive starting offset */
  def fromOffset: Long

  /** exclusive ending offset */
  def untilOffset: Long
}

/** Something that has a collection of OffsetRanges */
trait HasOffsetRanges {
  def offsetRanges: Array[OffsetRange]
}

private class OffsetRangeImpl(
  override val topic: String,
  override val partition: Int,
  override val fromOffset: Long,
  override val untilOffset: Long
) extends OffsetRange

object OffsetRange {
  def create(
    topic: String,
    partition: Int,
    fromOffset: Long,
    untilOffset: Long): OffsetRange =
    new OffsetRangeImpl(
      topic,
      partition,
      fromOffset,
      untilOffset)

  def create(
    topicAndPartition: TopicAndPartition,
    fromOffset: Long,
    untilOffset: Long): OffsetRange =
    new OffsetRangeImpl(
      topicAndPartition.topic,
      topicAndPartition.partition,
      fromOffset,
      untilOffset)

}
