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

/** Something that has a collection of OffsetRanges */
trait HasOffsetRanges {
  def offsetRanges: Array[OffsetRange]
}

/** Represents a range of offsets from a single Kafka TopicAndPartition */
final class OffsetRange private(
    /** kafka topic name */
    val topic: String,
    /** kafka partition id */
    val partition: Int,
    /** inclusive starting offset */
    val fromOffset: Long,
    /** exclusive ending offset */
    val untilOffset: Long) extends Serializable {
  import OffsetRange.OffsetRangeTuple

  /** this is to avoid ClassNotFoundException during checkpoint restore */
  private[streaming]
  def toTuple: OffsetRangeTuple = (topic, partition, fromOffset, untilOffset)
}

object OffsetRange {
  def create(topic: String, partition: Int, fromOffset: Long, untilOffset: Long): OffsetRange =
    new OffsetRange(topic, partition, fromOffset, untilOffset)

  def create(
      topicAndPartition: TopicAndPartition,
      fromOffset: Long,
      untilOffset: Long): OffsetRange =
    new OffsetRange(topicAndPartition.topic, topicAndPartition.partition, fromOffset, untilOffset)

  def apply(topic: String, partition: Int, fromOffset: Long, untilOffset: Long): OffsetRange =
    new OffsetRange(topic, partition, fromOffset, untilOffset)

  def apply(
      topicAndPartition: TopicAndPartition,
      fromOffset: Long,
      untilOffset: Long): OffsetRange =
    new OffsetRange(topicAndPartition.topic, topicAndPartition.partition, fromOffset, untilOffset)

  /** this is to avoid ClassNotFoundException during checkpoint restore */
  private[spark]
  type OffsetRangeTuple = (String, Int, Long, Long)

  private[streaming]
  def apply(t: OffsetRangeTuple) =
    new OffsetRange(t._1, t._2, t._3, t._4)
}
