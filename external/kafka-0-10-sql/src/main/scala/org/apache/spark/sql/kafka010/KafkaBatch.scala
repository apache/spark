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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Network.NETWORK_TIMEOUT
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.v2.reader.{Batch, InputPartition, PartitionReaderFactory}


private[kafka010] class KafkaBatch(
    strategy: ConsumerStrategy,
    sourceOptions: CaseInsensitiveMap[String],
    specifiedKafkaParams: Map[String, String],
    failOnDataLoss: Boolean,
    startingOffsets: KafkaOffsetRangeLimit,
    endingOffsets: KafkaOffsetRangeLimit)
  extends Batch with Logging {
  assert(startingOffsets != LatestOffsetRangeLimit,
    "Starting offset not allowed to be set to latest offsets.")
  assert(endingOffsets != EarliestOffsetRangeLimit,
    "Ending offset not allowed to be set to earliest offsets.")

  private[kafka010] val pollTimeoutMs = sourceOptions.getOrElse(
    KafkaSourceProvider.CONSUMER_POLL_TIMEOUT,
    (SparkEnv.get.conf.get(NETWORK_TIMEOUT) * 1000L).toString
  ).toLong

  override def planInputPartitions(): Array[InputPartition] = {
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = KafkaSourceProvider.batchUniqueGroupId(sourceOptions)

    val kafkaOffsetReader = new KafkaOffsetReader(
      strategy,
      KafkaSourceProvider.kafkaParamsForDriver(specifiedKafkaParams),
      sourceOptions,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    // Leverage the KafkaReader to obtain the relevant partition offsets
    val (fromPartitionOffsets, untilPartitionOffsets) = {
      try {
        (kafkaOffsetReader.fetchPartitionOffsets(startingOffsets),
          kafkaOffsetReader.fetchPartitionOffsets(endingOffsets))
      } finally {
        kafkaOffsetReader.close()
      }
    }

    // Obtain topicPartitions in both from and until partition offset, ignoring
    // topic partitions that were added and/or deleted between the two above calls.
    if (fromPartitionOffsets.keySet != untilPartitionOffsets.keySet) {
      implicit val topicOrdering: Ordering[TopicPartition] = Ordering.by(t => t.topic())
      val fromTopics = fromPartitionOffsets.keySet.toList.sorted.mkString(",")
      val untilTopics = untilPartitionOffsets.keySet.toList.sorted.mkString(",")
      throw new IllegalStateException("different topic partitions " +
        s"for starting offsets topics[${fromTopics}] and " +
        s"ending offsets topics[${untilTopics}]")
    }

    // Calculate offset ranges
    val offsetRanges = untilPartitionOffsets.keySet.map { tp =>
      val fromOffset = fromPartitionOffsets.getOrElse(tp,
        // This should not happen since topicPartitions contains all partitions not in
        // fromPartitionOffsets
        throw new IllegalStateException(s"$tp doesn't have a from offset"))
      val untilOffset = untilPartitionOffsets(tp)
      KafkaOffsetRange(tp, fromOffset, untilOffset, None)
    }.toArray

    val executorKafkaParams =
      KafkaSourceProvider.kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId)
    offsetRanges.map { range =>
      new KafkaBatchInputPartition(
        range, executorKafkaParams, pollTimeoutMs, failOnDataLoss, false)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    KafkaBatchReaderFactory
  }

  override def toString: String =
    s"KafkaBatch(strategy=$strategy, start=$startingOffsets, end=$endingOffsets)"
}
