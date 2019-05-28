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

import java.{util => ju}

import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.reader.{Batch, InputPartition, PartitionReader, PartitionReaderFactory}


private[kafka010] class KafkaBatch(
    strategy: ConsumerStrategy,
    sourceOptions: Map[String, String],
    specifiedKafkaParams: Map[String, String],
    failOnDataLoss: Boolean,
    startingOffsets: KafkaOffsetRangeLimit,
    endingOffsets: KafkaOffsetRangeLimit)
    extends Batch with Logging {

  private val pollTimeoutMs =
    sourceOptions.getOrElse(KafkaSourceProvider.CONSUMER_POLL_TIMEOUT, "512").toLong

  override def planInputPartitions(): Array[InputPartition] = {
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = s"spark-kafka-relation-${ju.UUID.randomUUID}"

    val kafkaOffsetReader = new KafkaOffsetReader(
      strategy,
      KafkaSourceProvider.kafkaParamsForDriver(specifiedKafkaParams),
      sourceOptions,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    // Leverage the KafkaReader to obtain the relevant partition offsets
    val (fromPartitionOffsets, untilPartitionOffsets) = {
      try {
        (getPartitionOffsets(kafkaOffsetReader, startingOffsets),
          getPartitionOffsets(kafkaOffsetReader, endingOffsets))
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

  private def getPartitionOffsets(
      kafkaReader: KafkaOffsetReader,
      kafkaOffsets: KafkaOffsetRangeLimit): Map[TopicPartition, Long] = {
    def validateTopicPartitions(partitions: Set[TopicPartition],
      partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
      assert(partitions == partitionOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
          "Use -1 for latest, -2 for earliest, if you don't care.\n" +
          s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions}")
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $partitionOffsets")
      partitionOffsets
    }
    val partitions = kafkaReader.fetchTopicPartitions()
    // Obtain TopicPartition offsets with late binding support
    kafkaOffsets match {
      case EarliestOffsetRangeLimit => partitions.map {
        case tp => tp -> KafkaOffsetRangeLimit.EARLIEST
      }.toMap
      case LatestOffsetRangeLimit => partitions.map {
        case tp => tp -> KafkaOffsetRangeLimit.LATEST
      }.toMap
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        validateTopicPartitions(partitions, partitionOffsets)
    }
  }

  override def toString: String =
    s"KafkaBatch(strategy=$strategy, start=$startingOffsets, end=$endingOffsets)"
}

/** Partition of the KafkaBatch */
private[kafka010] case class KafkaBatchInputPartition(
    offsetRange: KafkaOffsetRange,
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    reuseKafkaConsumer: Boolean) extends InputPartition {
  override def preferredLocations(): Array[String] =
    offsetRange.preferredLoc.map(Seq(_)).getOrElse(Seq.empty).toArray
}

private[kafka010] object KafkaBatchReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[KafkaBatchInputPartition]
    KafkaBatchPartitionReader(p.offsetRange, p.executorKafkaParams, p.pollTimeoutMs,
      p.failOnDataLoss, p.reuseKafkaConsumer)
  }
}

/** A [[PartitionReader]] for reading Kafka data in a batch query. */
private[kafka010] case class KafkaBatchPartitionReader(
    offsetRange: KafkaOffsetRange,
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    reuseKafkaConsumer: Boolean) extends PartitionReader[InternalRow] with Logging {

  private val consumer = KafkaDataConsumer.acquire(
    offsetRange.topicPartition, executorKafkaParams, reuseKafkaConsumer)

  private val rangeToRead = resolveRange(offsetRange)
  private val converter = new KafkaRecordToUnsafeRowConverter

  private var nextOffset = rangeToRead.fromOffset
  private var nextRow: UnsafeRow = _

  override def next(): Boolean = {
    if (nextOffset < rangeToRead.untilOffset) {
      val record = consumer.get(nextOffset, rangeToRead.untilOffset, pollTimeoutMs, failOnDataLoss)
      if (record != null) {
        nextRow = converter.toUnsafeRow(record)
        nextOffset = record.offset + 1
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): UnsafeRow = {
    assert(nextRow != null)
    nextRow
  }

  override def close(): Unit = {
    consumer.release()
  }

  private def resolveRange(range: KafkaOffsetRange): KafkaOffsetRange = {
    if (range.fromOffset < 0 || range.untilOffset < 0) {
      // Late bind the offset range
      val availableOffsetRange = consumer.getAvailableOffsetRange()
      val fromOffset = if (range.fromOffset < 0) {
        assert(range.fromOffset == KafkaOffsetRangeLimit.EARLIEST,
          s"earliest offset ${range.fromOffset} does not equal ${KafkaOffsetRangeLimit.EARLIEST}")
        availableOffsetRange.earliest
      } else {
        range.fromOffset
      }
      val untilOffset = if (range.untilOffset < 0) {
        assert(range.untilOffset == KafkaOffsetRangeLimit.LATEST,
          s"latest offset ${range.untilOffset} does not equal ${KafkaOffsetRangeLimit.LATEST}")
        availableOffsetRange.latest
      } else {
        range.untilOffset
      }
      KafkaOffsetRange(range.topicPartition, fromOffset, untilOffset, range.preferredLoc)
    } else {
      range
    }
  }
}
