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

import org.apache.spark.sql.util.CaseInsensitiveStringMap


/**
 * Class to calculate offset ranges to process based on the from and until offsets, and
 * the configured `minPartitions`.
 */
private[kafka010] class KafkaOffsetRangeCalculator(val minPartitions: Option[Int],
                                                   val maxBytesPerPartition: Option[Int]) {
  require(minPartitions.isEmpty || minPartitions.get > 0)

  /**
   * Calculate the offset ranges that we are going to process this batch. If `minPartitions`
   * is not set or is set less than or equal the number of `topicPartitions` that we're going to
   * consume, then we fall back to a 1-1 mapping of Spark tasks to Kafka partitions. If
   * `minPartitions` is set higher than the number of our `topicPartitions`, then we will split up
   * the read tasks of the skewed partitions to multiple Spark tasks.
   * The number of Spark tasks will be *approximately* `minPartitions`. It can be less or more
   * depending on rounding errors or Kafka partitions that didn't receive any new data.
   *
   * Empty (`KafkaOffsetRange.size == 0`) or invalid (`KafkaOffsetRange.size < 0`) ranges  will be
   * dropped.
   */
  def getRanges(
      ranges: Seq[KafkaOffsetRange],
      executorLocations: Seq[String] = Seq.empty): Seq[KafkaOffsetRange] = {
    val offsetRanges = ranges.filter(_.size > 0)

    // If minPartitions not set or there are enough partitions to satisfy minPartitions
    // and maxBytesPerPartition is empty
    if ((minPartitions.isEmpty || offsetRanges.size >= minPartitions.get)
        && maxBytesPerPartition.isEmpty) {
      // Assign preferred executor locations to each range such that the same topic-partition is
      // preferentially read from the same executor and the KafkaConsumer can be reused.
      offsetRanges.map { range =>
        range.copy(preferredLoc = getLocation(range.topicPartition, executorLocations))
      }
    } else if (minPartitions.isDefined && minPartitions.get > offsetRanges.size) {

      // Splits offset ranges with relatively large amount of data to smaller ones.
      val totalSize = offsetRanges.map(_.size).sum

      // First distinguish between any small (i.e. unsplit) ranges and large (i.e. split) ranges,
      // in order to exclude the contents of unsplit ranges from the proportional math applied to
      // split ranges
      val unsplitRanges = offsetRanges.filter { range =>
        getPartCount(range.size, totalSize, minPartitions.get) == 1
      }

      val unsplitRangeTotalSize = unsplitRanges.map(_.size).sum
      val splitRangeTotalSize = totalSize - unsplitRangeTotalSize
      val unsplitRangeTopicPartitions = unsplitRanges.map(_.topicPartition).toSet
      val splitRangeMinPartitions = math.max(minPartitions.get - unsplitRanges.size, 1)

      // Now we can apply the main calculation logic
      offsetRanges.flatMap { range =>
        val tp = range.topicPartition
        val size = range.size
        // number of partitions to divvy up this topic partition to
        val parts = if (unsplitRangeTopicPartitions.contains(tp)) {
          1
        } else {
          getPartCount(size, splitRangeTotalSize, splitRangeMinPartitions)
        }
        getDividedPartition(parts, range)
      }.filter(_.size > 0)
    } else {
      val avgMessageSize = 1 // Todo estimate row size
      val maxPartSize = maxBytesPerPartition.get

      offsetRanges.flatMap { range =>
        val size = range.size
        // number of partitions to divvy up this topic partition to
        val parts = (size * avgMessageSize) / maxPartSize + 1
        getDividedPartition(parts, range)
      }.filter(_.size > 0)
    }
  }

  private def getDividedPartition(parts: Long, offsetRange: KafkaOffsetRange)
  : IndexedSeq[KafkaOffsetRange] = {
    var remaining = offsetRange.size
    var startOffset = offsetRange.fromOffset
    val tp = offsetRange.topicPartition
    val untilOffset = offsetRange.untilOffset

    (0 until parts).map { part =>
      // Fine to do integer division. Last partition will consume all the round off errors
      val thisPartition = remaining / (parts - part)
      remaining -= thisPartition
      val endOffset = math.min(startOffset + thisPartition, untilOffset)
      val offsetRange = KafkaOffsetRange(tp, startOffset, endOffset, None)
      startOffset = endOffset
      offsetRange
    }
  }

  private def getPartCount(size: Long, totalSize: Long, minParts: Int): Int = {
    math.max(math.round(size.toDouble / totalSize * minParts), 1).toInt
  }

  private def getLocation(tp: TopicPartition, executorLocations: Seq[String]): Option[String] = {
    def floorMod(a: Long, b: Int): Int = ((a % b).toInt + b) % b

    val numExecutors = executorLocations.length
    if (numExecutors > 0) {
      // This allows cached KafkaConsumers in the executors to be re-used to read the same
      // partition in every batch.
      Some(executorLocations(floorMod(tp.hashCode, numExecutors)))
    } else None
  }
}

private[kafka010] object KafkaOffsetRangeCalculator {

  def apply(options: CaseInsensitiveStringMap): KafkaOffsetRangeCalculator = {
    val minPartition = Option(options.get(KafkaSourceProvider.MIN_PARTITIONS_OPTION_KEY))
      .map(_.toInt)
    val maxBytesPerPartition = Option(options.get(
      KafkaSourceProvider.MAX_BYTES_PER_PARTITIONS_OPTION_KEY))
      .map(_.toInt)
    new KafkaOffsetRangeCalculator(minPartition, maxBytesPerPartition)
  }
}

private[kafka010] case class KafkaOffsetRange(
    topicPartition: TopicPartition,
    fromOffset: Long,
    untilOffset: Long,
    preferredLoc: Option[String] = None) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition
  /**
   * The estimated size of messages in the range. It may be different than the real number of
   * messages due to log compaction or transaction metadata. It should not be used to provide
   * answers directly.
   */
  def size: Long = untilOffset - fromOffset
}
