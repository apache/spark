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
 * Class to calculate offset ranges to process based on the the from and until offsets, and
 * the configured `minPartitions`.
 */
private[kafka010] class KafkaOffsetRangeCalculator(val minPartitions: Option[Int]) {
  require(minPartitions.isEmpty || minPartitions.get > 0)

  /**
   * Calculate the offset ranges that we are going to process this batch. If `minPartitions`
   * is not set or is set less than or equal the number of `topicPartitions` that we're going to
   * consume, then we fall back to a 1-1 mapping of Spark tasks to Kafka partitions. If
   * `numPartitions` is set higher than the number of our `topicPartitions`, then we will split up
   * the read tasks of the skewed partitions to multiple Spark tasks.
   * The number of Spark tasks will be *approximately* `numPartitions`. It can be less or more
   * depending on rounding errors or Kafka partitions that didn't receive any new data.
   *
   * Empty ranges (`KafkaOffsetRange.size <= 0`) will be dropped.
   */
  def getRanges(
      fromOffsets: PartitionOffsetMap,
      untilOffsets: PartitionOffsetMap,
      executorLocations: Seq[String] = Seq.empty): Seq[KafkaOffsetRange] = {
    val partitionsToRead = untilOffsets.keySet.intersect(fromOffsets.keySet)

    val offsetRanges = partitionsToRead.toSeq.map { tp =>
      KafkaOffsetRange(tp, fromOffsets(tp), untilOffsets(tp), preferredLoc = None)
    }.filter(_.size > 0)

    // If minPartitions not set or there are enough partitions to satisfy minPartitions
    if (minPartitions.isEmpty || offsetRanges.size > minPartitions.get) {
      // Assign preferred executor locations to each range such that the same topic-partition is
      // preferentially read from the same executor and the KafkaConsumer can be reused.
      offsetRanges.map { range =>
        range.copy(preferredLoc = getLocation(range.topicPartition, executorLocations))
      }
    } else {

      // Splits offset ranges with relatively large amount of data to smaller ones.
      val totalSize = offsetRanges.map(_.size).sum
      val idealRangeSize = totalSize.toDouble / minPartitions.get

      offsetRanges.flatMap { range =>
        // Split the current range into subranges as close to the ideal range size
        val numSplitsInRange = math.round(range.size.toDouble / idealRangeSize).toInt

        (0 until numSplitsInRange).map { i =>
          val splitStart = range.fromOffset + range.size * (i.toDouble / numSplitsInRange)
          val splitEnd = range.fromOffset + range.size * ((i.toDouble + 1) / numSplitsInRange)
          KafkaOffsetRange(
            range.topicPartition, splitStart.toLong, splitEnd.toLong, preferredLoc = None)
        }
      }
    }
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
    val optionalValue = Option(options.get(KafkaSourceProvider.MIN_PARTITIONS_OPTION_KEY))
      .map(_.toInt)
    new KafkaOffsetRangeCalculator(optionalValue)
  }
}

private[kafka010] case class KafkaOffsetRange(
    topicPartition: TopicPartition,
    fromOffset: Long,
    untilOffset: Long,
    preferredLoc: Option[String]) {
  lazy val size: Long = untilOffset - fromOffset
}
