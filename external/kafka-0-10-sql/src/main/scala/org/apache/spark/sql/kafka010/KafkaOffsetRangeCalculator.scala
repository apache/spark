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

import org.apache.spark.sql.sources.v2.DataSourceOptions


private[kafka010] class KafkaOffsetRangeCalculator(val minPartitions: Int) {
  require(minPartitions >= 0)

  import KafkaOffsetRangeCalculator._
  /**
   * Calculate the offset ranges that we are going to process this batch. If `numPartitions`
   * is not set or is set less than or equal the number of `topicPartitions` that we're going to
   * consume, then we fall back to a 1-1 mapping of Spark tasks to Kafka partitions. If
   * `numPartitions` is set higher than the number of our `topicPartitions`, then we will split up
   * the read tasks of the skewed partitions to multiple Spark tasks.
   * The number of Spark tasks will be *approximately* `numPartitions`. It can be less or more
   * depending on rounding errors or Kafka partitions that didn't receive any new data.
   */
  def getRanges(
      fromOffsets: PartitionOffsetMap,
      untilOffsets: PartitionOffsetMap,
      executorLocations: Seq[String] = Seq.empty): Seq[KafkaOffsetRange] = {
    val partitionsToRead = untilOffsets.keySet.intersect(fromOffsets.keySet)

    val offsetRanges = partitionsToRead.toSeq.map { tp =>
      KafkaOffsetRange(tp, fromOffsets(tp), untilOffsets(tp))
    }

    // If minPartitions not set or there are enough partitions to satisfy minPartitions
    if (minPartitions == DEFAULT_MIN_PARTITIONS || offsetRanges.size > minPartitions) {
      // Assign preferred executor locations to each range such that the same topic-partition is
      // always read from the same executor and the KafkaConsumer can be reused
      offsetRanges.map { range =>
        range.copy(preferredLoc = getLocation(range.topicPartition, executorLocations))
      }
    } else {

      // Splits offset ranges with relatively large amount of data to smaller ones.
      val totalSize = offsetRanges.map(o => o.untilOffset - o.fromOffset).sum
      offsetRanges.flatMap { offsetRange =>
        val tp = offsetRange.topicPartition
        val size = offsetRange.untilOffset - offsetRange.fromOffset
        // number of partitions to divvy up this topic partition to
        val parts = math.max(math.round(size * 1.0 / totalSize * minPartitions), 1).toInt
        var remaining = size
        var startOffset = offsetRange.fromOffset
        (0 until parts).map { part =>
          // Fine to do integer division. Last partition will consume all the round off errors
          val thisPartition = remaining / (parts - part)
          remaining -= thisPartition
          val endOffset = startOffset + thisPartition
          val offsetRange = KafkaOffsetRange(tp, startOffset, endOffset, preferredLoc = None)
          startOffset = endOffset
          offsetRange
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

  private val DEFAULT_MIN_PARTITIONS = 0

  def apply(options: DataSourceOptions): KafkaOffsetRangeCalculator = {
    new KafkaOffsetRangeCalculator(options.getInt("minPartitions", DEFAULT_MIN_PARTITIONS))
  }
}


private[kafka010] case class KafkaOffsetRange(
  topicPartition: TopicPartition, fromOffset: Long, untilOffset: Long,
  preferredLoc: Option[String] = None)


