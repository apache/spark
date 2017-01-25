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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String


private[kafka010] class KafkaRelation(
    override val sqlContext: SQLContext,
    kafkaReader: KafkaOffsetReader,
    executorKafkaParams: ju.Map[String, Object],
    sourceOptions: Map[String, String],
    failOnDataLoss: Boolean,
    startingOffsets: KafkaOffsets,
    endingOffsets: KafkaOffsets)
  extends BaseRelation with TableScan with Logging {
  assert(startingOffsets != LatestOffsets,
    "Starting offset not allowed to be set to latest offsets.")
  assert(endingOffsets != EarliestOffsets,
    "Ending offset not allowed to be set to earliest offsets.")

  private val pollTimeoutMs = sourceOptions.getOrElse(
    "kafkaConsumer.pollTimeoutMs",
    sqlContext.sparkContext.conf.getTimeAsMs("spark.network.timeout", "120s").toString
  ).toLong

  override def schema: StructType = KafkaOffsetReader.kafkaSchema

  override def buildScan(): RDD[Row] = {
    // Leverage the KafkaReader to obtain the relevant partition offsets
    val fromPartitionOffsets = getPartitionOffsets(startingOffsets)
    val untilPartitionOffsets = getPartitionOffsets(endingOffsets)
    // Obtain topicPartitions in both from and until partition offset, ignoring
    // topic partitions that were added and/or deleted between the two above calls.
    if (fromPartitionOffsets.keySet.size != untilPartitionOffsets.keySet.size) {
      throw new IllegalStateException("Kafka return different topic partitions " +
        "for starting and ending offsets")
    }

    val sortedExecutors = KafkaUtils.getSortedExecutorList(sqlContext.sparkContext)
    val numExecutors = sortedExecutors.length
    logDebug("Sorted executors: " + sortedExecutors.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = untilPartitionOffsets.keySet.map { tp =>
      val fromOffset = fromPartitionOffsets.get(tp).getOrElse {
          // This should not happen since topicPartitions contains all partitions not in
          // fromPartitionOffsets
          throw new IllegalStateException(s"$tp doesn't have a from offset")
      }
      val untilOffset = untilPartitionOffsets(tp)
      KafkaSourceRDDOffsetRange(tp, fromOffset, untilOffset, None)
    }.toArray

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = new KafkaSourceRDD(
      sqlContext.sparkContext, executorKafkaParams, offsetRanges,
      pollTimeoutMs, failOnDataLoss, false).map { cr =>
      InternalRow(
        cr.key,
        cr.value,
        UTF8String.fromString(cr.topic),
        cr.partition,
        cr.offset,
        DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(cr.timestamp)),
        cr.timestampType.id)
    }
    sqlContext.internalCreateDataFrame(rdd, schema).rdd
  }

  private def getPartitionOffsets(kafkaOffsets: KafkaOffsets) = kafkaOffsets match {
    case EarliestOffsets => kafkaReader.fetchEarliestOffsets()
    case LatestOffsets => kafkaReader.fetchLatestOffsets()
    case SpecificOffsets(p) => kafkaReader.fetchSpecificStartingOffsets(p)
  }
}
