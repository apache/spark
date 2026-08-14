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

package org.apache.spark.sql.kafka010.benchmark

import scala.concurrent.duration._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 * End-to-end latency benchmark for Kafka -> windowed aggregation -> Kafka in RTM and MBM.
 *
 * The query groups 1000 repeated keys into one-minute tumbling windows and computes a running
 * count. It also carries the maximum input Kafka timestamp for each update, so the reported value
 * is aggregate-update latency for the newest reflected input. RTM emits an update per input row;
 * MBM can coalesce updates to the same group within a micro-batch, so sample counts are reported.
 * No watermark is used.
 *
 * To run this benchmark:
 * {{{
 *   build/sbt "sql-kafka-0-10/Test/runMain \
 *     org.apache.spark.sql.kafka010.benchmark.RTMKafkaWindowAggregationBenchmark"
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql-kafka-0-10/Test/runMain \
 *     org.apache.spark.sql.kafka010.benchmark.RTMKafkaWindowAggregationBenchmark"
 * }}}
 *
 * Results are written to
 * `connector/kafka-0-10-sql/benchmarks/RTMKafkaWindowAggregationBenchmark-results.txt`.
 */
object RTMKafkaWindowAggregationBenchmark extends RTMKafkaStatefulBenchmarkBase {

  private val numKeys = 1000L
  private val windowDuration = 1.minute
  private val windowDurationMs = windowDuration.toMillis

  override protected def benchmarkName: String = "window aggregation"

  override protected def benchmarkDetails: String =
    s"groupBy(window=$windowDuration,key).count, keys=$numKeys; native aggregate-update samples, " +
      "not a per-input distribution; MBM may coalesce updates within a micro-batch"

  override protected def percentileRatiosAreComparable: Boolean = false

  override protected def inputKey(recordNumber: Long): String = {
    (recordNumber % numKeys).toString
  }

  override protected def buildStatefulQuery(kafkaStream: DataFrame): DataFrame = {
    kafkaStream
      .select(
        col("key").cast("STRING").as("groupKey"),
        col("timestamp"),
        toUnixMillis(col("timestamp")).as("sourceTimestampMs"))
      .groupBy(window(col("timestamp"), windowDuration.toString), col("groupKey"))
      .agg(
        count(lit(1)).as("count"),
        max(col("sourceTimestampMs")).as("sourceTimestampMs"))
      .select(
        col("groupKey").as("key"),
        col("count").cast("STRING").as("value"),
        col("sourceTimestampMs"))
  }

  override protected def validateSinkOutput(
      isRtm: Boolean,
      numSourceRecords: Long,
      numSinkRecords: Long,
      kafkaSourceData: DataFrame,
      kafkaSinkData: DataFrame): Unit = {
    require(
      numSinkRecords <= numSourceRecords,
      s"Window aggregation emitted $numSinkRecords rows for $numSourceRecords inputs")
    // RTM plans StatefulStreamlineAggregateExec, whose Update mode emits once per input row.
    if (isRtm) {
      require(
        numSinkRecords == numSourceRecords,
        s"RTM emitted $numSinkRecords window updates for $numSourceRecords inputs")
    }

    val sourceCounts = kafkaSourceData
      .select(
        col("key").cast("STRING").as("groupKey"),
        windowStart(col("source-timestamp")).as("windowStart"))
      .groupBy("groupKey", "windowStart")
      .agg(count(lit(1)).as("sourceCount"))
    val sinkUpdates = kafkaSinkData
      .select(
        col("key").cast("STRING").as("groupKey"),
        windowStart(col("source-timestamp")).as("windowStart"),
        col("value").cast("STRING").cast("LONG").as("aggregateCount"))
    val sinkFinalCounts = sinkUpdates
      .groupBy("groupKey", "windowStart")
      .agg(max("aggregateCount").as("sinkCount"))
    val hasCountMismatch = sourceCounts
      .join(sinkFinalCounts, Seq("groupKey", "windowStart"), "full_outer")
      .filter(!col("sourceCount").eqNullSafe(col("sinkCount")))
      .limit(1)
      .count() > 0
    require(!hasCountMismatch, "Window aggregation sink does not cover every input group")

    val hasDuplicateUpdate = sinkUpdates
      .groupBy("groupKey", "windowStart", "aggregateCount")
      .count()
      .filter(col("count") > 1)
      .limit(1)
      .count() > 0
    require(!hasDuplicateUpdate, "Window aggregation sink contains duplicate updates")
  }

  private def windowStart(timestampMs: Column): Column = {
    timestampMs - pmod(timestampMs, lit(windowDurationMs))
  }
}
