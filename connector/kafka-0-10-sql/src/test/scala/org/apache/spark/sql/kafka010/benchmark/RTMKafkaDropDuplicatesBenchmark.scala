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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * End-to-end latency benchmark for Kafka -> `dropDuplicates` -> Kafka in RTM and MBM.
 *
 * Four out of every five records have a fresh key. Every fifth record repeats the immediately
 * preceding key, so the operator exercises both state insertion and duplicate suppression without
 * exhausting a finite key space. Latency samples contain the emitted unique records.
 *
 * To run this benchmark:
 * {{{
 *   build/sbt "sql-kafka-0-10/Test/runMain \
 *     org.apache.spark.sql.kafka010.benchmark.RTMKafkaDropDuplicatesBenchmark"
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql-kafka-0-10/Test/runMain \
 *     org.apache.spark.sql.kafka010.benchmark.RTMKafkaDropDuplicatesBenchmark"
 * }}}
 *
 * Results are written to
 * `connector/kafka-0-10-sql/benchmarks/RTMKafkaDropDuplicatesBenchmark-results.txt`.
 */
object RTMKafkaDropDuplicatesBenchmark extends RTMKafkaStatefulBenchmarkBase {

  override protected def benchmarkName: String = "dropDuplicates"

  override protected def benchmarkDetails: String =
    "dropDuplicates(key), 80% fresh keys, 20% guaranteed duplicates; samples are unique rows"

  override protected def expectedSinkRecordCount(numSourceRecords: Long): Option[Long] = {
    Some(numSourceRecords - numSourceRecords / 5L)
  }

  override protected def inputKey(recordNumber: Long): String = {
    val key = if (recordNumber % 5 == 0) recordNumber - 1 else recordNumber
    key.toString
  }

  override protected def buildStatefulQuery(kafkaStream: DataFrame): DataFrame = {
    kafkaStream
      .select(
        col("key").cast("STRING").as("dedupKey"),
        col("key"),
        col("value"),
        toUnixMillis(col("timestamp")).as("sourceTimestampMs"))
      .dropDuplicates("dedupKey")
      .select(col("key"), col("value"), col("sourceTimestampMs"))
  }
}
