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

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

private[benchmark] case class TransformWithStateBenchmarkRecord(
    key: String,
    value: Array[Byte],
    sourceTimestampMs: Long)

private[benchmark] class TransformWithStateBenchmarkProcessor
    extends StatefulProcessor[
      String,
      TransformWithStateBenchmarkRecord,
      TransformWithStateBenchmarkRecord] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState("count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      _key: String,
      inputRows: Iterator[TransformWithStateBenchmarkRecord],
      _timerValues: TimerValues): Iterator[TransformWithStateBenchmarkRecord] = {
    inputRows.map { row =>
      countState.update(countState.get() + 1L)
      row
    }
  }
}

/**
 * End-to-end latency benchmark for Kafka -> JVM `transformWithState` -> Kafka in RTM and MBM.
 *
 * The processor uses processing-time mode and performs one ValueState read and update per input
 * row, then emits that row unchanged. Keys repeat over a 50,000-key space, so the benchmark covers
 * both new and existing state while keeping state size bounded. It does not register timers or use
 * TTL, although processing-time mode still performs its empty expired-timer checks. The first pass
 * through the key space occurs during warm-up, so reported samples measure existing-state updates.
 * Timer registration and TTL should be measured as separate workloads.
 *
 * To run this benchmark:
 * {{{
 *   build/sbt "sql-kafka-0-10/Test/runMain \
 *     org.apache.spark.sql.kafka010.benchmark.RTMKafkaTransformWithStateBenchmark"
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql-kafka-0-10/Test/runMain \
 *     org.apache.spark.sql.kafka010.benchmark.RTMKafkaTransformWithStateBenchmark"
 * }}}
 *
 * Results are written to
 * `connector/kafka-0-10-sql/benchmarks/RTMKafkaTransformWithStateBenchmark-results.txt`.
 */
object RTMKafkaTransformWithStateBenchmark extends RTMKafkaStatefulBenchmarkBase {

  private val numKeys = 50000L

  override protected def benchmarkName: String = "transformWithState"

  override protected def benchmarkDetails: String =
    s"ProcessingTime, one ValueState get/update per row, keys=$numKeys, no registered timers or " +
      "TTL; includes empty timer checks; samples are steady-state updates"

  override protected def expectedSinkRecordCount(numSourceRecords: Long): Option[Long] = {
    Some(numSourceRecords)
  }

  override protected def inputKey(recordNumber: Long): String = {
    (recordNumber % numKeys).toString
  }

  override protected def buildStatefulQuery(kafkaStream: DataFrame): DataFrame = {
    val session = kafkaStream.sparkSession
    import session.implicits._

    kafkaStream
      .select(
        col("key").cast("STRING").as("key"),
        col("value"),
        toUnixMillis(col("timestamp")).as("sourceTimestampMs"))
      .as[TransformWithStateBenchmarkRecord]
      .groupByKey(_.key)
      .transformWithState(
        new TransformWithStateBenchmarkProcessor,
        TimeMode.ProcessingTime(),
        OutputMode.Update())
      .toDF()
  }
}
