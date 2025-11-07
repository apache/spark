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

package org.apache.spark.sql.streaming

import scala.concurrent.duration._

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.execution.streaming.LowLatencyMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamRealTimeModeAllowlistSuite extends StreamRealTimeModeE2ESuiteBase {
  import testImplicits._

  test("rtm source allowlist") {
    val query = spark.readStream
      .format("rate")
      .option("numPartitions", 1)
      .load()
      .writeStream
      .format("console")
      .outputMode("update")
      .trigger(defaultTrigger)
      .start()

    eventually(timeout(60.seconds)) {
      checkError(
        exception = query.exception.get.getCause.asInstanceOf[SparkIllegalArgumentException],
        condition = "STREAMING_REAL_TIME_MODE.INPUT_STREAM_NOT_SUPPORTED",
        parameters = Map(
          "className" ->
            "org.apache.spark.sql.execution.streaming.sources.RateStreamMicroBatchStream")
      )
    }
  }

  test("rtm operator allowlist") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
      val inputData = LowLatencyMemoryStream[Int](2)
      val staticDf = spark.range(1, 31, 1, 10).selectExpr("id AS join_key", "id AS join_value")

      val df = inputData.toDF()
        .select(col("value").as("key"), col("value").as("value"))
        .join(staticDf, col("value") === col("join_key"))
        .select(
          concat(col("key"), lit("-"), col("value"), lit("-"), col("join_value")).as("output"))

      val query = runStreamingQuery("operation_allowlist", df)

      eventually(timeout(60.seconds)) {
        checkError(
          exception = query.exception.get.getCause.asInstanceOf[SparkIllegalArgumentException],
          condition = "STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST",
          parameters = Map(
            "errorType" -> "operator",
            "message" -> (
              "org.apache.spark.sql.execution.SortExec, " +
                "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec, " +
                "org.apache.spark.sql.execution.joins.SortMergeJoinExec are"
              )
          )
        )
      }
    }
  }

  test("rtm sink allowlist") {
    val read = LowLatencyMemoryStream[Int](2)

    val query = read
      .toDF()
      .writeStream
      .format("noop")
      .outputMode(OutputMode.Update())
      .trigger(defaultTrigger)
      .queryName("rtm_sink_allowlist")

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        query.start()
      },
      condition = "STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST",
      parameters = Map(
        "errorType" -> "sink",
        "message" -> "org.apache.spark.sql.execution.datasources.noop.NoopTable$ is"
      ))

    withSQLConf(SQLConf.STREAMING_REAL_TIME_MODE_ALLOWLIST_CHECK.key -> "false") {
      val tmp = query.start()
      Thread.sleep(5000)
      tmp.stop()
    }
  }

  // TODO(SPARK-54237) : Remove this test after RTM can shuffle to multiple stages
  test("repartition not allowed") {
      val inputData = LowLatencyMemoryStream[Int](2)

      val df = inputData.toDF()
        .select(col("value").as("key"))
        .repartition(4, col("key"))

      val query = runStreamingQuery("repartition_allowlist", df)

      eventually(timeout(60.seconds)) {
        checkError(
          exception = query.exception.get.getCause.asInstanceOf[SparkIllegalArgumentException],
          condition = "STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST",
          parameters = Map(
            "errorType" -> "operator",
            "message" -> (
                "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec is"
              )
          )
        )
      }
  }

  // TODO(SPARK-54236) : Remove this test after RTM supports stateful queries
  test("stateful queries not allowed") {
    val inputData = LowLatencyMemoryStream[Int](2)

    val df = inputData.toDF()
      .select(col("value").as("key"))
      .groupBy(col("key"))
      .count()
      .select(concat(col("key"), lit("-"), col("count")))

    val query = runStreamingQuery("repartition_allowlist", df)

    eventually(timeout(60.seconds)) {
      checkError(
        exception = query.exception.get.getCause.asInstanceOf[SparkIllegalArgumentException],
        condition = "STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST",
        parameters = Map(
          "errorType" -> "operator",
          "message" -> (
            "org.apache.spark.sql.execution.aggregate.HashAggregateExec, " +
              "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec, " +
              "org.apache.spark.sql.execution.streaming" +
              ".operators.stateful.StateStoreRestoreExec, " +
              "org.apache.spark.sql.execution.streaming.operators.stateful.StateStoreSaveExec are"
            )
        )
      )
    }
  }
}
