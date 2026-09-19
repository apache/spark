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

import scala.collection.mutable

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class KafkaRealTimeModeAggregationSuite extends KafkaRealTimeModeBaseSuite {

  test("tumbling window max") {
    runTest {
      case params @ TestParams(query, clock, read, outputTopic, checkpointDir) =>
        val tumblingWindowDuration = 10
        val numRows = 10
        val readPart = read
          .toDF()
          .select(col("_1").as("timestamp").cast("TIMESTAMP"), col("_2").as("value"))
          .groupBy(window(column("timestamp"), s"${tumblingWindowDuration} seconds"))
          .max()
          .select(
            concat(
              col("window").cast("STRING"),
              lit("-"),
              col("max(value)").cast("STRING")
            ).as("value")
          )

        params.query =
          writeToKafka("tumbling_window_max_low_latency", outputTopic, checkpointDir, readPart)

        val expectedResults = mutable.ListBuffer[GenericRowWithSchema]()

        for (i <- 0 until 3) {
          for (k <- (1 to numRows).reverse) {
            val data = ((i * 10).toLong, k)
            read.addData(0, Seq(data))

            val windowDurationMs = tumblingWindowDuration * 1000

            val startTime = getDateTimeString(((i + 1) * windowDurationMs) - windowDurationMs)
            val endTime = getDateTimeString((i + 1) * windowDurationMs)

            expectedResults += new GenericRowWithSchema(
              Array(s"{$startTime, $endTime}-${numRows}"),
              schema = new StructType().add(StructField("value", StringType))
            )
          }

          eventually(timeout(60.seconds)) {
            checkAnswer(readKafkaTopic(outputTopic), expectedResults.toSeq)
          }
          // advance to next batch
          clock.advance(1000)

          eventually(timeout(60.seconds)) {
            params.query
              .asInstanceOf[StreamingQueryWrapper]
              .streamingQuery
              .getLatestExecutionContext()
              .batchId should be(i + 1)
            params.query.lastProgress.sources(0).numInputRows should be(numRows)
          }
        }
    }
  }

  test("tumbling window min") {
    runTest {
      case params @ TestParams(query, clock, read, outputTopic, checkpointDir) =>
        val tumblingWindowDuration = 10
        val numRows = 10

        val readPart = read
          .toDF()
          .select(col("_1").as("timestamp").cast("TIMESTAMP"), col("_2").as("value"))
          .groupBy(window(column("timestamp"), s"${tumblingWindowDuration} seconds"))
          .min()
          .select(
            concat(
              col("window").cast("STRING"),
              lit("-"),
              col("min(value)").cast("STRING")
            ).as("value")
          )

        params.query =
          writeToKafka("tumbling_window_min_low_latency", outputTopic, checkpointDir, readPart)

        val expectedResults = mutable.ListBuffer[GenericRowWithSchema]()

        for (i <- 0 until 3) {
          for (k <- (1 to numRows)) {
            val data = ((i * 10).toLong, k)
            read.addData(0, Seq(data))

            val windowDurationMs = tumblingWindowDuration * 1000

            val startTime = getDateTimeString(((i + 1) * windowDurationMs) - windowDurationMs)
            val endTime = getDateTimeString((i + 1) * windowDurationMs)

            expectedResults += new GenericRowWithSchema(
              Array(s"{$startTime, $endTime}-${1}"),
              schema = new StructType().add(StructField("value", StringType))
            )
          }

          eventually(timeout(60.seconds)) {
            checkAnswer(readKafkaTopic(outputTopic), expectedResults.toSeq)
          }
          // advance to next batch
          clock.advance(1000)

          eventually(timeout(60.seconds)) {
            params.query
              .asInstanceOf[StreamingQueryWrapper]
              .streamingQuery
              .getLatestExecutionContext()
              .batchId should be(i + 1)
            params.query.lastProgress.sources(0).numInputRows should be(numRows)
          }
        }
    }
  }

  test("tumbling window sum") {
    runTest {
      case params @ TestParams(query, clock, read, outputTopic, checkpointDir) =>
        val tumblingWindowDuration = 10
        val numRows = 10

        val readPart = read
          .toDF()
          .select(col("_1").as("timestamp").cast("TIMESTAMP"), col("_2").as("value"))
          .groupBy(window(column("timestamp"), s"${tumblingWindowDuration} seconds"))
          .sum()
          .select(
            concat(
              col("window").cast("STRING"),
              lit("-"),
              col("sum(value)").cast("STRING")
            ).as("value")
          )

        params.query =
          writeToKafka("tumbling_window_sum_low_latency", outputTopic, checkpointDir, readPart)

        val expectedResults = mutable.ListBuffer[GenericRowWithSchema]()

        for (i <- 0 until 3) {
          var sum = 0
          for (k <- (1 to numRows)) {
            val data = ((i * 10).toLong, k)
            read.addData(0, Seq(data))

            val windowDurationMs = tumblingWindowDuration * 1000

            val startTime = getDateTimeString(((i + 1) * windowDurationMs) - windowDurationMs)
            val endTime = getDateTimeString((i + 1) * windowDurationMs)

            sum += k
            expectedResults += new GenericRowWithSchema(
              Array(s"{$startTime, $endTime}-${sum}"),
              schema = new StructType().add(StructField("value", StringType))
            )
          }

          eventually(timeout(60.seconds)) {
            checkAnswer(readKafkaTopic(outputTopic), expectedResults.toSeq)
          }
          // advance to next batch
          clock.advance(1000)

          eventually(timeout(60.seconds)) {
            params.query
              .asInstanceOf[StreamingQueryWrapper]
              .streamingQuery
              .getLatestExecutionContext()
              .batchId should be(i + 1)
            params.query.lastProgress.sources(0).numInputRows should be(numRows)
          }
        }
    }
  }

  test("tumbling window avg") {
    runTest {
      case params @ TestParams(query, clock, read, outputTopic, checkpointDir) =>
        val tumblingWindowDuration = 10
        val numRows = 10

        val readPart = read
          .toDF()
          .select(col("_1").as("timestamp").cast("TIMESTAMP"), col("_2").as("value"))
          .groupBy(window(column("timestamp"), s"${tumblingWindowDuration} seconds"))
          .avg()
          .select(
            concat(
              col("window").cast("STRING"),
              lit("-"),
              col("avg(value)").cast("INT").cast("STRING")
            ).as("value")
          )

        params.query =
          writeToKafka("tumbling_window_avg_low_latency", outputTopic, checkpointDir, readPart)

        val expectedResults = mutable.ListBuffer[GenericRowWithSchema]()

        for (i <- 0 until 3) {
          var sum = 0
          for (k <- (1 to numRows)) {
            // Feed varying values (the row index k) so the assertion below actually exercises the
            // average rather than passing for any single-value aggregate: a constant input would
            // make avg == first == last == that value.
            val data = ((i * 10).toLong, k)
            read.addData(0, Seq(data))

            val windowDurationMs = tumblingWindowDuration * 1000

            val startTime = getDateTimeString(((i + 1) * windowDurationMs) - windowDurationMs)
            val endTime = getDateTimeString((i + 1) * windowDurationMs)

            // Update mode emits the running average after each record: sum(1..k) / k. Cast to INT
            // to match the query's avg(value) cast.
            sum += k
            val runningAvg = sum / k
            expectedResults += new GenericRowWithSchema(
              Array(s"{$startTime, $endTime}-$runningAvg"),
              schema = new StructType().add(StructField("value", StringType))
            )
          }

          eventually(timeout(60.seconds)) {
            checkAnswer(readKafkaTopic(outputTopic), expectedResults.toSeq)
          }
          // advance to next batch
          clock.advance(1000)

          eventually(timeout(60.seconds)) {
            params.query
              .asInstanceOf[StreamingQueryWrapper]
              .streamingQuery
              .getLatestExecutionContext()
              .batchId should be(i + 1)
            params.query.lastProgress.sources(0).numInputRows should be(numRows)
          }
        }
    }
  }
}
