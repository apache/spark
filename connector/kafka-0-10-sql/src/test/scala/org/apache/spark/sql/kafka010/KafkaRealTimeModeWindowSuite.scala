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

class KafkaRealTimeModeWindowSuite extends KafkaRealTimeModeBaseSuite {

  test("tumbling window count") {
    runTest {
      case params @ TestParams(query, clock, read, outputTopic, checkpointDir) =>
        val tumblingWindowDuration = 10
        val numRows = 10

        val readPart = read
          .toDF()
          .select(col("_1").as("timestamp").cast("TIMESTAMP"), col("_2").as("value"))
          .groupBy(
            window(column("timestamp"), s"${tumblingWindowDuration} seconds"),
            column("value")
          )
          .count()
          .select(
            concat(
              col("window").cast("STRING"),
              lit("-"),
              col("value").cast("STRING"),
              lit("-"),
              col("count").cast("STRING")
            ).as("value")
          )

        params.query = writeToKafka("tumbling_window_count_low_latency",
          outputTopic, checkpointDir, readPart)

        val expectedResults = mutable.ListBuffer[GenericRowWithSchema]()

        for (i <- 0 until 3) {
          for (k <- 0 until numRows) {
            val value = k % 2
            val data = ((i * 10).toLong, value)
            read.addData({
              data
            })

            /**
             * results should be something like this
             * {1969-12-31 16:00:00, 1969-12-31 16:00:10}-0-1
             * {1969-12-31 16:00:00, 1969-12-31 16:00:10}-1-1
             * {1969-12-31 16:00:00, 1969-12-31 16:00:10}-0-2
             * {1969-12-31 16:00:00, 1969-12-31 16:00:10}-1-2
             */
            val windowDurationMs = tumblingWindowDuration * 1000

            val startTime = getDateTimeString(((i + 1) * windowDurationMs) - windowDurationMs)
            val endTime = getDateTimeString((i + 1) * windowDurationMs)

            expectedResults += new GenericRowWithSchema(
              Array(s"{$startTime, $endTime}-$value-${Math.ceil((k + 1) / 2.0).toInt}"),
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

  test("sliding window count") {
    runTest {
      case params @ TestParams(query, clock, read, outputTopic, checkpointDir) =>
        val numRows = 10
        val slideWindowDuration = 5
        val windowDuration = 10

        val readPart = read
          .toDF()
          .select(col("_1").as("timestamp").cast("TIMESTAMP"), col("_2").as("value"))
          .groupBy(
            window(
              column("timestamp"),
              s"$windowDuration seconds",
              s"$slideWindowDuration seconds"
            ),
            column("value")
          )
          .count()
          .select(
            concat(
              col("window").cast("STRING"),
              lit("-"),
              col("value").cast("STRING"),
              lit("-"),
              col("count").cast("STRING")
            ).as("value")
          )

        params.query =
          writeToKafka("sliding_window_count_low_latency", outputTopic, checkpointDir, readPart)

        // -5 -> 5
        val bucket0 = mutable.HashMap[Int, Int]()
        // 0 -> 10
        val bucket1 = mutable.HashMap[Int, Int]()
        // 5 -> 15
        val bucket2 = mutable.HashMap[Int, Int]()
        // 10 -> 20
        val bucket3 = mutable.HashMap[Int, Int]()

        val expectedResults = mutable.ListBuffer[GenericRowWithSchema]()
        for (i <- 0 until 3) {
          for (k <- 0 until numRows) {
            val value = k % 2
            val data = ((i * 5).toLong, value)
            read.addData({
              data
            })

            /**
             * Results should be something like
             *
             * {1969-12-31 16:00:00, 1969-12-31 16:00:10}-0-1
             * {1969-12-31 15:59:55, 1969-12-31 16:00:05}-0-1
             * {1969-12-31 16:00:00, 1969-12-31 16:00:10}-1-1
             * {1969-12-31 15:59:55, 1969-12-31 16:00:05}-1-1
             * {1969-12-31 16:00:00, 1969-12-31 16:00:10}-0-2
             * {1969-12-31 15:59:55, 1969-12-31 16:00:05}-0-2
             * {1969-12-31 16:00:00, 1969-12-31 16:00:10}-1-2
             * {1969-12-31 15:59:55, 1969-12-31 16:00:05}-1-2
             * ...
             */
            val ts = data._1
            if (ts >= -5 && ts < 5) {
              bucket0(value) = bucket0.getOrElse(value, 0) + 1
            }

            if (ts >= 0 && ts < 10) {
              bucket1(value) = bucket1.getOrElse(value, 0) + 1
            }

            if (ts >= 5 && ts < 15) {
              bucket2(value) = bucket2.getOrElse(value, 0) + 1
            }

            if (ts >= 10 && ts < 20) {
              bucket3(value) = bucket3.getOrElse(value, 0) + 1
            }
          }

          bucket0.foreach(pair => {
            val k = pair._1
            val count = pair._2
            for (i <- 1 to count) {
              expectedResults += new GenericRowWithSchema(
                Array(s"{${getDateTimeString(-5000)}, ${getDateTimeString(5000)}}-$k-${i}"),
                schema = new StructType().add(StructField("value", StringType))
              )
            }
          })

          bucket1.foreach(pair => {
            val k = pair._1
            val count = pair._2
            for (i <- 1 to count) {
              expectedResults += new GenericRowWithSchema(
                Array(s"{${getDateTimeString(0)}, ${getDateTimeString(10000)}}-$k-${i}"),
                schema = new StructType().add(StructField("value", StringType))
              )
            }
          })

          bucket2.foreach(pair => {
            val k = pair._1
            val count = pair._2
            for (i <- 1 to count) {
              expectedResults += new GenericRowWithSchema(
                Array(s"{${getDateTimeString(5000)}, ${getDateTimeString(15000)}}-$k-${i}"),
                schema = new StructType().add(StructField("value", StringType))
              )
            }
          })

          bucket3.foreach(pair => {
            val k = pair._1
            val count = pair._2
            for (i <- 1 to count) {
              expectedResults += new GenericRowWithSchema(
                Array(s"{${getDateTimeString(10000)}, ${getDateTimeString(20000)}}-$k-${i}"),
                schema = new StructType().add(StructField("value", StringType))
              )
            }
          })

          eventually(timeout(60.seconds)) {
            checkAnswer(readKafkaTopic(outputTopic), expectedResults.toSeq)
          }

          expectedResults.clear()
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
