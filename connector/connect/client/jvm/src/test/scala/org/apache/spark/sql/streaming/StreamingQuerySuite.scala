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

import java.util.concurrent.TimeUnit

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.SQLHelper
import org.apache.spark.sql.connect.client.util.RemoteSparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.window

class StreamingQuerySuite extends RemoteSparkSession with SQLHelper {

  test("Streaming API with windowed aggregate query") {
    // This verifies standard streaming API by starting a streaming query with windowed count.
    withSQLConf(
      "spark.sql.shuffle.partitions" -> "1" // Avoid too many reducers.
    ) {
      val readDF = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .option("numPartitions", "1")
        .load()

      // Verify schema (results in sending an RPC)
      assert(readDF.schema.toDDL == "timestamp TIMESTAMP,value BIGINT")

      val countsDF = readDF
        .withWatermark("timestamp", "10 seconds")
        .groupBy(window(col("timestamp"), "5 seconds"))
        .count()
        .selectExpr("window.start as timestamp", "count as num_events")

      assert(countsDF.schema.toDDL == "timestamp TIMESTAMP,num_events BIGINT NOT NULL")

      // Start the query
      val queryName = "sparkConnectStreamingQuery"

      val query = countsDF.writeStream
        .format("memory")
        .queryName(queryName)
        .trigger(Trigger.ProcessingTime("1 second"))
        .start()

      try {
        // Verify some of the API.
        assert(query.isActive)

        eventually(timeout(30.seconds)) {
          assert(query.status.isDataAvailable)
          assert(query.recentProgress.nonEmpty) // Query made progress.
        }

        query.explain() // Prints the plan to console.
        // Consider verifying explain output by capturing stdout similar to
        // test("Dataset explain") in ClientE2ETestSuite.

      } finally {
        // Don't wait for any processed data. Otherwise the test could take multiple seconds.
        query.stop()

        // The query should still be accessible after stopped.
        assert(!query.isActive)
        assert(query.recentProgress.nonEmpty)
      }
    }
  }

  test("Streaming table API") {
    withSQLConf(
      "spark.sql.shuffle.partitions" -> "1" // Avoid too many reducers.
    ) {
      spark.sql("DROP TABLE IF EXISTS my_table")

      withTempPath { ckpt =>
        val q1 = spark.readStream
          .format("rate")
          .load()
          .writeStream
          .option("checkpointLocation", ckpt.getCanonicalPath)
          .toTable("my_table")

        val q2 = spark.readStream
          .table("my_table")
          .writeStream
          .format("memory")
          .queryName("my_sink")
          .start()

        try {
          q1.processAllAvailable()
          q2.processAllAvailable()
          eventually(timeout(30.seconds)) {
            assert(spark.table("my_sink").count() > 0)
          }
        } finally {
          q1.stop()
          q2.stop()
          spark.sql("DROP TABLE my_table")
        }
      }
    }
  }

  test("awaitTermination") {
    withSQLConf(
      "spark.sql.shuffle.partitions" -> "1" // Avoid too many reducers.
    ) {
      val q = spark.readStream
        .format("rate")
        .load()
        .writeStream
        .format("memory")
        .queryName("test")
        .start()

      val start = System.nanoTime
      val terminated = q.awaitTermination(500)
      val end = System.nanoTime
      assert((end - start) / 1e6 >= 500)
      assert(!terminated)

      q.stop()
      eventually(timeout(1.minute)) {
        q.awaitTermination()
      }
    }
  }

  test("foreach Row") {
    withTempPath { f =>
      val path = f.getCanonicalPath + "/output"
      val writer = new ForeachWriter[Row] {
        var fileWriter: FileWriter = _

        def open(partitionId: Long, version: Long): Boolean = {
          fileWriter = new FileWriter(path, true)
          true
        }

        def process(row: Row): Unit = {
          fileWriter.write(row.mkString(", "))
          fileWriter.write("\n")
        }

        def close(errorOrNull: Throwable): Unit = {
          fileWriter.close()
        }
      }

      val df = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .load()

      val query = df.writeStream
        .foreach(writer)
        .outputMode("update")
        .start()

      assert(query.isActive)
      assert(query.exception.isEmpty)

      query.stop()
    }
  }

  test("foreach Int") {
    withTempPath { f =>
      val path = f.getCanonicalPath + "/output"
      val writer = new ForeachWriter[Int] {
        var fileWriter: FileWriter = _

        def open(partitionId: Long, version: Long): Boolean = {
          fileWriter = new FileWriter(filePath, true) // true to append
          true
        }

        def process(value: Int): Unit = {
          fileWriter.write(value.toString)
          fileWriter.write("\n") // newline for each value
        }

        def close(errorOrNull: Throwable): Unit = {
          fileWriter.close()
        }
      }

      val df = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .load()

      val query = df
        .selectExpr("CAST(value AS INT)")
        .as[Int]
        .writeStream
        .foreach(writer)
        .outputMode("update")
        .start()

      assert(query.isActive)
      assert(query.exception.isEmpty)

      query.stop()
    }
  }

  test("streaming query manager") {
    assert(spark.streams.active.isEmpty)
    val q = spark.readStream
      .format("rate")
      .load()
      .writeStream
      .format("console")
      .start()

    assert(q.name == null)
    val q1 = spark.streams.get(q.id)
    val q2 = spark.streams.active(0)
    assert(q.id == q1.id && q.id == q2.id)
    assert(q.runId == q1.runId && q.runId == q2.runId)
    assert(q1.name == null && q2.name == null)

    spark.streams.resetTerminated()
    val start = System.nanoTime
    // Same setting as in test_query_manager_await_termination in test_streaming.py
    val terminated = spark.streams.awaitAnyTermination(2600)
    val end = System.nanoTime
    assert((end - start) >= TimeUnit.MILLISECONDS.toNanos(2000))
    assert(!terminated)

    q.stop()
    assert(!q1.isActive)
  }
}
