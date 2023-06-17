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

import java.io.{File, FileWriter}
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession, SQLHelper}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Append
import org.apache.spark.sql.connect.client.util.QueryTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.util.Utils

case class ClickEvent(id: String, timestamp: Timestamp)

case class ClickState(id: String, count: Int)

class StreamingQuerySuite extends QueryTest with SQLHelper {

  val flatMapGroupsWithStateSchema: StructType = StructType(
    Array(StructField("id", StringType), StructField("timestamp", TimestampType)))

  val flatMapGroupsWithStateData: Seq[ClickEvent] = Seq(
    ClickEvent("a", new Timestamp(12345)),
    ClickEvent("a", new Timestamp(12346)),
    ClickEvent("a", new Timestamp(12347)),
    ClickEvent("b", new Timestamp(12348)),
    ClickEvent("b", new Timestamp(12349)),
    ClickEvent("c", new Timestamp(12350)))

  val flatMapGroupsWithStateInitialStateData: Seq[ClickState] =
    Seq(ClickState("a", 2), ClickState("b", 1))

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

        val lastProgress = query.lastProgress
        assert(lastProgress != null)
        assert(lastProgress.name == queryName)
        assert(!lastProgress.durationMs.isEmpty)
        assert(!lastProgress.eventTime.isEmpty)
        assert(lastProgress.stateOperators.nonEmpty)
        assert(
          lastProgress.stateOperators.head.customMetrics.keySet().asScala == Set(
            "loadedMapCacheHitCount",
            "loadedMapCacheMissCount",
            "stateOnCurrentVersionSizeBytes"))
        assert(lastProgress.sources.nonEmpty)
        assert(lastProgress.sink.description == "MemorySink")
        assert(lastProgress.observedMetrics.isEmpty)

        query.recentProgress.foreach { p =>
          assert(p.id == lastProgress.id)
          assert(p.runId == lastProgress.runId)
          assert(p.name == lastProgress.name)
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
    val writer = new TestForeachWriter[Row]

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

  test("foreach Int") {
    val session: SparkSession = spark
    import session.implicits._

    val writer = new TestForeachWriter[Int]

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

  // TODO[SPARK-43796]: Enable this test once ammonite client fixes the issue.
  //  test("foreach Custom class") {
  //    val session: SparkSession = spark
  //    import session.implicits._
  //
  //    case class TestClass(value: Int) {
  //      override def toString: String = value.toString
  //    }
  //
  //    val writer = new TestForeachWriter[TestClass]
  //    val df = spark.readStream
  //      .format("rate")
  //      .option("rowsPerSecond", "10")
  //      .load()
  //
  //    val query = df
  //      .selectExpr("CAST(value AS INT)")
  //      .as[TestClass]
  //      .writeStream
  //      .foreach(writer)
  //      .outputMode("update")
  //      .start()
  //
  //    assert(query.isActive)
  //    assert(query.exception.isEmpty)
  //
  //    query.stop()
  //  }

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

  test("flatMapGroupsWithState - streaming") {
    val session: SparkSession = spark
    import session.implicits._

    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        if (state.exists) throw new IllegalArgumentException("state.exists should be false")
        Iterator(ClickState(key, values.size))
      }
    spark.sql("DROP TABLE IF EXISTS my_sink")

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      flatMapGroupsWithStateData.toDS().write.parquet(path)
      val q = spark.readStream
        .schema(flatMapGroupsWithStateSchema)
        .parquet(path)
        .as[ClickEvent]
        .groupByKey(_.id)
        .flatMapGroupsWithState(Append, GroupStateTimeout.NoTimeout)(stateFunc)
        .writeStream
        .format("memory")
        .queryName("my_sink")
        .start()

      try {
        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          checkDataset(
            spark.table("my_sink").toDF().as[ClickState],
            ClickState("c", 1),
            ClickState("b", 2),
            ClickState("a", 3))
        }
      } finally {
        q.stop()
        spark.sql("DROP TABLE IF EXISTS my_sink")
      }
    }
  }

  test("flatMapGroupsWithState - streaming - with initial state") {
    val session: SparkSession = spark
    import session.implicits._

    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        val currState = state.getOption.getOrElse(ClickState(key, 0))
        Iterator(ClickState(key, currState.count + values.size))
      }
    val initialState = flatMapGroupsWithStateInitialStateData
      .toDS()
      .groupByKey(_.id)
      .mapValues(x => x)
    spark.sql("DROP TABLE IF EXISTS my_sink")

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      flatMapGroupsWithStateData.toDS().write.parquet(path)
      val q = spark.readStream
        .schema(flatMapGroupsWithStateSchema)
        .parquet(path)
        .as[ClickEvent]
        .groupByKey(_.id)
        .flatMapGroupsWithState(Append, GroupStateTimeout.NoTimeout, initialState)(stateFunc)
        .writeStream
        .format("memory")
        .queryName("my_sink")
        .start()

      try {
        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          checkDataset(
            spark.table("my_sink").toDF().as[ClickState],
            ClickState("c", 1),
            ClickState("b", 3),
            ClickState("a", 5))
        }
      } finally {
        q.stop()
        spark.sql("DROP TABLE IF EXISTS my_sink")
      }
    }
  }

  test("mapGroupsWithState - streaming") {
    val session: SparkSession = spark
    import session.implicits._

    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        if (state.exists) throw new IllegalArgumentException("state.exists should be false")
        ClickState(key, values.size)
      }
    spark.sql("DROP TABLE IF EXISTS my_sink")

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      flatMapGroupsWithStateData.toDS().write.parquet(path)
      val q = spark.readStream
        .schema(flatMapGroupsWithStateSchema)
        .parquet(path)
        .as[ClickEvent]
        .groupByKey(_.id)
        .mapGroupsWithState(GroupStateTimeout.NoTimeout)(stateFunc)
        .writeStream
        .format("memory")
        .queryName("my_sink")
        .outputMode("update")
        .start()

      try {
        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          checkDataset(
            spark.table("my_sink").toDF().as[ClickState],
            ClickState("c", 1),
            ClickState("b", 2),
            ClickState("a", 3))
        }
      } finally {
        q.stop()
        spark.sql("DROP TABLE IF EXISTS my_sink")
      }
    }
  }

  test("mapGroupsWithState - streaming - with initial state") {
    val session: SparkSession = spark
    import session.implicits._

    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        val currState = state.getOption.getOrElse(ClickState(key, 0))
        ClickState(key, currState.count + values.size)
      }
    val initialState = flatMapGroupsWithStateInitialStateData
      .toDS()
      .groupByKey(_.id)
      .mapValues(x => x)
    spark.sql("DROP TABLE IF EXISTS my_sink")

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      flatMapGroupsWithStateData.toDS().write.parquet(path)
      val q = spark.readStream
        .schema(flatMapGroupsWithStateSchema)
        .parquet(path)
        .as[ClickEvent]
        .groupByKey(_.id)
        .mapGroupsWithState(GroupStateTimeout.NoTimeout, initialState)(stateFunc)
        .writeStream
        .format("memory")
        .queryName("my_sink")
        .outputMode("update")
        .start()

      try {
        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          checkDataset(
            spark.table("my_sink").toDF().as[ClickState],
            ClickState("c", 1),
            ClickState("b", 3),
            ClickState("a", 5))
        }
      } finally {
        q.stop()
        spark.sql("DROP TABLE IF EXISTS my_sink")
      }
    }
  }
}

class TestForeachWriter[T] extends ForeachWriter[T] {
  var fileWriter: FileWriter = _
  var path: File = _

  def open(partitionId: Long, version: Long): Boolean = {
    path = Utils.createTempDir()
    fileWriter = new FileWriter(path, true)
    true
  }

  def process(row: T): Unit = {
    fileWriter.write(row.toString)
    fileWriter.write("\n")
  }

  def close(errorOrNull: Throwable): Unit = {
    fileWriter.close()
    Utils.deleteRecursively(path)
  }
}
