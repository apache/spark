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

package org.apache.spark.sql.execution.streaming

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.util.Utils

class SequentialUnionExecutionSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Enable source naming and evolution features required for sequential union
    spark.conf.set("spark.sql.streaming.queryEvolution.enableSourceEvolution", "true")
  }

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  /** Helper to create multiple temp directories */
  protected def withTempDirs(f: java.io.File => Unit): Unit = {
    val dir = Utils.createTempDir()
    try f(dir) finally {
      Utils.deleteRecursively(dir)
    }
  }

  protected def withTempDirs(f: (java.io.File, java.io.File) => Unit): Unit = {
    val dir1 = Utils.createTempDir()
    val dir2 = Utils.createTempDir()
    try f(dir1, dir2) finally {
      Utils.deleteRecursively(dir1)
      Utils.deleteRecursively(dir2)
    }
  }

  protected def withTempDirs(f: (java.io.File, java.io.File, java.io.File) => Unit): Unit = {
    val dir1 = Utils.createTempDir()
    val dir2 = Utils.createTempDir()
    val dir3 = Utils.createTempDir()
    try f(dir1, dir2, dir3) finally {
      Utils.deleteRecursively(dir1)
      Utils.deleteRecursively(dir2)
      Utils.deleteRecursively(dir3)
    }
  }

  test("followedBy with file sources and rate source") {
    withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
      withTempDirs { (checkpointDir, dir1, dir2) =>
      // Write some data to file sources (backfill data)
      Seq("file1-a", "file1-b", "file1-c").toDF("value")
        .write.mode("overwrite").json(dir1.getCanonicalPath)
      Seq("file2-d", "file2-e", "file2-f").toDF("value")
        .write.mode("overwrite").json(dir2.getCanonicalPath)

      // Create file sources for backfill
      val backfill1 = spark.readStream
        .format("json")
        .schema("value STRING")
        .name("backfill1")
        .load(dir1.getCanonicalPath)

      val backfill2 = spark.readStream
        .format("json")
        .schema("value STRING")
        .name("backfill2")
        .load(dir2.getCanonicalPath)

      // Create rate source for live streaming
      val live = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "1")
        .option("numPartitions", "1")
        .name("live")
        .load()
        .selectExpr("CONCAT('rate-', CAST(value AS STRING)) as value")

      // Sequential union: backfill1 -> backfill2 -> live
      val sequential = backfill1.followedBy(backfill2, live)

      val query = sequential
        .writeStream
        .format("memory")
        .queryName("sequential_test")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .trigger(Trigger.ProcessingTime("1 second"))
        .start()

      try {
        // Verify it's using SequentialUnionExecution
        assert(query.isInstanceOf[StreamingQueryWrapper])
        val execution = query.asInstanceOf[StreamingQueryWrapper].streamingQuery
        assert(execution.isInstanceOf[SequentialUnionExecution],
          s"Expected SequentialUnionExecution but got ${execution.getClass.getName}")

        // Wait for some batches to process
        query.processAllAvailable()

        // Check we got data from backfill sources
        val results = spark.sql("SELECT * FROM sequential_test ORDER BY value").collect()
        assert(results.length >= 6,
          s"Expected at least 6 rows from backfill, got ${results.length}")

        // Verify backfill data
        val backfillValues = results.take(6).map(_.getString(0)).toSet
        val expectedValues = Set("file1-a", "file1-b", "file1-c", "file2-d", "file2-e", "file2-f")
        assert(backfillValues == expectedValues,
          s"Expected backfill values $expectedValues, got $backfillValues")

      } finally {
        query.stop()
      }
    }
    }
  }


  test("followedBy preserves state across source transitions") {
    withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
      withTempDirs { (checkpointDir, dir1, dir2) =>
      // Write data with duplicate keys across sources
      Seq((1, "a"), (2, "b"), (1, "c")).toDF("key", "value")
        .write.mode("overwrite").json(dir1.getCanonicalPath)
      Seq((2, "d"), (3, "e"), (1, "f")).toDF("key", "value")
        .write.mode("overwrite").json(dir2.getCanonicalPath)

      val source1 = spark.readStream
        .format("json")
        .schema("key INT, value STRING")
        .name("source1")
        .load(dir1.getCanonicalPath)

      val source2 = spark.readStream
        .format("json")
        .schema("key INT, value STRING")
        .name("source2")
        .load(dir2.getCanonicalPath)

      // Sequential union with aggregation (state should be preserved)
      val sequential = source1
        .followedBy(source2)
        .groupBy("key")
        .count()

      val query = sequential
        .writeStream
        .format("memory")
        .queryName("state_test")
        .outputMode("complete")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .trigger(Trigger.AvailableNow)
        .start()

      try {
        query.processAllAvailable()

        // Check aggregation results
        val results = spark.sql("SELECT * FROM state_test ORDER BY key")
          .collect()
          .map(r => (r.getInt(0), r.getLong(1)))
          .toMap

        // Key 1 appears 3 times total (across both sources)
        // Key 2 appears 2 times total
        // Key 3 appears 1 time
        assert(results == Map(1 -> 3L, 2 -> 2L, 3 -> 1L),
          s"Expected aggregation across sources, got $results")

      } finally {
        query.stop()
      }
    }
    }
  }

  test("followedBy with three sequential file sources") {
    withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
      withTempDirs { (checkpointDir, dir1, dir2, dir3) =>
        // Write data to three file sources
        Seq("src1-a", "src1-b").toDF("value")
          .write.mode("overwrite").json(dir1.getCanonicalPath)
        Seq("src2-c", "src2-d").toDF("value")
          .write.mode("overwrite").json(dir2.getCanonicalPath)
        Seq("src3-e", "src3-f").toDF("value")
          .write.mode("overwrite").json(dir3.getCanonicalPath)

        val source1 = spark.readStream
          .format("json")
          .schema("value STRING")
          .name("source1")
          .load(dir1.getCanonicalPath)

        val source2 = spark.readStream
          .format("json")
          .schema("value STRING")
          .name("source2")
          .load(dir2.getCanonicalPath)

        val source3 = spark.readStream
          .format("json")
          .schema("value STRING")
          .name("source3")
          .load(dir3.getCanonicalPath)

        // Sequential union: source1 -> source2 -> source3
        val sequential = source1.followedBy(source2, source3)

        val query = sequential
          .writeStream
          .format("memory")
          .queryName("three_sources_test")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.AvailableNow)
          .start()

        try {
          query.processAllAvailable()

          val results = spark.sql("SELECT * FROM three_sources_test ORDER BY value")
            .collect()
            .map(_.getString(0))
            .toSet

          val expected = Set("src1-a", "src1-b", "src2-c", "src2-d", "src3-e", "src3-f")
          assert(results == expected,
            s"Expected all values from 3 sources: $expected, got $results")

        } finally {
          query.stop()
        }
      }
    }
  }

  test("followedBy handles empty intermediate source") {
    withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
      withTempDirs { (checkpointDir, dir1, dir2, dir3) =>
        // Write data to first and third sources, leave second empty
        Seq("first-a", "first-b").toDF("value")
          .write.mode("overwrite").json(dir1.getCanonicalPath)
        // dir2 is empty (no data written)
        Seq("third-c", "third-d").toDF("value")
          .write.mode("overwrite").json(dir3.getCanonicalPath)

        val source1 = spark.readStream
          .format("json")
          .schema("value STRING")
          .name("source1")
          .load(dir1.getCanonicalPath)

        val source2 = spark.readStream
          .format("json")
          .schema("value STRING")
          .name("source2_empty")
          .load(dir2.getCanonicalPath)

        val source3 = spark.readStream
          .format("json")
          .schema("value STRING")
          .name("source3")
          .load(dir3.getCanonicalPath)

        val sequential = source1.followedBy(source2, source3)

        val query = sequential
          .writeStream
          .format("memory")
          .queryName("empty_source_test")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.AvailableNow)
          .start()

        try {
          query.processAllAvailable()

          val results = spark.sql("SELECT * FROM empty_source_test ORDER BY value")
            .collect()
            .map(_.getString(0))
            .toSet

          // Should get data from source1 and source3, skipping empty source2
          val expected = Set("first-a", "first-b", "third-c", "third-d")
          assert(results == expected,
            s"Expected data from non-empty sources: $expected, got $results")

        } finally {
          query.stop()
        }
      }
    }
  }

  test("followedBy uses SequentialUnionExecution") {
    withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
      withTempDirs { (checkpointDir, dir1, dir2) =>
        Seq("a").toDF("value").write.mode("overwrite").json(dir1.getCanonicalPath)
        Seq("b").toDF("value").write.mode("overwrite").json(dir2.getCanonicalPath)

        val source1 = spark.readStream.format("json").schema("value STRING")
          .name("s1").load(dir1.getCanonicalPath)
        val source2 = spark.readStream.format("json").schema("value STRING")
          .name("s2").load(dir2.getCanonicalPath)

        val query = source1.followedBy(source2)
          .writeStream
          .format("memory")
          .queryName("execution_type_test")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.AvailableNow)
          .start()

        try {
          // Verify it's using SequentialUnionExecution
          assert(query.isInstanceOf[StreamingQueryWrapper])
          val execution = query.asInstanceOf[StreamingQueryWrapper].streamingQuery
          assert(execution.isInstanceOf[SequentialUnionExecution],
            s"Expected SequentialUnionExecution but got ${execution.getClass.getName}")

        } finally {
          query.stop()
        }
      }
    }
  }
}
