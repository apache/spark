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

package org.apache.spark.sql.execution.streaming.sources

import java.io.File

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.spark.ExecutorDeadException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.SerializeFromObjectExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStoreCommitValidationFailed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.util.ArrayImplicits._

case class KV(key: Int, value: Long)

class ForeachBatchSinkSuite extends StreamTest {
  import testImplicits._

  test("foreachBatch with non-stateful query") {
    val mem = MemoryStream[Int]
    val ds = mem.toDS().map(_ + 1)

    val tester = new ForeachBatchTester[Int](mem)
    val writer = (ds: Dataset[Int], batchId: Long) => tester.record(batchId, ds.map(_ + 1))

    import tester._
    testWriter(ds, writer)(
      check(in = 1, 2, 3)(out = 3, 4, 5), // out = in + 2 (i.e. 1 in query, 1 in writer)
      check(in = 5, 6, 7)(out = 7, 8, 9))
  }

  test("foreachBatch with non-stateful query - untyped Dataset") {
    val mem = MemoryStream[Int]
    val ds = mem.toDF().selectExpr("value + 1 as value")

    val tester = new ForeachBatchTester[Row](mem)(ExpressionEncoder(ds.schema))
    val writer = (df: DataFrame, batchId: Long) =>
      tester.record(batchId, df.selectExpr("value + 1"))

    import tester._
    testWriter(ds, writer)(
      // out = in + 2 (i.e. 1 in query, 1 in writer)
      check(in = 1, 2, 3)(out = Row(3), Row(4), Row(5)),
      check(in = 5, 6, 7)(out = Row(7), Row(8), Row(9)))
  }

  test("foreachBatch with stateful query in update mode") {
    val mem = MemoryStream[Int]
    val ds = mem.toDF()
      .select($"value" % 2 as "key")
      .groupBy("key")
      .agg(count("*") as "value")
      .toDF().as[KV]

    val tester = new ForeachBatchTester[KV](mem)
    val writer = (batchDS: Dataset[KV], batchId: Long) => tester.record(batchId, batchDS)

    import tester._
    testWriter(ds, writer, outputMode = OutputMode.Update)(
      check(in = 0)(out = (0, 1L)),
      check(in = 1)(out = (1, 1L)),
      check(in = 2, 3)(out = (0, 2L), (1, 2L)))
  }

  test("foreachBatch with stateful query in complete mode") {
    val mem = MemoryStream[Int]
    val ds = mem.toDF()
      .select($"value" % 2 as "key")
      .groupBy("key")
      .agg(count("*") as "value")
      .toDF().as[KV]

    val tester = new ForeachBatchTester[KV](mem)
    val writer = (batchDS: Dataset[KV], batchId: Long) => tester.record(batchId, batchDS)

    import tester._
    testWriter(ds, writer, outputMode = OutputMode.Complete)(
      check(in = 0)(out = (0, 1L)),
      check(in = 1)(out = (0, 1L), (1, 1L)),
      check(in = 2)(out = (0, 2L), (1, 1L)))
  }

  test("foreachBatch with batch specific operations") {
    val mem = MemoryStream[Int]
    val ds = mem.toDS().map(_ + 1)

    val tester = new ForeachBatchTester[Int](mem)
    val writer: (Dataset[Int], Long) => Unit = { case (df, batchId) =>
      df.persist()

      val newDF = df
        .map(_ + 1)
        .repartition(1)
        .sort(Column("value").desc)
      tester.record(batchId, newDF)

      // just run another simple query against cached DF to confirm they don't conflict each other
      val curValues = df.collect()
      val newValues = df.map(_ + 2).collect()
      assert(curValues.map(_ + 2) === newValues)

      df.unpersist()
    }

    import tester._
    testWriter(ds, writer)(
      // out = in + 2 (i.e. 1 in query, 1 in writer), with sorted
      check(in = 1, 2, 3)(out = 5, 4, 3),
      check(in = 5, 6, 7)(out = 9, 8, 7))
  }

  test("foreachBatchSink does not affect metric generation") {
    val mem = MemoryStream[Int]
    val ds = mem.toDS().map(_ + 1)

    val tester = new ForeachBatchTester[Int](mem)
    val writer = (ds: Dataset[Int], batchId: Long) => tester.record(batchId, ds.map(_ + 1))

    import tester._
    testWriter(ds, writer)(
      check(in = 1, 2, 3)(out = 3, 4, 5),
      checkMetrics)
  }

  test("throws errors in invalid situations") {
    val ds = MemoryStream[Int].toDS()
    val ex1 = intercept[IllegalArgumentException] {
      ds.writeStream.foreachBatch(null.asInstanceOf[(Dataset[Int], Long) => Unit]).start()
    }
    assert(ex1.getMessage.contains("foreachBatch function cannot be null"))
    val ex2 = intercept[AnalysisException] {
      ds.writeStream.foreachBatch((_: Dataset[Int], _: Long) => {})
        .trigger(Trigger.Continuous("1 second")).start()
    }
    assert(ex2.getMessage.contains("'foreachBatch' is not supported with continuous trigger"))
    val ex3 = intercept[AnalysisException] {
      ds.writeStream.foreachBatch((_: Dataset[Int], _: Long) => {}).partitionBy("value").start()
    }
    assert(ex3.getMessage.contains("'foreachBatch' does not support partitioning"))
  }

  test("foreachBatch should not introduce object serialization") {
    def assertPlan[T](stream: MemoryStream[Int], ds: Dataset[T]): Unit = {
      var planAsserted = false

      val writer: (Dataset[T], Long) => Unit = { case (df, _) =>
        assert(!df.queryExecution.executedPlan.exists { p =>
          p.isInstanceOf[SerializeFromObjectExec]
        }, "Untyped Dataset should not introduce serialization on object!")
        planAsserted = true
      }

      stream.addData(1, 2, 3, 4, 5)

      val query = ds.writeStream.trigger(Trigger.AvailableNow()).foreachBatch(writer).start()
      query.awaitTermination()

      assert(planAsserted, "ForeachBatch writer should be called!")
    }

    // typed
    val mem = MemoryStream[Int]
    val ds = mem.toDS().map(_ + 1)
    assertPlan(mem, ds)

    // untyped
    val mem2 = MemoryStream[Int]
    val dsUntyped = mem2.toDF().selectExpr("value + 1 as value")
    assertPlan(mem2, dsUntyped)
  }

  test("foreachBatch user function error is classified") {
    val mem = MemoryStream[Int]
    val ds = mem.toDS().map(_ + 1)
    mem.addData(1, 2, 3, 4, 5)

    val funcEx = new IllegalAccessException("access error")
    val queryEx = intercept[StreamingQueryException] {
      val query = ds.writeStream.foreachBatch((_: Dataset[Int], _: Long) => throw funcEx).start()
      query.awaitTermination()
    }

    val errClass = "FOREACH_BATCH_USER_FUNCTION_ERROR"

    // verify that we classified the exception
    assert(queryEx.getMessage.contains(errClass))
    assert(queryEx.getCause == funcEx)

    val sparkEx = ExecutorDeadException("network error")
    val ex = intercept[StreamingQueryException] {
      val query = ds.writeStream.foreachBatch((_: Dataset[Int], _: Long) => throw sparkEx).start()
      query.awaitTermination()
    }

    // we didn't wrap the spark exception
    assert(!ex.getMessage.contains(errClass))
    assert(ex.getCause == sparkEx)
  }

  test("SPARK-51265: IncrementalExecution should set the command execution code correctly") {
    val mem = MemoryStream[Int]
    val ds = mem.toDS().map(_ + 1)

    def foreachBatchFn(df: Dataset[Int], batchId: Long): Unit = {
      withTempView("param", "s") {
        df.createOrReplaceTempView("param")
        val streamDf = df.sparkSession.readStream.format("rate").load()
        streamDf.createOrReplaceTempView("s")
        withTable("output") {
          val ex = intercept[AnalysisException] {
            // Creates a table from streaming source with batch query. This should fail.
            df.sparkSession.sql("CREATE TABLE output USING csv AS SELECT * FROM s")
          }
          assert(
            ex.getMessage.contains("Queries with streaming sources must be executed with " +
              "writeStream.start(), or from a streaming table or flow definition within a Spark " +
              "Declarative Pipeline.")
          )

          // Creates a table from batch source (materialized RDD plan of streaming query).
          // This should be work properly.
          df.sparkSession.sql("CREATE TABLE output USING csv AS SELECT * from param")

          checkAnswer(
            df.sparkSession.sql("SELECT value FROM output"),
            Seq(Row(2), Row(3), Row(4), Row(5), Row(6)))
        }
      }
    }

    mem.addData(1, 2, 3, 4, 5)

    val query = ds.writeStream
      .trigger(Trigger.AvailableNow())
      .foreachBatch(foreachBatchFn _)
      .start()
    query.awaitTermination()
  }

  test("SPARK-52008: foreachBatch with show() should fail with appropriate error") {
    // This test verifies that commit validation is enabled by default
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempDir { tempDir =>
        val checkpointPath = tempDir.getCanonicalPath
        // Create a simple streaming DataFrame
        val streamingDF = spark.readStream
          .format("rate")
          .option("rowsPerSecond", 3)
          .load()
          .withColumn("pt_id", (rand() * 99 + 1).cast("int"))
          .withColumn("event_time",
            expr("timestampadd(SECOND, cast(rand() * 2 * 86400 - 86400 as int), timestamp)"))
          .withColumn("in_map", (rand() * 2).cast("int") === 1)
          .drop("value")

        // Create a stateful streaming query
        val windowedDF = streamingDF
          .withWatermark("event_time", "1 day")
          .groupBy("pt_id")
          .agg(
            max("event_time").as("latest_event_time"),
            last("in_map").as("in_map")
          )
          .withColumn("output_time", current_timestamp())

        // Define a foreachBatch function that uses show(), which only consumes some partitions
        def problematicBatchProcessor(batchDF: DataFrame, batchId: Long): Unit = {
          // show() only processes enough partitions to display the specified number of rows
          // This doesn't consume all partitions, causing state files to be missing
          batchDF.show(2) // Only shows 2 rows, not processing all partitions
        }

        // Start the streaming query
        val queryEx = intercept[StreamingQueryException] {
          val query = windowedDF.writeStream
            .queryName("reproducer_test")
            .option("checkpointLocation", checkpointPath)
            .foreachBatch(problematicBatchProcessor _)
            .outputMode("update")
            .start()

          // Wait for the exception to be thrown
          query.awaitTermination()
        }

        // Verify we get the StateStore commit validation error
        // The error is wrapped by RPC framework, so we need to check the cause chain
        val rootCause = queryEx.getCause
        assert(rootCause != null, "Expected a root cause for the StreamingQueryException")

        // The RPC framework wraps our exception, so check the cause of the cause
        val actualException = rootCause.getCause
        assert(actualException != null, "Expected a cause for the RPC exception")
        assert(actualException.isInstanceOf[StateStoreCommitValidationFailed],
          s"Expected StateStoreCommitValidationFailed but got ${actualException.getClass.getName}")

        val errorMessage = actualException.getMessage
        assert(errorMessage.contains("[STATE_STORE_COMMIT_VALIDATION_FAILED]"),
          s"Expected STATE_STORE_COMMIT_VALIDATION_FAILED error, but got: $errorMessage")
        assert(errorMessage.contains("State store commit validation failed"),
          s"Expected state store commit validation message, but got: $errorMessage")
        assert(errorMessage.contains("Missing commits"),
          s"Expected missing commits message, but got: $errorMessage")

        // Extract and validate the expected vs actual commit counts
        val expectedPattern = "Expected (\\d+) commits but got (\\d+)".r
        val missingPattern = "Missing commits: (.+)".r

        expectedPattern.findFirstMatchIn(errorMessage) match {
          case Some(m) =>
            val expectedCommits = m.group(1).toInt
            val actualCommits = m.group(2).toInt

            // We should have fewer actual commits than expected due to show(2)
            // not processing all partitions
            assert(actualCommits < expectedCommits,
              s"Expected fewer actual commits ($actualCommits)" +
                s" than expected commits ($expectedCommits)")
            assert(actualCommits >= 1,
              s"Expected at least 1 actual commit from show(2), but got $actualCommits")
            assert(expectedCommits == 5,
              s"Expected more than 5 commits but got $expectedCommits")

          case None =>
            fail(s"Could not find expected/actual commit counts in error message: $errorMessage")
        }

        // Validate that missing commits are reported with proper structure
        missingPattern.findFirstMatchIn(errorMessage) match {
          case Some(m) =>
            val missingCommits = m.group(1)
            assert(missingCommits.nonEmpty,
              s"Expected non-empty missing commits list, but got: '$missingCommits'")
            // Should contain operator and partition information
            assert(missingCommits.contains("operator=") && missingCommits.contains("partition="),
              s"Expected missing commits to contain operator and" +
                s" partition info, but got: '$missingCommits'")
          case None =>
            fail(s"Could not find missing commits in error message: $errorMessage")
        }
      }
    }
  }

  test("StateStore commit validation should detect missing commits") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempDir { tempDir =>
        val checkpointPath = tempDir.getCanonicalPath

        // Create a streaming DataFrame with controlled partitioning
        val streamingDF = spark.readStream
          .format("rate")
          .option("rowsPerSecond", 10)
          .option("numPartitions", 4) // Ensure we have multiple partitions
          .load()
          .withColumn("key", col("value") % 100)

        // Create a stateful operation that requires all partitions to process
        val aggregatedDF = streamingDF
          .groupBy("key")
          .agg(count("*").as("count"))

        // ForeachBatch function that only processes some data and then throws exception
        // This should cause some StateStore commits to be missing
        def problematicBatchProcessor(batchDF: DataFrame, batchId: Long): Unit = {
          // Force evaluation of only a subset of partitions by using limit
          // This should cause some partitions to not process their StateStores
          batchDF.limit(5).collect() // Only process first 5 rows

          // Simulate a failure that prevents remaining partitions from committing
          if (batchId > 0) {
            throw new RuntimeException("Simulated batch processing failure")
          }
        }

        // This should fail with StateStore commit validation error
        val queryEx = intercept[StreamingQueryException] {
          val query = aggregatedDF.writeStream
            .queryName("commit_validation_test")
            .option("checkpointLocation", checkpointPath)
            .foreachBatch(problematicBatchProcessor _)
            .outputMode("complete")
            .start()

          query.awaitTermination()
        }

        // Should fail with either our new validation error or the simulated RuntimeException
        // Check the cause chain since RPC wraps exceptions
        val rootCause = queryEx.getCause
        val actualException = if (rootCause != null) rootCause.getCause else null

        val hasCommitValidationError = actualException != null && (
          actualException.isInstanceOf[StateStoreCommitValidationFailed] ||
          actualException.getMessage.contains("[STATE_STORE_COMMIT_VALIDATION_FAILED]"))
        val hasSimulatedError = queryEx.getMessage.contains("Simulated batch processing failure")

        assert(hasCommitValidationError || hasSimulatedError,
          s"Expected StateStore commit validation error or simulated error," +
            s" but got: ${queryEx.getMessage}")
      }
    }
  }

  test("StateStore commit validation with AvailableNow trigger") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempDir { tempDir =>
        val checkpointPath = tempDir.getCanonicalPath

        // Create a temporary file with data
        val inputPath = new File(tempDir, "input").getCanonicalPath
        val inputData = spark.range(0, 100)
          .selectExpr("id", "id % 10 as key")
        inputData.write
          .mode("overwrite")
          .parquet(inputPath)

        // Get the schema from the written data
        val schema = inputData.schema

        // Create a streaming DataFrame with AvailableNow trigger
        val streamingDF = spark.readStream
          .format("parquet")
          .schema(schema) // Provide the schema explicitly
          .load(inputPath)

        // Stateful aggregation
        val aggregatedDF = streamingDF
          .groupBy("key")
          .agg(count("*").as("count"))

        // ForeachBatch that only processes partial data
        def problematicBatchProcessor(batchDF: DataFrame, batchId: Long): Unit = {
          // Only show first 2 rows, won't process all partitions
          batchDF.show(2)
        }

        val queryEx = intercept[StreamingQueryException] {
          val query = aggregatedDF.writeStream
            .queryName("availablenow_commit_test")
            .option("checkpointLocation", checkpointPath)
            .foreachBatch(problematicBatchProcessor _)
            .outputMode("complete")
            .trigger(Trigger.AvailableNow())
            .start()

          query.awaitTermination()
        }

        // Check the cause chain since RPC wraps exceptions
        val rootCause = queryEx.getCause
        assert(rootCause != null, "Expected a root cause for the StreamingQueryException")
        val actualException = rootCause.getCause
        assert(actualException != null, "Expected a cause for the RPC exception")

        assert(actualException.isInstanceOf[StateStoreCommitValidationFailed] ||
          actualException.getMessage.contains("[STATE_STORE_COMMIT_VALIDATION_FAILED]"),
          s"Expected STATE_STORE_COMMIT_VALIDATION_FAILED error," +
            s" but got: ${actualException.getMessage}")
      }
    }
  }

  test("StateStore commit validation with swallowed exceptions in foreachBatch") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempDir { tempDir =>
        val checkpointPath = tempDir.getCanonicalPath

        // Create streaming DataFrame
        val streamingDF = spark.readStream
          .format("rate")
          .option("rowsPerSecond", 10)
          .option("numPartitions", 4)
          .load()
          .withColumn("key", col("value") % 10)

        // Stateful aggregation
        val aggregatedDF = streamingDF
          .groupBy("key")
          .agg(sum("value").as("sum"))

        // ForeachBatch that swallows exceptions and returns early
        def problematicBatchProcessor(batchDF: DataFrame, batchId: Long): Unit = {
          try {
            // Process only first few rows
            val firstRows = batchDF.limit(2).collect()

            // Simulate some processing that might fail
            if (firstRows.length > 1) {
              throw new RuntimeException("Processing failed!")
            }
          } catch {
            case _: Exception =>
              // Swallow the exception and return early
              // This means remaining partitions won't be processed
              return
          }

          // This code is never reached due to early return
          batchDF.collect()
        }

        val queryEx = intercept[StreamingQueryException] {
          val query = aggregatedDF.writeStream
            .queryName("swallowed_exception_test")
            .option("checkpointLocation", checkpointPath)
            .foreachBatch(problematicBatchProcessor _)
            .outputMode("update")
            .start()

          query.awaitTermination()
        }

        // Check the cause chain since RPC wraps exceptions
        val rootCause = queryEx.getCause
        assert(rootCause != null, "Expected a root cause for the StreamingQueryException")
        val actualException = rootCause.getCause
        assert(actualException != null, "Expected a cause for the RPC exception")

        assert(actualException.isInstanceOf[StateStoreCommitValidationFailed] ||
          actualException.getMessage.contains("[STATE_STORE_COMMIT_VALIDATION_FAILED]"),
          s"Expected STATE_STORE_COMMIT_VALIDATION_FAILED error," +
            s" but got: ${actualException.getMessage}")
      }
    }
  }

  test("StateStore commit validation with multiple swallowed exceptions") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempDir { tempDir =>
        val checkpointPath = tempDir.getCanonicalPath

        val streamingDF = spark.readStream
          .format("rate")
          .option("rowsPerSecond", 10)
          .load()
          .withColumn("key", col("value") % 5)

        // Multiple aggregations to create multiple StateStores
        val aggregatedDF = streamingDF
          .groupBy("key")
          .agg(
            count("*").as("count"),
            sum("value").as("sum"),
            avg("value").as("avg")
          )

        var processedCount = 0

        // ForeachBatch with multiple exception swallowing points
        def problematicBatchProcessor(batchDF: DataFrame, batchId: Long): Unit = {
          try {
            // First processing attempt
            batchDF.limit(1).collect()
            processedCount += 1

            try {
              // Second processing attempt that fails
              if (processedCount > 0) {
                throw new IllegalStateException("Second processing failed")
              }
            } catch {
              case _: IllegalStateException =>
              // Swallow and continue
            }

            try {
              // Third processing attempt
              batchDF.limit(2).collect()
              throw new RuntimeException("Third processing failed")
            } catch {
              case _: RuntimeException =>
                // Swallow and return early
                return
            }

            // Never reached - full processing
            batchDF.collect()
          } catch {
            case _: Exception =>
            // Outer catch that swallows everything
          }
        }

        val queryEx = intercept[StreamingQueryException] {
          val query = aggregatedDF.writeStream
            .queryName("multiple_swallowed_exceptions_test")
            .option("checkpointLocation", checkpointPath)
            .foreachBatch(problematicBatchProcessor _)
            .outputMode("complete")
            .start()

          query.awaitTermination()
        }

        // Check the cause chain since RPC wraps exceptions
        val rootCause = queryEx.getCause
        assert(rootCause != null, "Expected a root cause for the StreamingQueryException")
        val actualException = rootCause.getCause
        assert(actualException != null, "Expected a cause for the RPC exception")

        assert(actualException.isInstanceOf[StateStoreCommitValidationFailed] ||
          actualException.getMessage.contains("[STATE_STORE_COMMIT_VALIDATION_FAILED]"),
          s"Expected STATE_STORE_COMMIT_VALIDATION_FAILED error," +
            s" but got: ${actualException.getMessage}")
      }
    }
  }

  test("StateStore commit validation can be disabled via configuration") {
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.STATE_STORE_COMMIT_VALIDATION_ENABLED.key -> "false") {
      withTempDir { tempDir =>
        val checkpointPath = tempDir.getCanonicalPath

        val streamingDF = spark.readStream
          .format("rate")
          .option("rowsPerSecond", 3)
          .load()
          .withColumn("key", col("value") % 10)

        val aggregatedDF = streamingDF
          .groupBy("key")
          .agg(count("*").as("count"))

        // ForeachBatch that only processes partial data
        // With validation disabled, this should not fail
        def partialProcessor(batchDF: DataFrame, batchId: Long): Unit = {
          // Only show first 2 rows, won't process all partitions
          batchDF.show(2)
        }

        // This should complete successfully with validation disabled
        val query = aggregatedDF.writeStream
          .queryName("validation_disabled_test")
          .option("checkpointLocation", checkpointPath)
          .foreachBatch(partialProcessor _)
          .outputMode("complete")
          .trigger(Trigger.ProcessingTime("1 second"))
          .start()

        try {
          // Wait for at least 2-3 batches to be processed
          eventually(timeout(streamingTimeout)) {
            assert(query.lastProgress != null, "Query should have made progress")
            assert(query.lastProgress.batchId >= 2,
              s"Query should have processed at least 3 batches, " +
                s"but only processed ${query.lastProgress.batchId + 1}")
          }
        } finally {
          query.stop()
          query.awaitTermination()
        }
      }
    }
  }

  // ============== Helper classes and methods =================

  private class ForeachBatchTester[T: Encoder](memoryStream: MemoryStream[Int]) {
    trait Test
    private case class Check(in: Seq[Int], out: Seq[T]) extends Test
    private case object CheckMetrics extends Test

    private val recordedOutput = new mutable.HashMap[Long, Seq[T]]

    def testWriter(
        ds: Dataset[T],
        outputBatchWriter: (Dataset[T], Long) => Unit,
        outputMode: OutputMode = OutputMode.Append())(tests: Test*): Unit = {
      try {
        var expectedBatchId = -1
        val query = ds.writeStream.outputMode(outputMode).foreachBatch(outputBatchWriter).start()

        tests.foreach {
          case Check(in, out) =>
            expectedBatchId += 1
            memoryStream.addData(in)
            query.processAllAvailable()
            assert(recordedOutput.contains(expectedBatchId))
            val ds: Dataset[T] = spark.createDataset[T](recordedOutput(expectedBatchId))
            checkDataset[T](ds, out: _*)
          case CheckMetrics =>
            assert(query.recentProgress.exists(_.numInputRows > 0))
        }
      } finally {
        sqlContext.streams.active.foreach(_.stop())
      }
    }

    def check(in: Int*)(out: T*): Test = Check(in, out)
    def checkMetrics: Test = CheckMetrics
    def record(batchId: Long, ds: Dataset[T]): Unit =
      recordedOutput.put(batchId, ds.collect().toImmutableArraySeq)
    implicit def conv(x: (Int, Long)): KV = KV(x._1, x._2)
  }
}
