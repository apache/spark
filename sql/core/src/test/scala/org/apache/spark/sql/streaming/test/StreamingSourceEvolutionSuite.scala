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

package org.apache.spark.sql.streaming.test

import java.io.File
import java.nio.file.Files

import scala.concurrent.duration._

import org.apache.hadoop.fs.Path
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest.Tag

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.streaming.UserProvided
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.{StreamingQueryWrapper, StreamingRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamTest}
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

/**
 * Test suite for streaming source naming and validation.
 * Tests cover the naming API, validation rules, and resolution pipeline.
 */
@SlowSQLTest
class StreamingSourceEvolutionSuite extends StreamTest {

  private def newMetadataDir =
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

  override def afterEach(): Unit = {
    spark.streams.active.foreach(_.stop())
    super.afterEach()
  }

  /**
   * Helper to verify that a source was created with the expected metadata path.
   * @param checkpointLocation the checkpoint location path
   * @param sourcePath the expected source path (e.g., "source1" or "0")
   * @param mode mockito verification mode (default: times(1))
   */
  private def verifySourcePath(
      checkpointLocation: Path,
      sourcePath: String,
      mode: org.mockito.verification.VerificationMode = times(1)): Unit = {
    verify(LastOptions.mockStreamSourceProvider, mode).createSource(
      any(),
      meq(s"${new Path(makeQualifiedPath(
        checkpointLocation.toString)).toString}/sources/$sourcePath"),
      meq(None),
      meq("org.apache.spark.sql.streaming.test"),
      meq(Map.empty))
  }

  // ====================
  // Name Validation Tests
  // ====================

  testWithSourceEvolution("invalid source name - contains hyphen") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my-source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my-source"))
  }

  testWithSourceEvolution("invalid source name - contains space") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my source"))
  }

  testWithSourceEvolution("invalid source name - contains dot") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my.source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my.source"))
  }

  testWithSourceEvolution("invalid source name - contains special characters") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my.source@123")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my.source@123"))
  }

  testWithSourceEvolution("valid source names - various patterns") {
    // Test that valid names work correctly
    Seq("mySource", "my_source", "MySource123", "_private", "source_123_test", "123source")
      .foreach { name =>
        val df = spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name(name)
          .load()
        assert(df.isStreaming, s"DataFrame should be streaming for name: $name")
      }
  }

  testWithSourceEvolution("method chaining - name() returns reader for chaining") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("my_source")
      .option("opt1", "value1")
      .load()

    assert(df.isStreaming, "DataFrame should be streaming")
  }

  // ==========================
  // Duplicate Detection Tests
  // ==========================

  testWithSourceEvolution("duplicate source names - rejected when starting stream") {
    withTempDir { checkpointDir =>
      val df1 = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .name("duplicate_name")
        .load()

      val df2 = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .name("duplicate_name")  // Same name - should fail
        .load()

      checkError(
        exception = intercept[AnalysisException] {
          df1.union(df2).writeStream
            .format("org.apache.spark.sql.streaming.test")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .start()
        },
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.DUPLICATE_SOURCE_NAMES",
        parameters = Map("names" -> "'duplicate_name'"))
    }
  }

  testWithSourceEvolution("enforcement enabled - unnamed source rejected") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .load() // Unnamed - throws error at load() time
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
      parameters = Map("sourceInfo" -> ".*"),
      matchPVals = true)
  }

  testWithSourceEvolution("enforcement enabled - all sources named succeeds") {
    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("alpha")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("beta")
      .load()

    // Should not throw - all sources are named
    val union = df1.union(df2)
    assert(union.isStreaming, "Union should be streaming")
  }

  test("without enforcement - naming a source keeps its name") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
      val df = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .name("mySource")
        .load()

      val sourceNames = df.queryExecution.analyzed.collect {
        case r: StreamingRelation => r.sourceIdentifyingName
      }
      assert(sourceNames === Seq(UserProvided("mySource")))
    }
  }

  // =======================
  // Metadata Path Tests
  // =======================

  testWithSourceEvolution("offset log uses VERSION_2 when source evolution is enabled") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val q = df1.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.processAllAvailable()
    q.stop()

    val offsetLog = new OffsetSeqLog(spark, s"$checkpointLocation/offsets")
    val latestBatch = offsetLog.getLatest()
    assert(latestBatch.isDefined, "Offset log should have at least one entry")
    val (_, offsetSeq) = latestBatch.get
    assert(offsetSeq.isInstanceOf[OffsetMap],
      s"Expected OffsetMap but got ${offsetSeq.getClass.getSimpleName}")
    assert(offsetSeq.version === 2, s"Expected version 2 but got ${offsetSeq.version}")
  }

  testWithSourceEvolution("named sources - metadata path uses source name") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q = df1.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.processAllAvailable()
    q.stop()

    verifySourcePath(checkpointLocation, "source1")
    verifySourcePath(checkpointLocation, "source2")
  }

  test("unnamed sources use positional IDs for metadata path") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
      LastOptions.clear()

      val checkpointLocation = new Path(newMetadataDir)

      val df1 = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load()

      val df2 = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load()

      val q = df1.union(df2).writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", checkpointLocation.toString)
        .trigger(ProcessingTime(10.seconds))
        .start()
      q.processAllAvailable()
      q.stop()

      // Without naming, sources get sequential IDs (Unassigned -> 0, 1, ...)
      verifySourcePath(checkpointLocation, "0")
      verifySourcePath(checkpointLocation, "1")
    }
  }

  // ========================
  // Source Evolution Tests
  // ========================

  testWithSourceEvolution("source evolution - reorder sources with named sources") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    // First query: source1 then source2
    val df1a = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2a = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q1 = df1a.union(df2a).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q1.processAllAvailable()
    q1.stop()

    LastOptions.clear()

    // Second query: source2 then source1 (reordered) - should still work
    val df1b = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2b = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q2 = df2b.union(df1b).writeStream  // Note: reversed order
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q2.processAllAvailable()
    q2.stop()

    // Both sources should still use their named paths
    verifySourcePath(checkpointLocation, "source1", atLeastOnce())
    verifySourcePath(checkpointLocation, "source2", atLeastOnce())
  }

  testWithSourceEvolution("source evolution - add new source with named sources") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    // First query: only source1
    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val q1 = df1.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q1.processAllAvailable()
    q1.stop()

    LastOptions.clear()

    // Second query: add source2
    val df1b = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q2 = df1b.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q2.processAllAvailable()
    q2.stop()

    // Both sources should have been created
    verifySourcePath(checkpointLocation, "source1", atLeastOnce())
    verifySourcePath(checkpointLocation, "source2")
  }

  testWithSourceEvolution("named sources enforcement uses V2 offset log format") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q = df1.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.processAllAvailable()
    q.stop()

    import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeqLog}
    val offsetLog = new OffsetSeqLog(spark,
      makeQualifiedPath(checkpointLocation.toString).toString + "/offsets")
    val offsetSeq = offsetLog.get(0)
    assert(offsetSeq.isDefined, "Offset log should have batch 0")
    assert(offsetSeq.get.isInstanceOf[OffsetMap],
      s"Expected OffsetMap but got ${offsetSeq.get.getClass.getSimpleName}")
  }

  testWithSourceEvolution("names preserved through union operations") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("alpha")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("beta")
      .load()

    val df3 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("gamma")
      .load()

    // Complex union: (alpha union beta) union gamma
    val q = df1.union(df2).union(df3).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.processAllAvailable()
    q.stop()

    // All three sources should use their named paths
    verifySourcePath(checkpointLocation, "alpha")
    verifySourcePath(checkpointLocation, "beta")
    verifySourcePath(checkpointLocation, "gamma")
  }

  testWithSourceEvolution("enforcement config is persisted in offset metadata") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    // Start query with enforcement enabled
    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val q1 = df1.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q1.processAllAvailable()
    q1.stop()

    // Verify config was persisted in offset metadata
    import org.apache.spark.sql.execution.streaming.checkpointing.OffsetSeqLog
    val offsetLog = new OffsetSeqLog(spark,
      makeQualifiedPath(checkpointLocation.toString).toString + "/offsets")
    val offsetSeq = offsetLog.get(0)
    assert(offsetSeq.isDefined, "Offset log should have batch 0")
    assert(offsetSeq.get.metadataOpt.isDefined, "Offset metadata should be present")
    assert(offsetSeq.get.metadataOpt.get.conf.contains(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key),
      "ENABLE_STREAMING_SOURCE_EVOLUTION should be in offset metadata")
    assert(offsetSeq.get.metadataOpt.get.conf(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key) == "true",
      "ENABLE_STREAMING_SOURCE_EVOLUTION should be true in offset metadata")
  }

  test("upgrade from old checkpoint without enforcement config uses default value") {
    import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqMetadata, OffsetSeqMetadataV2}

    // Simulate old checkpoint metadata without ENABLE_STREAMING_SOURCE_EVOLUTION
    val oldMetadata = OffsetSeqMetadata(
      batchWatermarkMs = 0,
      batchTimestampMs = 0,
      conf = Map(
        // Old checkpoint has other configs but not ENABLE_STREAMING_SOURCE_EVOLUTION
        SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key -> "max"
      )
    )

    // Verify that reading the config returns the default value (false)
    val value = OffsetSeqMetadata.readValueOpt(
      oldMetadata, SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION)
    assert(value.contains("false"),
      s"Expected default value 'false' for missing config, but got: $value")

    // Also test with V2 metadata
    val oldMetadataV2 = OffsetSeqMetadataV2(
      batchWatermarkMs = 0,
      batchTimestampMs = 0,
      conf = Map(
        SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key -> "max"
      )
    )

    val valueV2 = OffsetSeqMetadata.readValueOpt(
      oldMetadataV2, SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION)
    assert(valueV2.contains("false"),
      s"Expected default value 'false' for missing config in V2, but got: $valueV2")
  }

  // ================================
  // Checkpoint Source Tracking Tests
  // ================================

  test("restart that keeps the checkpoint's source tracking succeeds") {
    val positionalCheckpoint = new Path(newMetadataDir)
    withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
      runQuery(testSource(None), positionalCheckpoint)
      LastOptions.clear()
      runQuery(testSource(None), positionalCheckpoint)
    }
    verifySourcePath(positionalCheckpoint, "0")

    val namedCheckpoint = new Path(newMetadataDir)
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      runQuery(testSource(Some("source1")), namedCheckpoint)
      LastOptions.clear()
      runQuery(testSource(Some("source1")), namedCheckpoint)
    }
    verifySourcePath(namedCheckpoint, "source1")
  }

  test("named checkpoint - restart without names is rejected before creating sources") {
    val checkpointLocation = new Path(newMetadataDir)
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      runQuery(testSource(Some("source1")), checkpointLocation)
    }

    LastOptions.clear()
    checkError(
      exception = startExpectingFailure(testSource(None), checkpointLocation),
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.CANNOT_DISABLE_ON_EXISTING_CHECKPOINT",
      parameters = Map.empty)
    verifyNoSourceCreated()
  }

  Seq(1, 2).foreach { version =>
    test(s"positional v$version checkpoint - restart with names is rejected before creating " +
        "sources") {
      val checkpointLocation = new Path(newMetadataDir)
      withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> version.toString) {
        runQuery(testSource(None), checkpointLocation)
      }

      LastOptions.clear()
      withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
        checkError(
          exception = startExpectingFailure(testSource(Some("source1")), checkpointLocation),
          condition = "STREAMING_QUERY_EVOLUTION_ERROR.CANNOT_ENABLE_ON_EXISTING_CHECKPOINT",
          parameters = Map.empty)
      }
      verifyNoSourceCreated()
    }
  }

  gridTest("positional checkpoint written under source names keeps running with those names")(
      Seq(1, 2)) { version =>
    withTable("legacy_named_source") {
      sql("CREATE TABLE legacy_named_source (id INT) USING parquet")
      sql("INSERT INTO legacy_named_source VALUES (1)")
      val checkpointDir = Utils.createTempDir(namePrefix = "streaming.metadata")
      val outputDir = Utils.createTempDir(namePrefix = "streaming.output")
      def runSqlQuery(query: String): Unit = {
        val q = sql(query).writeStream
          .format("parquet")
          .option("path", outputDir.getCanonicalPath)
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .start()
        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }

      withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> version.toString) {
        runSqlQuery("SELECT * FROM STREAM legacy_named_source")
      }
      // Spark 4.2 ran a catalog table named by IDENTIFIED BY by position, with the source
      // metadata in sources/<name>.
      assert(new File(checkpointDir, "sources/0").renameTo(new File(checkpointDir, "sources/src")))
      sql("INSERT INTO legacy_named_source VALUES (2)")

      val e = intercept[StreamingQueryException] {
        runSqlQuery("SELECT * FROM STREAM legacy_named_source IDENTIFIED BY other")
      }
      checkError(
        exception = e.cause.asInstanceOf[AnalysisException],
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.CANNOT_ENABLE_ON_EXISTING_CHECKPOINT",
        parameters = Map.empty)
      assert(!new File(checkpointDir, "sources/other").exists())

      runSqlQuery("SELECT * FROM STREAM legacy_named_source IDENTIFIED BY src")
      checkAnswer(spark.read.parquet(outputDir.getCanonicalPath), Seq(Row(1), Row(2)))
      val offsetLog = new OffsetSeqLog(spark, s"${checkpointDir.getCanonicalPath}/offsets")
      val (latestBatchId, latestOffsets) = offsetLog.getLatest().get
      assert(latestBatchId === 1)
      assert(latestOffsets.version === version)
      assert(latestOffsets.metadataOpt.get.conf(
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key) === "false")
      if (version == 2) {
        assert(latestOffsets.asInstanceOf[OffsetMap].offsetsMap.keySet === Set("0"))
      }
    }
  }

  test("named and unnamed sources analyzed separately are rejected before creating sources") {
    LastOptions.clear()
    val checkpointLocation = new Path(newMetadataDir)
    // The union is analyzed after each side, so no NamedStreamingRelation is left to check.
    val unnamed = testSource(None)
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      checkError(
        exception = startExpectingFailure(
          testSource(Some("source1")).union(unnamed), checkpointLocation),
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map("sourceInfo" -> "Leaf position 1 - dummySource"))
    }
    verifyNoSourceCreated()
  }

  // ==================================
  // Source Naming Without Config Tests
  // ==================================

  test("named source without the config - new query uses source evolution") {
    LastOptions.clear()
    val checkpointLocation = new Path(newMetadataDir)
    runQuery(testSource(Some("source1")), checkpointLocation)

    verifySourcePath(checkpointLocation, "source1")
    val offsetSeq = offsetLogAt(checkpointLocation).get(0).get
    assert(offsetSeq.version === 2)
    assert(offsetSeq.asInstanceOf[OffsetMap].offsetsMap.keySet === Set("source1"))
    assert(offsetSeq.metadataOpt.get.conf(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key) === "true")
    assert(!spark.sessionState.conf.enableStreamingSourceEvolution)
  }

  test("named source without the config - restart keeps source evolution") {
    val checkpointLocation = new Path(newMetadataDir)
    runQuery(testSource(Some("source1")), checkpointLocation)

    LastOptions.clear()
    val q = startQuery(testSource(Some("source1")), checkpointLocation)
    try {
      q.processAllAvailable()
      val querySession = q.asInstanceOf[StreamingQueryWrapper].streamingQuery.sparkSessionForStream
      assert(querySession.sessionState.conf.enableStreamingSourceEvolution)
    } finally {
      q.stop()
    }
    verifySourcePath(checkpointLocation, "source1")

    LastOptions.clear()
    checkError(
      exception = startExpectingFailure(testSource(None), checkpointLocation),
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.CANNOT_DISABLE_ON_EXISTING_CHECKPOINT",
      parameters = Map.empty)
    verifyNoSourceCreated()
  }

  test("named source without the config - unnamed query in the same session is unaffected") {
    runQuery(testSource(Some("source1")), new Path(newMetadataDir))

    LastOptions.clear()
    val checkpointLocation = new Path(newMetadataDir)
    runQuery(testSource(None), checkpointLocation)

    verifySourcePath(checkpointLocation, "0")
    val offsetSeq = offsetLogAt(checkpointLocation).get(0).get
    assert(offsetSeq.isInstanceOf[OffsetSeq])
    assert(offsetSeq.metadataOpt.get.conf(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key) === "false")
  }

  test("named source without the config - unnamed source in the same query is rejected") {
    LastOptions.clear()
    checkError(
      exception = startExpectingFailure(
        testSource(Some("source1")).union(testSource(None)), new Path(newMetadataDir)),
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
      parameters = Map("sourceInfo" -> "Leaf position 1 - dummySource"))
    verifyNoSourceCreated()
  }

  test("SQL IDENTIFIED BY without the config - new query uses source evolution") {
    withTable("named_sql_source") {
      sql("CREATE TABLE named_sql_source (id INT) USING parquet")
      sql("INSERT INTO named_sql_source VALUES (1)")
      val checkpointLocation = new Path(newMetadataDir)
      runQuery(
        sql("SELECT * FROM STREAM named_sql_source IDENTIFIED BY src"), checkpointLocation)

      val offsetSeq = offsetLogAt(checkpointLocation).get(0).get
      assert(offsetSeq.asInstanceOf[OffsetMap].offsetsMap.keySet === Set("src"))
      assert(offsetSeq.metadataOpt.get.conf(
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key) === "true")
    }
  }

  test("SQL IDENTIFIED BY without the config - unnamed source in the same query is rejected") {
    withTable("named_sql_source", "unnamed_sql_source") {
      sql("CREATE TABLE named_sql_source (id INT) USING parquet")
      sql("CREATE TABLE unnamed_sql_source (id INT) USING parquet")
      val data = sql("SELECT * FROM STREAM named_sql_source IDENTIFIED BY src " +
        "UNION ALL SELECT * FROM STREAM unnamed_sql_source")
      val checkpoint = newMetadataDir
      checkError(
        exception = startExpectingFailure(data, new Path(checkpoint)),
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map("sourceInfo" -> "(?s)Leaf position 1 - .*"),
        matchPVals = true)
      assert(!new File(checkpoint, "sources").exists())
    }
  }

  test("new query with the config - unnamed source analyzed without it is rejected before " +
      "creating sources") {
    LastOptions.clear()
    // Analyzed without the config, the source has no NamedStreamingRelation left to check.
    val unnamed = testSource(None)
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      checkError(
        exception = startExpectingFailure(unnamed, new Path(newMetadataDir)),
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map("sourceInfo" -> "Leaf position 0 - dummySource"))
    }
    verifyNoSourceCreated()
  }

  test("new named query rejects separately analyzed named and unnamed sources") {
    val named = withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      testSource(Some("a"))
    }
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
      val checkpointLocation = new Path(newMetadataDir)
      LastOptions.clear()
      checkError(
        exception = startExpectingFailure(named.union(testSource(None)), checkpointLocation),
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map("sourceInfo" -> "Leaf position 1 - dummySource"))
      verifyNoSourceCreated()
    }
  }

  test("named checkpoint - restart with an unnamed source is rejected before creating sources") {
    val checkpointLocation = new Path(newMetadataDir)
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      runQuery(testSource(Some("a")).union(testSource(Some("b"))), checkpointLocation)
    }

    LastOptions.clear()
    val unnamed = testSource(None)
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      checkError(
        exception = startExpectingFailure(
          testSource(Some("a")).union(unnamed), checkpointLocation),
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map("sourceInfo" -> "Leaf position 1 - dummySource"))
    }
    verifyNoSourceCreated()
  }

  test("positional checkpoint - restart with named and unnamed sources is rejected before " +
      "creating sources") {
    val checkpointLocation = new Path(newMetadataDir)
    withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
      runQuery(testSource(None).union(testSource(None)), checkpointLocation)
    }

    LastOptions.clear()
    val unnamed = testSource(None)
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      checkError(
        exception = startExpectingFailure(
          testSource(Some("a")).union(unnamed), checkpointLocation),
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.CANNOT_ENABLE_ON_EXISTING_CHECKPOINT",
        parameters = Map.empty)
    }
    verifyNoSourceCreated()
  }

  test("positional checkpoint without offset metadata - restart with names is rejected before " +
      "creating sources") {
    val checkpointLocation = new Path(newMetadataDir)
    runQuery(testSource(None), checkpointLocation)
    // The offset log reads an empty metadata line as an entry without metadata.
    val offsetFile = new File(checkpointLocation.toString, "offsets/0")
    val lines = Files.readAllLines(offsetFile.toPath)
    lines.set(1, "")
    Files.write(offsetFile.toPath, lines)
    new File(offsetFile.getParentFile, ".0.crc").delete()
    val named = withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      testSource(Some("source1"))
    }

    LastOptions.clear()
    checkError(
      exception = startExpectingFailure(named, checkpointLocation),
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.CANNOT_ENABLE_ON_EXISTING_CHECKPOINT",
      parameters = Map.empty)
    verifyNoSourceCreated()
  }

  gridTest("legacy mixed sources recover without re-reading file metadata")(
      for (version <- Seq(1, 2); api <- Seq("sql", "view", "dataframe")) yield (version, api)) {
    case (version, api) =>
      withTable("legacy_left", "legacy_right") {
        sql("CREATE TABLE legacy_left (id INT) USING parquet")
        sql("CREATE TABLE legacy_right (id INT) USING parquet")
        sql("INSERT INTO legacy_left VALUES (1)")
        sql("INSERT INTO legacy_right VALUES (10)")
        withTempDir { checkpoint =>
          withTempDir { output =>
            withSQLConf(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> version.toString) {
              runFileQuery(sql("SELECT * FROM STREAM legacy_left UNION ALL " +
                "SELECT * FROM STREAM legacy_right"), checkpoint, output)
            }
            assert(new File(checkpoint, "sources/0").renameTo(new File(checkpoint, "sources/src")))
            assert(new File(checkpoint, "sources/1").renameTo(new File(checkpoint, "sources/0")))
            sql("INSERT INTO legacy_left VALUES (2)")
            sql("INSERT INTO legacy_right VALUES (20)")
            withTempView("legacy_view") {
              spark.readStream.table("legacy_right").createOrReplaceTempView("legacy_view")
              val data = api match {
                case "dataframe" => spark.readStream.name("src").table("legacy_left")
                  .union(spark.readStream.table("legacy_right"))
                case "view" => sql("SELECT * FROM STREAM legacy_left IDENTIFIED BY src " +
                  "UNION ALL SELECT * FROM STREAM legacy_view")
                case _ => sql("SELECT * FROM STREAM legacy_left IDENTIFIED BY src " +
                  "UNION ALL SELECT * FROM STREAM legacy_right")
              }
              runFileQuery(data, checkpoint, output)
            }
            checkAnswer(spark.read.parquet(output.getCanonicalPath),
              Seq(Row(1), Row(10), Row(2), Row(20)))
            val offsets = new OffsetSeqLog(spark, s"$checkpoint/offsets").getLatest().get._2
            assert(offsets.version == version)
            assert(offsets.metadataOpt.get.conf(
              SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key) == "false")
          }
        }
      }
  }

  test("legacy recovery requires every source name to retain its metadata directory") {
    withTable("legacy_first", "legacy_second") {
      sql("CREATE TABLE legacy_first (id INT) USING parquet")
      sql("CREATE TABLE legacy_second (id INT) USING parquet")
      sql("INSERT INTO legacy_first VALUES (1)")
      sql("INSERT INTO legacy_second VALUES (10)")
      withTempDir { checkpoint =>
        withTempDir { output =>
          runFileQuery(sql("SELECT * FROM STREAM legacy_first UNION ALL " +
            "SELECT * FROM STREAM legacy_second"), checkpoint, output)
          assert(new File(checkpoint, "sources/0").renameTo(new File(checkpoint, "sources/a")))
          assert(new File(checkpoint, "sources/1").renameTo(new File(checkpoint, "sources/b")))
          val renamed = spark.readStream.name("changed").table("legacy_first")
            .union(spark.readStream.name("b").table("legacy_second"))
          checkError(
            exception = startExpectingFailure(renamed, new Path(checkpoint.getCanonicalPath)),
            condition = "STREAMING_QUERY_EVOLUTION_ERROR.CANNOT_ENABLE_ON_EXISTING_CHECKPOINT",
            parameters = Map.empty)
          assert(!new File(checkpoint, "sources/changed").exists())
          sql("INSERT INTO legacy_first VALUES (2)")
          val unchanged = spark.readStream.name("a").table("legacy_first")
            .union(spark.readStream.name("b").table("legacy_second"))
          runFileQuery(unchanged, checkpoint, output)
          checkAnswer(spark.read.parquet(output.getCanonicalPath), Seq(Row(1), Row(10), Row(2)))
        }
      }
    }
  }

  test("SQL source names are validated before a new named source is created") {
    withTable("sql_name_validation") {
      sql("CREATE TABLE sql_name_validation (id INT) USING parquet")
      Seq("orders:v1", "orders-v1", "orders v1").foreach { name =>
        withTempDir { checkpoint =>
          val data = sql(s"SELECT * FROM STREAM sql_name_validation IDENTIFIED BY `$name`")
          checkError(
            exception = startExpectingFailure(data, new Path(checkpoint.getCanonicalPath)),
            condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
            parameters = Map("sourceName" -> name))
          assert(!new File(checkpoint, "sources").exists())
        }
      }
    }
  }

  gridTest("valid SQL source names preserve named offsets across restarts")(
      Seq("orders_v1", "123")) { name =>
    withTable("sql_named_recovery") {
      sql("CREATE TABLE sql_named_recovery (id INT) USING parquet")
      sql("INSERT INTO sql_named_recovery VALUES (1)")
      withTempDir { checkpoint =>
        withTempDir { output =>
          def data: DataFrame =
            sql(s"SELECT * FROM STREAM sql_named_recovery IDENTIFIED BY `$name`")
          runFileQuery(data, checkpoint, output)
          sql("INSERT INTO sql_named_recovery VALUES (2)")
          runFileQuery(data, checkpoint, output)
          checkAnswer(spark.read.parquet(output.getCanonicalPath), Seq(Row(1), Row(2)))
          val offsets = new OffsetSeqLog(spark, s"$checkpoint/offsets").getLatest().get._2
            .asInstanceOf[OffsetMap]
          assert(offsets.offsetsMap.keySet == Set(name))
          val invalid = sql("SELECT * FROM STREAM sql_named_recovery IDENTIFIED BY `orders:v1`")
          checkError(
            exception = startExpectingFailure(invalid, new Path(checkpoint.getCanonicalPath)),
            condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
            parameters = Map("sourceName" -> "orders:v1"))
          assert(!new File(checkpoint, "sources/orders:v1").exists())
        }
      }
    }
  }

  test("continuous processing rejects DataFrame and SQL source names before source creation") {
    withTable("continuous_named_source") {
      sql("CREATE TABLE continuous_named_source (id INT) USING parquet")
      Seq[() => DataFrame](
        () => spark.readStream.format("rate").name("events").load(),
        () => sql("SELECT * FROM STREAM continuous_named_source IDENTIFIED BY events"))
        .foreach { read =>
          withTempDir { checkpoint =>
            checkError(
              exception = intercept[SparkIllegalArgumentException] {
                read().writeStream.format("console")
                  .option("checkpointLocation", checkpoint.getCanonicalPath)
                  .trigger(Continuous("1 second")).start()
              },
              condition = "STREAMING_QUERY_EVOLUTION_ERROR." +
                "SOURCE_EVOLUTION_CONTINUOUS_PROCESSING_NOT_SUPPORTED",
              parameters = Map.empty)
            assert(!new File(checkpoint, "sources").exists())
          }
        }
    }
  }

  test("continuous recovery rejects a checkpoint that tracks sources by name") {
    withTempDir { checkpoint =>
      val named = spark.readStream.format("rate").option("numPartitions", 1).name("events").load()
      val microBatch = named.writeStream.format("console")
        .option("checkpointLocation", checkpoint.getCanonicalPath).trigger(Once()).start()
      try {
        microBatch.awaitTermination()
      } finally {
        microBatch.stop()
      }
      val offsets = new OffsetSeqLog(spark, s"$checkpoint/offsets").getLatest().get._2
      assert(offsets.metadataOpt.get.conf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key) == "true")
      val unnamed = spark.readStream.format("rate").option("numPartitions", 1).load()
      val continuous = unnamed.writeStream.format("console")
        .option("checkpointLocation", checkpoint.getCanonicalPath)
        .trigger(Continuous("1 second")).start()
      try {
        val error = intercept[StreamingQueryException](continuous.awaitTermination())
        checkError(
          exception = error.cause.asInstanceOf[SparkIllegalArgumentException],
          condition = "STREAMING_QUERY_EVOLUTION_ERROR." +
            "SOURCE_EVOLUTION_CONTINUOUS_PROCESSING_NOT_SUPPORTED",
          parameters = Map.empty)
      } finally {
        continuous.stop()
      }
    }
  }

  gridTest("streaming temporary views cannot be assigned a source name")(Seq(false, true)) {
    enabled =>
      withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> enabled.toString) {
        withTempView("named_streaming_view") {
          spark.readStream.format("rate").name("underlying").load()
            .createOrReplaceTempView("named_streaming_view")
          Seq[() => DataFrame](
            () => spark.readStream.name("view_name").table("named_streaming_view"),
            () => sql("SELECT * FROM STREAM named_streaming_view IDENTIFIED BY view_name"))
            .foreach { read =>
              checkError(
                exception = intercept[AnalysisException](read()),
                condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNSUPPORTED_SOURCE_NAMING",
                parameters = Map("sourceName" -> "view_name", "sourceType" -> "SubqueryAlias"))
            }
        }
      }
  }

  // ==============
  // Helper Methods
  // ==============

  /**
   * Helper method to run tests with source evolution enabled.
   * Enabling source evolution automatically forces offset log format V2 (OffsetMap) for new
   * queries, since named sources require it.
   */
  def testWithSourceEvolution(testName: String, testTags: Tag*)(testBody: => Any): Unit = {
    test(testName, testTags: _*) {
      withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
        testBody
      }
    }
  }

  private def runFileQuery(data: DataFrame, checkpoint: File, output: File): Unit = {
    val query = data.writeStream.format("parquet")
      .option("checkpointLocation", checkpoint.getCanonicalPath)
      .start(output.getCanonicalPath)
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }
  }

  private def testSource(name: Option[String]): DataFrame = {
    val reader = spark.readStream.format("org.apache.spark.sql.streaming.test")
    name.fold(reader)(reader.name).load()
  }

  private def startQuery(df: DataFrame, checkpointLocation: Path): StreamingQuery = {
    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .start()
  }

  private def runQuery(df: DataFrame, checkpointLocation: Path): Unit = {
    val q = startQuery(df, checkpointLocation)
    q.processAllAvailable()
    q.stop()
  }

  /** Starts `df` on `checkpointLocation` and returns the error that fails the query. */
  private def startExpectingFailure(df: DataFrame, checkpointLocation: Path): AnalysisException = {
    val e = intercept[StreamingQueryException] {
      startQuery(df, checkpointLocation).processAllAvailable()
    }
    e.cause.asInstanceOf[AnalysisException]
  }

  private def offsetLogAt(checkpointLocation: Path): OffsetSeqLog = {
    new OffsetSeqLog(spark, makeQualifiedPath(checkpointLocation.toString).toString + "/offsets")
  }

  private def verifyNoSourceCreated(): Unit = {
    verify(LastOptions.mockStreamSourceProvider, never())
      .createSource(any(), any(), any(), any(), any())
  }
}
