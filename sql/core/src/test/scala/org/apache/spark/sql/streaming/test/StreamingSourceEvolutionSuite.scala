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

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeqLog}
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

  test("without enforcement - naming sources throws error") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.readStream
            .format("org.apache.spark.sql.streaming.test")
            .name("mySource")
            .load()
        },
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.SOURCE_NAMING_NOT_SUPPORTED",
        parameters = Map("name" -> "mySource"))
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
          parameters = Map("existingVersion" -> version.toString))
      }
      verifyNoSourceCreated()
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

  test("new positional query accepts separately analyzed named and unnamed sources") {
    val named = withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      testSource(Some("a"))
    }
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
      val checkpointLocation = new Path(newMetadataDir)
      LastOptions.clear()
      runQuery(named.union(testSource(None)), checkpointLocation)
      verifySourcePath(checkpointLocation, "a")
      verifySourcePath(checkpointLocation, "0")
      val offsets = new OffsetSeqLog(spark, s"$checkpointLocation/offsets").getLatest().get._2
      assert(!offsets.metadataOpt.get.conf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key).toBoolean)
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
        parameters = Map("existingVersion" -> "2"))
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
      parameters = Map("existingVersion" -> "1"))
    verifyNoSourceCreated()
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

  private def verifyNoSourceCreated(): Unit = {
    verify(LastOptions.mockStreamSourceProvider, never())
      .createSource(any(), any(), any(), any(), any())
  }
}
