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

import java.io.File

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NamedStreamingRelation
import org.apache.spark.sql.catalyst.streaming.UserProvided
import org.apache.spark.sql.classic.{DataFrame => ClassicDataFrame, Dataset => ClassicDataset}
import org.apache.spark.sql.execution.datasources.v2.{StreamingDataSourceV2Relation, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

/**
 * Tests for streaming source naming and source evolution under Real-Time Mode (RTM).
 *
 * Source evolution and RTM were developed independently and their startup paths meet in
 * [[org.apache.spark.sql.execution.streaming.runtime.MicroBatchExecution]]: source evolution makes
 * `sourceIdMap` name-keyed and forces offset log v2 ([[OffsetMap]]), while RTM writes its end
 * offsets at the *end* of a batch keyed by the physical scan's `SparkDataStream` rather than by
 * source id. These tests pin down that the name-keyed checkpoint bookkeeping survives the
 * RTM-specific offset path.
 *
 * All tests use [[LowLatencyMemoryStream]], which supports RTM. Names are attached by wrapping the
 * stream's logical plan in a [[NamedStreamingRelation]] (see `namedStream`), because the `.name()`
 * API lives on `DataStreamReader` and a memory stream is not reachable through `spark.readStream`.
 * Kafka coverage that exercises the `.name()` API directly lives in
 * `org.apache.spark.sql.kafka010.KafkaRealTimeModeSourceEvolutionSuite`.
 */
@SlowSQLTest
class RealTimeModeSourceEvolutionSuite extends StreamRealTimeModeSuiteBase {
  import testImplicits._

  /**
   * Timeout for RTM answer checks. RTM batches are time-boxed rather than data-driven, so a check
   * has to wait for at least one batch boundary.
   */
  private val rtmTimeoutMs = 60000L

  override def afterEach(): Unit = {
    spark.streams.active.foreach(_.stop())
    super.afterEach()
  }

  // ==============
  // Helper Methods
  // ==============

  /** Runs `testBody` with source evolution (and therefore named-source enforcement) enabled. */
  private def testWithSourceEvolution(testName: String)(testBody: => Any): Unit = {
    test(testName) {
      withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
        testBody
      }
    }
  }

  /**
   * Attaches `sourceName` to an RTM-capable memory stream. The stream's `StreamingRelationV2`
   * implements `HasStreamingSourceIdentifyingName`, so the `NameStreamingSources` analyzer rule
   * unwraps this node and propagates the name onto the relation.
   */
  private def namedStream[A](
      input: LowLatencyMemoryStream[A],
      sourceName: String): ClassicDataFrame = {
    val plan = input.toDF().queryExecution.logical
    ClassicDataset.ofRows(spark, NamedStreamingRelation(plan, UserProvided(sourceName)))
  }

  /** Runs an RTM query over `df` against a fresh in-memory sink. */
  private def testRealTimeStream(df: ClassicDataFrame)(actions: StreamAction*): Unit = {
    testStream(df, OutputMode.Update, sink = new ContinuousMemorySink())(actions: _*)
  }

  /**
   * The per-source checkpoint path suffixes the query resolved, read off the executed plan's
   * `StreamingDataSourceV2Relation.metadataPath`. Read from the plan rather than the filesystem
   * because a memory stream never writes anything under `sources/`, so the directories are not
   * materialized on disk.
   */
  private def sourceMetadataPathSuffixes(query: StreamExecution): Set[String] = {
    val paths = query.logicalPlan.collect {
      case r: StreamingDataSourceV2ScanRelation => r.relation.metadataPath
      case r: StreamingDataSourceV2Relation => r.metadataPath
    }
    assert(paths.nonEmpty, s"Expected at least one V2 streaming relation in ${query.logicalPlan}")
    paths.map { path =>
      val marker = "/sources/"
      val index = path.indexOf(marker)
      assert(index >= 0, s"Expected a sources path, got $path")
      path.substring(index + marker.length)
    }.toSet
  }

  /** Reads the latest offset log entry, asserting it is a v2 name-keyed [[OffsetMap]]. */
  private def latestOffsetMap(checkpointDir: File): OffsetMap = {
    val offsetLog = new OffsetSeqLog(spark, checkpointDir.getAbsolutePath + "/offsets")
    val latest = offsetLog.getLatest()
    assert(latest.isDefined, "Offset log should have at least one entry")
    latest.get._2 match {
      case offsetMap: OffsetMap => offsetMap
      case other =>
        fail(s"Expected OffsetMap (offset log v2) but got ${other.getClass.getSimpleName}")
    }
  }

  // =============================================
  // Named sources drive the RTM checkpoint layout
  // =============================================

  testWithSourceEvolution("RTM named source uses name-keyed checkpoint paths and offset log") {
    withTempDir { checkpointDir =>
      val input = LowLatencyMemoryStream.singlePartition[Int]

      testRealTimeStream(namedStream(input, "alpha"))(
        AddData(input, 1, 2, 3),
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 1, 2, 3),
        // The source's metadata path is keyed by name, not by positional id.
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("alpha")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )

      // RTM writes its end offsets at end of batch, keyed by the physical scan's stream object.
      // They must still resolve back to the source name when persisted.
      val offsetMap = latestOffsetMap(checkpointDir)
      assert(offsetMap.offsetsMap.keySet === Set("alpha"))
      assert(offsetMap.offsetsMap("alpha").isDefined,
        "The active named source should have a real offset")

      // Source evolution forces offset log v2.
      assert(offsetMap.version === 2)

      // The enforcement flag is pinned in the offset metadata so restarts inherit it.
      val metadata = offsetMap.metadataOpt
      assert(metadata.isDefined, "Offset metadata should be present")
      assert(metadata.get.conf.get(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key)
        .contains("true"))
    }
  }

  testWithSourceEvolution("RTM named sources in a union get separate name-keyed paths") {
    withTempDir { checkpointDir =>
      val input1 = LowLatencyMemoryStream.singlePartition[Int]
      val input2 = LowLatencyMemoryStream.singlePartition[Int]
      val unioned = namedStream(input1, "alpha").union(namedStream(input2, "beta"))

      testRealTimeStream(unioned)(
        AddData(input1, 1, 2),
        AddData(input2, 3, 4),
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 1, 2, 3, 4),
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("alpha", "beta")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )

      // Both RTM scans' end offsets must resolve to their own names. A collision here would
      // surface as a lost or overwritten offset for one of the two sources.
      val offsetMap = latestOffsetMap(checkpointDir)
      assert(offsetMap.offsetsMap.keySet === Set("alpha", "beta"))
      assert(offsetMap.offsetsMap.values.forall(_.isDefined),
        s"Both named sources should have real offsets, got ${offsetMap.offsetsMap}")
    }
  }

  testWithSourceEvolution("RTM named sources keep their offsets when reordered in the union") {
    withTempDir { checkpointDir =>
      val checkpointPath = checkpointDir.getAbsolutePath
      val input1 = LowLatencyMemoryStream.singlePartition[Int]
      val input2 = LowLatencyMemoryStream.singlePartition[Int]

      // Run 1: alpha then beta.
      testRealTimeStream(namedStream(input1, "alpha").union(namedStream(input2, "beta")))(
        AddData(input1, 1),
        AddData(input2, 2),
        StartStream(checkpointLocation = checkpointPath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 1, 2),
        WaitUntilCurrentBatchProcessed,
        StopStream
      )

      val offsetsAfterFirstRun = latestOffsetMap(checkpointDir).offsetsMap
        .collect { case (name, Some(offset)) => name -> offset.json() }
      assert(offsetsAfterFirstRun.keySet === Set("alpha", "beta"))

      // Run 2: beta then alpha. The plan's leaf order changed, but names are position-independent,
      // so each source must resume from its own offset rather than the other's.
      testRealTimeStream(namedStream(input2, "beta").union(namedStream(input1, "alpha")))(
        AddData(input1, 3),
        AddData(input2, 4),
        StartStream(checkpointLocation = checkpointPath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 3, 4),
        // Reordering must not fall back to positional source paths.
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("alpha", "beta")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )

      val offsetsAfterSecondRun = latestOffsetMap(checkpointDir).offsetsMap
        .collect { case (name, Some(offset)) => name -> offset.json() }
      assert(offsetsAfterSecondRun.keySet === Set("alpha", "beta"))
      // Each source advanced past where it was, and neither inherited the other's offset.
      offsetsAfterSecondRun.foreach { case (name, offset) =>
        assert(offset != offsetsAfterFirstRun(name),
          s"Source $name should have advanced after run 2")
      }
    }
  }

  testWithSourceEvolution("RTM restart with an added named source keeps existing source state") {
    withTempDir { checkpointDir =>
      val checkpointPath = checkpointDir.getAbsolutePath
      val input1 = LowLatencyMemoryStream.singlePartition[Int]

      // Run 1: only alpha.
      testRealTimeStream(namedStream(input1, "alpha"))(
        AddData(input1, 1),
        StartStream(checkpointLocation = checkpointPath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 1),
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("alpha")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )
      val alphaOffsetAfterFirstRun =
        latestOffsetMap(checkpointDir).offsetsMap("alpha").map(_.json())
      assert(alphaOffsetAfterFirstRun.isDefined)

      // Run 2: add beta.
      val input2 = LowLatencyMemoryStream.singlePartition[Int]
      testRealTimeStream(namedStream(input1, "alpha").union(namedStream(input2, "beta")))(
        AddData(input1, 2),
        AddData(input2, 3),
        StartStream(checkpointLocation = checkpointPath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 2, 3),
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("alpha", "beta")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )

      // alpha resumed from its own checkpointed offset (it advanced rather than restarting), and
      // beta was added alongside it under its own name.
      val offsetMap = latestOffsetMap(checkpointDir)
      assert(offsetMap.offsetsMap.keySet === Set("alpha", "beta"))
      assert(offsetMap.offsetsMap("alpha").map(_.json()) != alphaOffsetAfterFirstRun,
        "alpha should have advanced past the offset it committed in run 1")
      assert(offsetMap.offsetsMap("beta").isDefined,
        "The newly added beta source should have a real offset")
    }
  }

  // ==========================
  // Enforcement under RTM
  // ==========================

  testWithSourceEvolution("RTM rejects duplicate source names") {
    withTempDir { checkpointDir =>
      val input1 = LowLatencyMemoryStream.singlePartition[Int]
      val input2 = LowLatencyMemoryStream.singlePartition[Int]

      // Two distinct streams sharing one name would collide on a single `sources/<name>` path
      // and on a single offset-map key. The duplicate check runs during `start()` analysis and
      // throws synchronously, so it surfaces out of `testStream`. `ContinuousMemorySink` is used
      // because it is RTM-allowlisted; the RTM sink check runs before write analysis and would
      // otherwise mask the error under test.
      checkError(
        exception = intercept[AnalysisException] {
          testRealTimeStream(
            namedStream(input1, "same").union(namedStream(input2, "same")))(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath)
          )
        },
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.DUPLICATE_SOURCE_NAMES",
        parameters = Map("names" -> "'same'"))
    }
  }

  // ===========================================
  // Baseline: RTM without source evolution
  // ===========================================

  test("RTM without source evolution keeps positional source paths") {
    withTempDir { checkpointDir =>
      val input = LowLatencyMemoryStream.singlePartition[Int]

      // Baseline for the tests above: with enforcement off, an RTM query is unchanged and still
      // uses positional source ids.
      testRealTimeStream(input.toDF())(
        AddData(input, 1, 2),
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerRowsContainsWithTimeout(rtmTimeoutMs, 1, 2),
        Execute { q => assert(sourceMetadataPathSuffixes(q) === Set("0")) },
        WaitUntilCurrentBatchProcessed,
        StopStream
      )
    }
  }
}
