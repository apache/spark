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

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkIllegalArgumentException, SparkThrowable}
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBConf,
  RocksDBStateStoreProvider}
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests the configuration defaults a Real-Time Mode query applies at query start.
 *
 * These are not engine-wide defaults because they are only the right choice for a low-latency,
 * long-running batch. Each is applied only when the user has not set it, so an explicit choice
 * always wins -- both directions are asserted here, since a defaulting block that silently
 * overrode a user's setting would be worse than having no default at all.
 */
class StreamRealTimeModeDefaultConfsSuite extends StreamRealTimeModeSuiteBase {

  import testImplicits._

  private val changelogKey =
    s"${RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX}.changelogCheckpointing.enabled"

  /** Runs a trivial RTM query to completion and returns the effective conf values afterwards. */
  private def runRealTimeQueryAndReadConfs(keys: Seq[String]): Map[String, Option[String]] = {
    val inputData = LowLatencyMemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)
    var observed: Map[String, Option[String]] = Map.empty

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4),
      Execute { q =>
        // Read from sparkSessionForStream, the isolated CLONE the batches actually run with
        // (StreamExecution.sparkSessionForStream). The defaults are applied there, not to the
        // caller's session, so reading `q.sparkSession` would observe the unmodified original.
        val conf = q.sparkSessionForStream.conf
        observed = keys.map(k => k -> conf.getOption(k)).toMap
      },
      StopStream
    )
    observed
  }

  test("a Real-Time Mode query defaults to checkpoint format v2 with the RocksDB state store") {
    val keys = Seq(
      SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key)
    val observed = runRealTimeQueryAndReadConfs(keys)

    assert(observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key).contains("2"),
      "Real-Time Mode should default the state-store checkpoint format to v2, got " +
        observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
    assert(observed(SQLConf.STATE_STORE_PROVIDER_CLASS.key)
        .contains(classOf[RocksDBStateStoreProvider].getName),
      "Real-Time Mode should default the state-store provider to RocksDB, got " +
        observed(SQLConf.STATE_STORE_PROVIDER_CLASS.key))
  }

  test("the checkpoint-format and provider defaults are applied independently") {
    // The two are defaulted by independent guards (matching the runtime): pinning one does not
    // suppress the other. Pin only the version (to a compatible v2, since an incompatible explicit
    // config is rejected up front by the pre-flight) and confirm the provider is still defaulted.
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      val observed = runRealTimeQueryAndReadConfs(
        Seq(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
          SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
      assert(observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key).contains("2"),
        "the pinned version must be left alone, got " +
          observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
      assert(observed(SQLConf.STATE_STORE_PROVIDER_CLASS.key)
          .contains(classOf[RocksDBStateStoreProvider].getName),
        "the provider default is independent of the version, so it must still be RocksDB, got " +
          observed(SQLConf.STATE_STORE_PROVIDER_CLASS.key))
    }
  }

  test("an explicit checkpoint format version is not overridden by Real-Time Mode") {
    // The defaulting guard respects an explicit version rather than forcing 2. v2 is used here
    // (not v1) because an explicit v1 is rejected up front by the pre-flight (covered below).
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      val observed = runRealTimeQueryAndReadConfs(
        Seq(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
      assert(observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key).contains("2"),
        "an explicitly configured checkpoint format version must be left alone, got " +
          observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
    }
  }

  test("an explicit incompatible config is rejected up front for a Real-Time Mode query") {
    // The pre-flight in StreamingQueryManager rejects explicit RTM-incompatible session configs
    // before the query is built: checkpoint format below v2, a non-RocksDB provider, and
    // sortBeforeRepartition=true. Each is reported in one SQL_CONFIGURATION_NOT_SUPPORTED error.
    def assertRejected(confs: (String, String)*): Unit = {
      withSQLConf(confs: _*) {
        val inputData = LowLatencyMemoryStream[Int]
        val e = intercept[SparkIllegalArgumentException] {
          testStream(inputData.toDS(), OutputMode.Update, Map.empty, new ContinuousMemorySink())(
            StartStream())
        }
        checkError(e, condition = "STREAMING_REAL_TIME_MODE.SQL_CONFIGURATION_NOT_SUPPORTED",
          parameters = e.getMessageParameters.asScala.toMap)
      }
    }
    assertRejected(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1")
    assertRejected(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[HDFSBackedStateStoreProvider].getName)
    assertRejected(SQLConf.SORT_BEFORE_REPARTITION.key -> "true")
  }

  test("the checkpoint-v1 escape hatch bypasses the pre-flight version check") {
    // With the escape hatch on, an explicit v1 config is permitted through the pre-flight (the
    // existing-v1-checkpoint fail-fast in initializeExecution is covered separately below).
    withSQLConf(
      SQLConf.STREAMING_REAL_TIME_MODE_DANGEROUSLY_ALLOW_CHECKPOINT_V1.key -> "true",
      SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
      val inputData = LowLatencyMemoryStream[Int]
      testStream(inputData.toDS(), OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, 1),
        StartStream(),
        CheckAnswerWithTimeout(60000, 1),
        StopStream
      )
    }
  }

  test("a Real-Time Mode query defaults changelog checkpointing on with RocksDB") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
      val observed = runRealTimeQueryAndReadConfs(Seq(changelogKey))
      assert(observed(changelogKey).contains("true"),
        s"Real-Time Mode should default $changelogKey to true, got ${observed(changelogKey)}")
    }
  }

  test("an explicit changelog checkpointing setting is not overridden by Real-Time Mode") {
    // The RocksDB provider must be in force, otherwise the `usingRocksDb` gate skips the block and
    // the guard under test is never reached -- the assertion would hold vacuously.
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      changelogKey -> "false") {
      val observed = runRealTimeQueryAndReadConfs(Seq(changelogKey))
      assert(observed(changelogKey).contains("false"),
        s"an explicitly configured $changelogKey must be left alone, got ${observed(changelogKey)}")
    }
  }

  test("switching an existing v1 checkpoint to Real-Time Mode fails fast") {
    // An existing v1 checkpoint carries no explicit config, so the pre-flight cannot see it; the
    // initializeExecution fail-fast catches it instead, from the resolved state-store format.
    // Resolution keeps an existing checkpoint at the version it was created with, so rather than
    // silently running at v1 (where a re-executed batch can reuse the failed batch's state file
    // names and lose data) the query is rejected at start. The rejection is unconditional -- a
    // plain (stateless) query is rejected too.
    withTempDir { checkpointDir =>
      val inputData = LowLatencyMemoryStream[Int]
      val mapped = inputData.toDS().map(_ + 1)
      testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, 1),
        // Phase 1: a micro-batch trigger, which writes a v1 commit log.
        StartStream(
          trigger = Trigger.ProcessingTime("1 second"),
          checkpointLocation = checkpointDir.getAbsolutePath),
        WaitUntilCurrentBatchProcessed,
        StopStream,
        AddData(inputData, 2),
        // Phase 2: the same checkpoint under the Real-Time trigger must be rejected.
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        ExpectFailure[SparkIllegalArgumentException] { e =>
          checkError(
            e.asInstanceOf[SparkThrowable],
            condition = "STREAMING_REAL_TIME_MODE.CHECKPOINT_FORMAT_V1_NOT_SUPPORTED",
            parameters = Map(
              "config" -> SQLConf.STREAMING_REAL_TIME_MODE_DANGEROUSLY_ALLOW_CHECKPOINT_V1.key))
        }
      )
    }
  }

  test("the escape hatch allows Real-Time Mode on an existing v1 checkpoint") {
    withSQLConf(
      SQLConf.STREAMING_REAL_TIME_MODE_DANGEROUSLY_ALLOW_CHECKPOINT_V1.key -> "true") {
      withTempDir { checkpointDir =>
        val inputData = LowLatencyMemoryStream[Int]
        val mapped = inputData.toDS().map(_ + 1)
        testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          AddData(inputData, 1),
          StartStream(
            trigger = Trigger.ProcessingTime("1 second"),
            checkpointLocation = checkpointDir.getAbsolutePath),
          WaitUntilCurrentBatchProcessed,
          StopStream,
          AddData(inputData, 2),
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          // The sink accumulates across both phases, so batch 1's row is still present.
          CheckAnswerWithTimeout(60000, 2, 3),
          StopStream
        )
      }
    }
  }

  test("a fresh Real-Time Mode checkpoint is not rejected") {
    // A fresh checkpoint takes state-store format v2 from the Real-Time Mode defaults, so the
    // rejection does not apply. Only a resolved state-store format v1 -- from an existing
    // incompatible checkpoint or an explicit v1 config without the escape hatch -- is rejected.
    withTempDir { checkpointDir =>
      val inputData = LowLatencyMemoryStream[Int]
      testStream(inputData.toDS(), OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, 1, 2),
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerWithTimeout(60000, 1, 2),
        StopStream
      )
    }
  }

}
