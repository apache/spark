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

  test("pinning the state-store provider suppresses the checkpoint-format default") {
    // The two move together: checkpoint format v2 is incompatible with
    // HDFSBackedStateStoreProvider, which throws on checkpointFormatVersion > 1. Defaulting the
    // version for a user who pinned the HDFS provider would break a query that works today.
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[HDFSBackedStateStoreProvider].getName) {
      val observed = runRealTimeQueryAndReadConfs(
        Seq(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
          SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
      assert(observed(SQLConf.STATE_STORE_PROVIDER_CLASS.key)
          .contains(classOf[HDFSBackedStateStoreProvider].getName),
        "a pinned state-store provider must be left alone, got " +
          observed(SQLConf.STATE_STORE_PROVIDER_CLASS.key))
      assert(!observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key).contains("2"),
        "pinning the provider must also suppress the checkpoint-format default, got " +
          observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
    }
  }

  test("an explicit checkpoint format version is not overridden by Real-Time Mode") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
      val observed = runRealTimeQueryAndReadConfs(
        Seq(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
      assert(observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key).contains("1"),
        "an explicitly configured checkpoint format version must be left alone, got " +
          observed(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
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

  test("changelog checkpointing is not defaulted when the state store is not RocksDB") {
    // The config is RocksDB-specific, so setting it for the HDFS-backed provider would be
    // meaningless noise in the effective configuration.
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[HDFSBackedStateStoreProvider].getName) {
      val observed = runRealTimeQueryAndReadConfs(Seq(changelogKey))
      assert(observed(changelogKey).isEmpty,
        s"$changelogKey should be left unset for a non-RocksDB provider, got " +
          observed(changelogKey))
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

  test("an explicit changelog setting in non-canonical casing is not overridden") {
    // RocksDB resolves its configs case-insensitively (RocksDBConf.apply uses a
    // CaseInsensitiveMap), so a user setting the key in any casing has really configured it. A
    // case-sensitive `conf.contains` check would miss this and silently override them.
    val lowerCased = changelogKey.toLowerCase(java.util.Locale.ROOT)
    assert(lowerCased != changelogKey,
      "the key must have mixed case for this test to mean anything")
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      lowerCased -> "false") {
      val observed = runRealTimeQueryAndReadConfs(Seq(lowerCased, changelogKey))
      assert(observed(lowerCased).contains("false"),
        s"a lower-cased $changelogKey must be left alone, got ${observed(lowerCased)}")
      assert(observed(changelogKey).isEmpty,
        "the canonical key must not be set alongside the user's lower-cased one, got " +
          observed(changelogKey))
    }
  }
}
