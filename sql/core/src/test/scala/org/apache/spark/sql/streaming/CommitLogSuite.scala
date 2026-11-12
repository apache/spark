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

import java.io.{ByteArrayInputStream, FileInputStream, FileOutputStream}
import java.nio.file.Path

import org.apache.spark.sql.execution.streaming.checkpointing.{CheckpointVersionManager, CommitLog, CommitLogType, CommitMetadata, CommitMetadataBase, CommitMetadataV2, CommitMetadataV3, OffsetSeqLog, SinkMetadataInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CommitLogSuite extends SharedSparkSession {

  private def testCommitLogV2FilePath: Path = {
    getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "resources",
      "structured-streaming",
      "testCommitLogV2"
    )
  }

  private def testCommitLogV2FilePathEmptyUniqueId: Path = {
    getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "resources",
      "structured-streaming",
      "testCommitLogV2-empty-unique-id"
    )
  }

  private def testCommitLogV1FilePath: Path = {
    getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "resources",
      "structured-streaming",
      "testCommitLogV1"
    )
  }

  private def testSerde(commitMetadata: CommitMetadataBase, path: Path): Unit = {
    if (regenerateGoldenFiles) {
      val commitLog = new CommitLog(spark, path.toString)
      val outputStream = new FileOutputStream(path.resolve("testCommitLog").toFile)
      commitLog.serialize(commitMetadata, outputStream)
    } else {
      val commitLog = new CommitLog(spark, path.toString)
      val inputStream = new FileInputStream(path.resolve("testCommitLog").toFile)
      val metadata = commitLog.deserialize(inputStream)
      // Array comparison are reference based, so we need to compare the elements
      assert(metadata.nextBatchWatermarkMs == commitMetadata.nextBatchWatermarkMs)
      if (metadata.stateUniqueIds.isEmpty) {
        assert(commitMetadata.stateUniqueIds.isEmpty)
      } else {
        assert(metadata.stateUniqueIds.get.size == commitMetadata.stateUniqueIds.get.size)
        commitMetadata.stateUniqueIds.get.foreach { case (operatorId, uniqueIds) =>
          assert(metadata.stateUniqueIds.get.contains(operatorId))
          assert(metadata.stateUniqueIds.get(operatorId).length == uniqueIds.length)
          assert(metadata.stateUniqueIds.get(operatorId).zip(uniqueIds).forall {
            case (a, b) => a.sameElements(b)
          })
        }
      }
    }
  }

  test("Basic Commit Log V1 SerDe") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
      val testMetadataV1 = CommitMetadata(1)
      testSerde(testMetadataV1, testCommitLogV1FilePath)
    }
  }

  test("Basic Commit Log V2 SerDe - nonempty stateUniqueIds") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      val testStateUniqueIds: Map[Long, Array[Array[String]]] =
        Map(
          0L -> Array(Array("unique_id1", "unique_id2"), Array("unique_id3", "unique_id4")),
            1L -> Array(Array("unique_id5", "unique_id6"), Array("unique_id7", "unique_id8"))
        )
      val testMetadataV2 = CommitMetadataV2(0, Some(testStateUniqueIds))
      testSerde(testMetadataV2, testCommitLogV2FilePath)
    }
  }

  test("Basic Commit Log V2 SerDe - empty stateUniqueIds") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      val testMetadataV2 = CommitMetadataV2(0, Some(Map[Long, Array[Array[String]]]()))
      testSerde(testMetadataV2, testCommitLogV2FilePathEmptyUniqueId)
    }
  }

  test("Basic Commit Log V3 SerDe - single active sink") {
    withTempDir { tempDir =>
      val commitLog = new CommitLog(spark, tempDir.getAbsolutePath)
      val sinkInfo = SinkMetadataInfo(
        sinkName = "sink-0",
        commitOffset = OffsetSeqLog.SERIALIZED_VOID_OFFSET,
        providerName = "memory",
        apiVersion = "v2",
        isActive = true)
      val metadata = commitLog.createMetadata(
        nextBatchWatermarkMs = 42,
        sinkMetadataMap = Map("sink-0" -> sinkInfo),
        commitLogFormatVersion = CommitLog.VERSION_3)
      assert(commitLog.add(0, metadata))

      val read = commitLog.get(0).get
      assert(read.version === CommitLog.VERSION_3)
      assert(read.nextBatchWatermarkMs === 42)
      val readV3 = read.asInstanceOf[CommitMetadataV3]
      assert(readV3.sinkMetadataMap === Map("sink-0" -> sinkInfo))
      assert(readV3.activeSinkMetadataInfo === sinkInfo)
    }
  }

  test("Commit Log V3 - retains historical sinks alongside active") {
    withTempDir { tempDir =>
      val commitLog = new CommitLog(spark, tempDir.getAbsolutePath)
      val historical = SinkMetadataInfo(
        sinkName = "sink-0",
        commitOffset = """{"offset":3}""",
        providerName = "memory",
        apiVersion = "v2",
        isActive = false)
      val active = SinkMetadataInfo(
        sinkName = "sink-1",
        commitOffset = """{"offset":7}""",
        providerName = "memory",
        apiVersion = "v2",
        isActive = true)
      val metadata = commitLog.createMetadata(
        nextBatchWatermarkMs = 100,
        sinkMetadataMap = Map("sink-0" -> historical, "sink-1" -> active),
        commitLogFormatVersion = CommitLog.VERSION_3)
      assert(commitLog.add(0, metadata))

      val readV3 = commitLog.get(0).get.asInstanceOf[CommitMetadataV3]
      assert(readV3.activeSinkMetadataInfo === active)
      assert(readV3.sinkMetadataMap("sink-0") === historical)
      assert(readV3.sinkMetadataMap("sink-1") === active)
    }
  }

  test("createMetadata for V3 requires non-empty sinkMetadataMap") {
    withTempDir { tempDir =>
      val commitLog = new CommitLog(spark, tempDir.getAbsolutePath)
      intercept[IllegalArgumentException] {
        commitLog.createMetadata(
          nextBatchWatermarkMs = 0,
          sinkMetadataMap = Map.empty,
          commitLogFormatVersion = CommitLog.VERSION_3)
      }
    }
  }

  test("CommitMetadataV3 requires exactly one active sink") {
    val historical = SinkMetadataInfo(
      sinkName = "sink-0",
      commitOffset = OffsetSeqLog.SERIALIZED_VOID_OFFSET,
      providerName = "memory",
      apiVersion = "v2",
      isActive = false)
    val active = SinkMetadataInfo(
      sinkName = "sink-1",
      commitOffset = OffsetSeqLog.SERIALIZED_VOID_OFFSET,
      providerName = "memory",
      apiVersion = "v2",
      isActive = true)

    // No active sink.
    intercept[IllegalArgumentException] {
      CommitMetadataV3(sinkMetadataMap = Map("sink-0" -> historical))
    }
    // More than one active sink.
    intercept[IllegalArgumentException] {
      CommitMetadataV3(sinkMetadataMap =
        Map("sink-0" -> active.copy(sinkName = "sink-0"), "sink-1" -> active))
    }
  }

  // SPARK-50653: When the configured commit log version is V2, a V1 file on disk should still
  // deserialize successfully into a V1 [[CommitMetadata]] because the wire format version is now
  // discovered from the file header rather than enforced to match the conf.
  test("Cross-version V1 SerDe") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      val commitlogV1 = """v1
                          |{"nextBatchWatermarkMs":233}""".stripMargin
      val inputStream: ByteArrayInputStream =
        new ByteArrayInputStream(commitlogV1.getBytes("UTF-8"))

      val commitMetadata = new CommitLog(
        spark, testCommitLogV1FilePath.toString).deserialize(inputStream)
      assert(commitMetadata.version === CommitLog.VERSION_1)
      assert(commitMetadata.nextBatchWatermarkMs === 233)
      assert(commitMetadata.stateUniqueIds.isEmpty)
    }
  }

  test("SPARK-56970: creating a V1 commit with stateUniqueIds should fail") {
    withTempDir { tmpDir =>
      val commitLog = new CommitLog(spark, tmpDir.getCanonicalPath)
      val stateUniqueIds: Map[Long, Array[Array[String]]] =
        Map(0L -> Array(Array("unique_id1", "unique_id2")))

      // Through the createMetadata factory with an explicit V1 format version.
      val e1 = intercept[IllegalArgumentException] {
        commitLog.createMetadata(
          nextBatchWatermarkMs = 1,
          stateUniqueIds = Some(stateUniqueIds),
          commitLogFormatVersion = CommitLog.VERSION_1)
      }
      assert(e1.getMessage.contains("stateUniqueIds cannot be set"))

      // Directly through withStateUniqueIds on a V1 metadata.
      val e2 = intercept[IllegalArgumentException] {
        CommitMetadata(1).withStateUniqueIds(Some(stateUniqueIds))
      }
      assert(e2.getMessage.contains("stateUniqueIds cannot be set"))

      // None and an empty map are allowed for V1 (no unique ids to persist).
      assert(CommitMetadata(1).withStateUniqueIds(None).stateUniqueIds.isEmpty)
      assert(commitLog.createMetadata(
        nextBatchWatermarkMs = 1,
        stateUniqueIds = Some(Map.empty[Long, Array[Array[String]]]),
        commitLogFormatVersion = CommitLog.VERSION_1).version === CommitLog.VERSION_1)
    }
  }

  /** The commit log version the session config asks for, via the public resolution entry point. */
  private def sessionCommitLogVersion(): Int = {
    CheckpointVersionManager.resolveCommitLogVersion(spark, latestCommittedBatch = None)
  }

  test("commit log version derives from the state store checkpoint format") {
    // Nothing set: defaults to VERSION_1.
    assert(sessionCommitLogVersion() === CommitLog.VERSION_1)

    // State store checkpoint format v2 makes each batch write stateUniqueIds, which only a commit
    // log at VERSION_2 or above can persist, so it raises the commit log version to v2.
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      assert(sessionCommitLogVersion() === CommitLog.VERSION_2,
        "state store v2 must raise the commit log to v2")
    }

    // The resolved version is capped at VERSION_2: VERSION_3 exists only to carry sink-evolution
    // metadata and is written exclusively by the sink-evolution path, never derived from a config.
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "3") {
      assert(sessionCommitLogVersion() === CommitLog.VERSION_2,
        "a config-derived commit log version must never resolve to v3")
    }
  }

  test("an existing checkpoint's commit log version wins over the session config") {
    // The whole point of resolution: a commit log created at one version keeps being written at
    // that version, so a higher state store checkpoint format cannot start writing a format the
    // checkpoint lacks.
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      val existing: CommitMetadataBase = CommitMetadata(nextBatchWatermarkMs = 0)
      assert(existing.version === CommitLog.VERSION_1)
      val resolved =
        CheckpointVersionManager.resolveCommitLogVersion(spark, Some((7L, existing)))
      assert(resolved === CommitLog.VERSION_1,
        s"an existing V1 commit log must stay V1 even with the state store at v2, got $resolved")
    }
  }

  test("recording a commit log version sets the implied state store format") {
    val sinkMetadataMap = Map("sink" -> SinkMetadataInfo(
      sinkName = "sink",
      commitOffset = OffsetSeqLog.SERIALIZED_VOID_OFFSET,
      providerName = "provider",
      apiVersion = "DSv2"))
    val session = spark.cloneSession()
    val v3WithStateIds = CommitMetadataV3(0, Some(Map.empty), sinkMetadataMap)
    CheckpointVersionManager.setFormatVersion(
      session, CommitLogType, CommitLog.VERSION_3, Some(v3WithStateIds))
    assert(session.conf.get(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key) === "2",
      "state checkpoint ids in a V3 commit imply state store format v2")

    val v3WithoutStateIdsSession = spark.cloneSession()
    val v3WithoutStateIds = CommitMetadataV3(0, None, sinkMetadataMap)
    CheckpointVersionManager.setFormatVersion(
      v3WithoutStateIdsSession, CommitLogType, CommitLog.VERSION_3, Some(v3WithoutStateIds))
    assert(v3WithoutStateIdsSession.conf.get(
      SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key) === "1",
      "a V3 commit without state checkpoint ids must preserve state store format v1")

    val v1Session = spark.cloneSession()
    CheckpointVersionManager.setFormatVersion(v1Session, CommitLogType, CommitLog.VERSION_1)
    assert(v1Session.conf.get(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key) === "1",
      "a V1 commit log cannot carry state store checkpoint ids, so the state store must be v1")
  }

}
