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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.{CommitLog, CommitMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CommitLogSuite extends SparkFunSuite with SharedSparkSession {

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

  private def testSerde(commitMetadata: CommitMetadata, path: Path): Unit = {
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
      val testMetadataV2 = CommitMetadata(0, Some(testStateUniqueIds))
      testSerde(testMetadataV2, testCommitLogV2FilePath)
    }
  }

  test("Basic Commit Log V2 SerDe - empty stateUniqueIds") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      val testMetadataV2 = CommitMetadata(0, Some(Map[Long, Array[Array[String]]]()))
      testSerde(testMetadataV2, testCommitLogV2FilePathEmptyUniqueId)
    }
  }

  // Old metadata structure with no state unique ids should not affect the deserialization
  test("Cross-version V1 SerDe") {
    withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      val commitlogV1 = """v1
                          |{"nextBatchWatermarkMs":233}""".stripMargin
      val inputStream: ByteArrayInputStream =
        new ByteArrayInputStream(commitlogV1.getBytes("UTF-8"))

      // TODO [SPARK-50653]: Uncomment the below when v2 -> v1 backward compatibility is added
      // val commitMetadata: CommitMetadata = new CommitLog(
      // spark, testCommitLogV1FilePath.toString).deserialize(inputStream)
      // assert(commitMetadata.nextBatchWatermarkMs === 233)
      // assert(commitMetadata.stateUniqueIds === Map.empty)

      // TODO [SPARK-50653]: remove the below when v2 -> v1 backward compatibility is added
      val e = intercept[IllegalStateException] {
        new CommitLog(spark, testCommitLogV1FilePath.toString).deserialize(inputStream)
      }

      assert (e.getMessage.contains("only supported log version"))
    }
  }
}
