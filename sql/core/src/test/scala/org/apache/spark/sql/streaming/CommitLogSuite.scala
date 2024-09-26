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
import org.apache.spark.sql.test.SharedSparkSession

class CommitLogSuite extends SparkFunSuite with SharedSparkSession {

//  import testImplicits._

  private def testCommitLogV2FilePath: Path = {
    getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "scala",
      "org",
      "apache",
      "spark",
      "sql",
      "streaming",
      "resources",
      "testCommitLogV2"
    )
  }

  private def testCommitLogV1FilePath: Path = {
    getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "scala",
      "org",
      "apache",
      "spark",
      "sql",
      "streaming",
      "resources",
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
      assert(metadata.stateUniqueIds.size == commitMetadata.stateUniqueIds.size)
      commitMetadata.stateUniqueIds.foreach { case (operatorId, uniqueIds) =>
        assert(metadata.stateUniqueIds.contains(operatorId))
        assert(metadata.stateUniqueIds(operatorId).sameElements(uniqueIds))
      }
    }
  }

  test("Basic Commit Log V1 SerDe") {
    val testMetadataV1 = CommitMetadata(1)
    testSerde(testMetadataV1, testCommitLogV1FilePath)
  }

  test("Basic Commit Log V2 SerDe") {
    val testStateUniqueIds: Map[Long, Array[String]] =
      Map(
        0L -> Array("unique_id1", "unique_id2", "unique_id3"),
          1L -> Array("unique_id4", "unique_id5", "unique_id6")
      )
    val testMetadataV2 = CommitMetadata(0, testStateUniqueIds)
    testSerde(testMetadataV2, testCommitLogV2FilePath)
  }

  // Old metadata structure with no state unique ids should not affect the deserialization
  test("Cross-version V1 SerDe") {
    val commitlogV1 = """v1
                        |{"nextBatchWatermarkMs":233}""".stripMargin
    val inputStream: ByteArrayInputStream =
      new ByteArrayInputStream(commitlogV1.getBytes("UTF-8"))
    val commitMetadata: CommitMetadata = new CommitLog(
      spark, testCommitLogV1FilePath.toString).deserialize(inputStream)
    assert(commitMetadata.nextBatchWatermarkMs === 233)
    assert(commitMetadata.stateUniqueIds === Map.empty)
  }
}


