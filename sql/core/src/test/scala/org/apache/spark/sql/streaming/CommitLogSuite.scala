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

import java.io.{ByteArrayInputStream, FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path

import scala.io.{Source => IOSource}

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CommitLog, CommitMetadata, HDFSMetadataLog}
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
      assert(metadata.stateUniqueIds.size == commitMetadata.stateUniqueIds.size)
      commitMetadata.stateUniqueIds.foreach { case (operatorId, uniqueIds) =>
        assert(metadata.stateUniqueIds.contains(operatorId))
        assert(metadata.stateUniqueIds(operatorId).length == uniqueIds.length)
        assert(metadata.stateUniqueIds(operatorId).zip(uniqueIds).forall {
          case (a, b) => a.sameElements(b)
        })
      }
    }
  }

  test("Basic Commit Log V1 SerDe") {
    val testMetadataV1 = CommitMetadata(1)
    testSerde(testMetadataV1, testCommitLogV1FilePath)
  }

  test("Basic Commit Log V2 SerDe") {
    val testStateUniqueIds: Map[Long, Array[Array[String]]] =
      Map(
        0L -> Array(Array("unique_id1", "unique_id2"), Array("unique_id3", "unique_id4")),
          1L -> Array(Array("unique_id5", "unique_id6"), Array("unique_id7", "unique_id8"))
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

  // Test an old version of Spark can ser-de the new version of commit log,
  // but running under V1 (i.e. no stateUniqueIds)
  test("v1 Serde backward compatibility") {
    // This is the json created by a V1 commit log
    val commitLogV1WithStateUniqueId = """v1
                        |{"nextBatchWatermarkMs":1,"stateUniqueIds":{}}""".stripMargin
    val inputStream: ByteArrayInputStream =
      new ByteArrayInputStream(commitLogV1WithStateUniqueId.getBytes("UTF-8"))
    val commitMetadata: CommitMetadataLegacy = new CommitLogLegacy(
      spark, testCommitLogV1FilePath.toString).deserialize(inputStream)
    assert(commitMetadata.nextBatchWatermarkMs === 1)
  }
}

// DO-NOT-MODIFY-THE-CODE-BELOW
// Below are the legacy commit log code carbon copied from Spark branch-3.5, except
// adding a "Legacy" to the class names.
case class CommitMetadataLegacy(nextBatchWatermarkMs: Long = 0) {
  def json: String = Serialization.write(this)(CommitMetadataLegacy.format)
}

object CommitMetadataLegacy {
  implicit val format: Formats = Serialization.formats(NoTypeHints)

  def apply(json: String): CommitMetadataLegacy = Serialization.read[CommitMetadataLegacy](json)
}

class CommitLogLegacy(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[CommitMetadataLegacy](sparkSession, path) {

  private val VERSION = 1
  private val EMPTY_JSON = "{}"

  override def deserialize(in: InputStream): CommitMetadataLegacy = {
    // called inside a try-finally where the underlying stream is closed in the caller
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file in the offset commit log")
    }
    validateVersion(lines.next().trim, VERSION)
    val metadataJson = if (lines.hasNext) lines.next() else EMPTY_JSON
    CommitMetadataLegacy(metadataJson)
  }

  override def serialize(metadata: CommitMetadataLegacy, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(s"v${VERSION}".getBytes(UTF_8))
    out.write('\n')

    // write metadata
    out.write(metadata.json.getBytes(UTF_8))
  }
}
