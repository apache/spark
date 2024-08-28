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

import java.io.FileOutputStream
import java.nio.file.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.{CommitLog, CommitMetadata, LongOffset, Offset, SerializedOffset}

trait OffsetSuite extends SparkFunSuite {
  /** Creates test to check all the comparisons of offsets given a `one` that is less than `two`. */
  def compare(one: Offset, two: Offset): Unit = {
    test(s"comparison $one <=> $two") {
      assert(one == one)
      assert(two == two)
      assert(one != two)
      assert(two != one)
    }
  }
}

class CommitLogV2Suite extends OffsetSuite {
  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
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
    ).toAbsolutePath
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
      "testCommitLogV2"
    ).toAbsolutePath
  }

  def testSerde(commitMetadata: CommitMetadata, path: Path): Unit = {
    if (regenerateGoldenFiles) {
      val commitLog = new CommitLog(spark, path.toString)
      //      val  = testCommitLogV2FilePath.getFileSystem(spark.sessionState.newHadoopConf())
      val outputStream = new FileOutputStream(path)
      commitLog.serialize(commitMetadata, outputStream)
    } else {
      val commitLog = new CommitLog(spark, path.toString)
      val metadata = commitLog.deserialize(path)
      assert(metadata === commitMetadata)
    }
  }

  test("Basic Commit Log V2 SerDe") {
    val testStateUniqueIds: Map[String, Map[String, Map[String, Seq[String]]]] =
      Map(
        "0" -> Map("0" -> Map("default" -> Seq("unique_id1", "unique_id2", "unique_id3")),
          "1" -> Map("default" -> Seq("unique_id4", "unique_id5", "unique_id6"))),
        "1" -> Map("0" -> Map("default" -> Seq("unique_id7", "unique_id8", "unique_id9")),
          "1" -> Map("default" -> Seq("unique_id10", "unique_id11", "unique_id12"))),
      )

    val testMetadataV2 = CommitMetadata(0, testStateUniqueIds)

    testSerde(testMetadataV2, testCommitLogV2FilePath)

  }

  test("Basic Commit Log V1 SerDe") {
    val testMetadataV1 = CommitMetadata(0)
    testSerde(testMetadataV1, testCommitLogV2FilePath)
  }

  // TODO: a commit log created under V1 (withou this change) but read using V2
  //       basically put an actual json string
}


