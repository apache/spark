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

package org.apache.spark.sql.execution.streaming

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class FileStreamSinkLogSuite extends SparkFunSuite with SharedSQLContext {

  import FileStreamSinkLog._

  test("getBatchIdFromFileName") {
    assert(1234L === getBatchIdFromFileName("1234"))
    assert(1234L === getBatchIdFromFileName("1234.compact"))
    intercept[NumberFormatException] {
      FileStreamSinkLog.getBatchIdFromFileName("1234a")
    }
  }

  test("isCompactionBatch") {
    assert(false === isCompactionBatch(0, compactInterval = 3))
    assert(false === isCompactionBatch(1, compactInterval = 3))
    assert(true === isCompactionBatch(2, compactInterval = 3))
    assert(false === isCompactionBatch(3, compactInterval = 3))
    assert(false === isCompactionBatch(4, compactInterval = 3))
    assert(true === isCompactionBatch(5, compactInterval = 3))
  }

  test("nextCompactionBatchId") {
    assert(2 === nextCompactionBatchId(0, compactInterval = 3))
    assert(2 === nextCompactionBatchId(1, compactInterval = 3))
    assert(5 === nextCompactionBatchId(2, compactInterval = 3))
    assert(5 === nextCompactionBatchId(3, compactInterval = 3))
    assert(5 === nextCompactionBatchId(4, compactInterval = 3))
    assert(8 === nextCompactionBatchId(5, compactInterval = 3))
  }

  test("getValidBatchesBeforeCompactionBatch") {
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(0, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(1, compactInterval = 3)
    }
    assert(Seq(0, 1) === getValidBatchesBeforeCompactionBatch(2, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(3, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(4, compactInterval = 3)
    }
    assert(Seq(2, 3, 4) === getValidBatchesBeforeCompactionBatch(5, compactInterval = 3))
  }

  test("getAllValidBatches") {
    assert(Seq(0) === getAllValidBatches(0, compactInterval = 3))
    assert(Seq(0, 1) === getAllValidBatches(1, compactInterval = 3))
    assert(Seq(2) === getAllValidBatches(2, compactInterval = 3))
    assert(Seq(2, 3) === getAllValidBatches(3, compactInterval = 3))
    assert(Seq(2, 3, 4) === getAllValidBatches(4, compactInterval = 3))
    assert(Seq(5) === getAllValidBatches(5, compactInterval = 3))
    assert(Seq(5, 6) === getAllValidBatches(6, compactInterval = 3))
    assert(Seq(5, 6, 7) === getAllValidBatches(7, compactInterval = 3))
    assert(Seq(8) === getAllValidBatches(8, compactInterval = 3))
  }

  test("compactLogs") {
    val logs = Seq(
      SinkFileStatus("/a/b/x", 100L, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/y", 200L, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/z", 300L, FileStreamSinkLog.ADD_ACTION))
    assert(logs === compactLogs(logs))

    val logs2 = Seq(
      SinkFileStatus("/a/b/m", 100L, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/n", 200L, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/z", 300L, FileStreamSinkLog.DELETE_ACTION))
    assert(logs.dropRight(1) ++ logs2.dropRight(1) === compactLogs(logs ++ logs2))
  }

  test("serialize") {
    withFileStreamSinkLog { sinkLog =>
      val logs = Seq(
        SinkFileStatus("/a/b/x", 100L, FileStreamSinkLog.ADD_ACTION),
        SinkFileStatus("/a/b/y", 200L, FileStreamSinkLog.DELETE_ACTION),
        SinkFileStatus("/a/b/z", 300L, FileStreamSinkLog.ADD_ACTION))

      val expected = s"""${FileStreamSinkLog.VERSION}
          |{"path":"/a/b/x","size":100,"action":"add"}
          |{"path":"/a/b/y","size":200,"action":"delete"}
          |{"path":"/a/b/z","size":300,"action":"add"}""".stripMargin
      assert(expected === new String(sinkLog.serialize(logs), UTF_8))

      assert(FileStreamSinkLog.VERSION === new String(sinkLog.serialize(Nil), UTF_8))
    }
  }

  test("deserialize") {
    withFileStreamSinkLog { sinkLog =>
      val logs = s"""${FileStreamSinkLog.VERSION}
          |{"path":"/a/b/x","size":100,"action":"add"}
          |{"path":"/a/b/y","size":200,"action":"delete"}
          |{"path":"/a/b/z","size":300,"action":"add"}""".stripMargin

      val expected = Seq(
        SinkFileStatus("/a/b/x", 100L, FileStreamSinkLog.ADD_ACTION),
        SinkFileStatus("/a/b/y", 200L, FileStreamSinkLog.DELETE_ACTION),
        SinkFileStatus("/a/b/z", 300L, FileStreamSinkLog.ADD_ACTION))

      assert(expected === sinkLog.deserialize(logs.getBytes(UTF_8)))

      assert(Nil === sinkLog.deserialize(FileStreamSinkLog.VERSION.getBytes(UTF_8)))
    }
  }

  test("batchIdToPath") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
      withFileStreamSinkLog { sinkLog =>
        assert("0" === sinkLog.batchIdToPath(0).getName)
        assert("1" === sinkLog.batchIdToPath(1).getName)
        assert("2.compact" === sinkLog.batchIdToPath(2).getName)
        assert("3" === sinkLog.batchIdToPath(3).getName)
        assert("4" === sinkLog.batchIdToPath(4).getName)
        assert("5.compact" === sinkLog.batchIdToPath(5).getName)
      }
    }
  }

  test("compact") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
      withFileStreamSinkLog { sinkLog =>
        for (batchId <- 0 to 10) {
          sinkLog.add(
            batchId,
            Seq(SinkFileStatus("/a/b/" + batchId, 100L, FileStreamSinkLog.ADD_ACTION)))
          val expectedFiles = (0 to batchId).map {
            id => SinkFileStatus("/a/b/" + id, 100L, FileStreamSinkLog.ADD_ACTION)
          }
          assert(sinkLog.allFiles() === expectedFiles)
          if (isCompactionBatch(batchId, 3)) {
            // Since batchId is a compaction batch, the batch log file should contain all logs
            assert(sinkLog.get(batchId).getOrElse(Nil) === expectedFiles)
          }
        }
      }
    }
  }

  test("delete expired file") {
    // Set FILE_SINK_LOG_CLEANUP_DELAY to 0 so that we can detect the deleting behaviour
    // deterministically
    withSQLConf(
      SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3",
      SQLConf.FILE_SINK_LOG_CLEANUP_DELAY.key -> "0") {
      withFileStreamSinkLog { sinkLog =>
        val metadataPath = new File(sinkLog.metadataPath.toUri.toString)

        def listBatchFiles(): Set[String] = {
          metadataPath.listFiles().map(_.getName).filter { fileName =>
            try {
              getBatchIdFromFileName(fileName)
              true
            } catch {
              case _: NumberFormatException => false
            }
          }.toSet
        }

        sinkLog.add(0, Seq(SinkFileStatus("/a/b/0", 100L, FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0") === listBatchFiles())
        sinkLog.add(1, Seq(SinkFileStatus("/a/b/1", 100L, FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0", "1") === listBatchFiles())
        sinkLog.add(2, Seq(SinkFileStatus("/a/b/2", 100L, FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact") === listBatchFiles())
        sinkLog.add(3, Seq(SinkFileStatus("/a/b/3", 100L, FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3") === listBatchFiles())
        sinkLog.add(4, Seq(SinkFileStatus("/a/b/4", 100L, FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4") === listBatchFiles())
        sinkLog.add(5, Seq(SinkFileStatus("/a/b/5", 100L, FileStreamSinkLog.ADD_ACTION)))
        assert(Set("5.compact") === listBatchFiles())
      }
    }
  }

  def withFileStreamSinkLog(f: FileStreamSinkLog => Unit): Unit = {
    withTempDir { file =>
      val sinkLog = new FileStreamSinkLog(sqlContext, file.getCanonicalPath)
      f(sinkLog)
    }
  }
}
