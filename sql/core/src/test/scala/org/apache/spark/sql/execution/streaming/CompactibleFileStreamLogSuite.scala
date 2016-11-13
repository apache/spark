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

import java.io._
import java.nio.charset.StandardCharsets._

import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.streaming.FakeFileSystem._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSQLContext

class CompactibleFileStreamLogSuite extends SparkFunSuite with SharedSQLContext {

  /** To avoid caching of FS objects */
  override protected val sparkConf =
    new SparkConf().set(s"spark.hadoop.fs.$scheme.impl.disable.cache", "true")

  import CompactibleFileStreamLog._

  testWithUninterruptibleThread(
    "correct results from multiples runs with different compact intervals") {
    withTempDir { dir =>
      def newFakeCompactibleFileStreamLog(compactInterval: Int): FakeCompactibleFileStreamLog =
        new FakeCompactibleFileStreamLog(
          _fileCleanupDelayMs = Long.MaxValue,
          _compactInterval = compactInterval,
          spark,
          dir.getCanonicalPath)

      var compactibleLog = newFakeCompactibleFileStreamLog(2)
      assert(compactibleLog.knownCompactionBatches === Array())
      assert(compactibleLog.zeroBatch === 0)
      assert(compactibleLog.allFiles() === Array())

      compactibleLog.add(0, Array("entry_0"))
      compactibleLog.add(1, Array("entry_1")) // should compact

      compactibleLog = newFakeCompactibleFileStreamLog(2)
      assert(compactibleLog.knownCompactionBatches === Array(1))
      assert(compactibleLog.zeroBatch === 2)
      assert(compactibleLog.allFiles() === (0 to 1).map(idx => s"entry_$idx"))

      compactibleLog.add(2, Array("entry_2"))
      compactibleLog.add(3, Array("entry_3")) // should compact

      compactibleLog = newFakeCompactibleFileStreamLog(3)
      assert(compactibleLog.knownCompactionBatches === Array(1, 3))
      assert(compactibleLog.zeroBatch === 4)
      assert(compactibleLog.allFiles() === (0 to 3).map(idx => s"entry_$idx"))

      compactibleLog.add(4, Array("entry_4"))
      compactibleLog.add(5, Array("entry_5"))
      compactibleLog.add(6, Array("entry_6")) // should compact

      compactibleLog = newFakeCompactibleFileStreamLog(5)
      assert(compactibleLog.knownCompactionBatches === Array(1, 3, 6))
      assert(compactibleLog.zeroBatch === 7)
      assert(compactibleLog.allFiles() === (0 to 6).map(idx => s"entry_$idx"))

      compactibleLog.add(7, Array("entry_7"))
      compactibleLog.add(8, Array("entry_8"))
      compactibleLog.add(9, Array("entry_9"))
      compactibleLog.add(10, Array("entry_10"))

      compactibleLog = newFakeCompactibleFileStreamLog(2)
      assert(compactibleLog.knownCompactionBatches === Array(1, 3, 6))
      assert(compactibleLog.zeroBatch === 11)
      assert(compactibleLog.allFiles() === (0 to 10).map(idx => s"entry_$idx"))
    }
  }

  private val emptyKnownCompactionBatches = Array[Long]()
  private val knownCompactionBatches = Array[Long](
    1, 3, // produced with interval = 2
    6     // produced with interval = 3
  )

  /** -- testing of `object CompactibleFileStreamLog` begins -- */

  test("getBatchIdFromFileName") {
    assert(1234L === getBatchIdFromFileName("1234"))
    assert(1234L === getBatchIdFromFileName("1234.compact"))
    intercept[NumberFormatException] {
      getBatchIdFromFileName("1234a")
    }
  }

  test("isCompactionBatchFromFileName") {
    assert(false === isCompactionBatchFromFileName("1234"))
    assert(true === isCompactionBatchFromFileName("1234.compact"))
  }

  test("isCompactionBatch") {
    // test empty knownCompactionBatches cases
    assert(false === isCompactionBatch(
      emptyKnownCompactionBatches, zeroBatch = 0, batchId = 0, compactInterval = 3))
    assert(false === isCompactionBatch(
      emptyKnownCompactionBatches, zeroBatch = 0, batchId = 1, compactInterval = 3))
    assert(true === isCompactionBatch(
      emptyKnownCompactionBatches, zeroBatch = 0, batchId = 2, compactInterval = 3))
    assert(false === isCompactionBatch(
      emptyKnownCompactionBatches, zeroBatch = 0, batchId = 3, compactInterval = 3))
    assert(false === isCompactionBatch(
      emptyKnownCompactionBatches, zeroBatch = 0, batchId = 4, compactInterval = 3))
    assert(true === isCompactionBatch(
      emptyKnownCompactionBatches, zeroBatch = 0, batchId = 5, compactInterval = 3))

    // test non-empty knownCompactionBatches cases
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 0, compactInterval = 3))
    assert(true === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 1, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 2, compactInterval = 3))
    assert(true === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 3, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 4, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 5, compactInterval = 3))
    assert(true === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 6, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 7, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 8, compactInterval = 3))
    assert(true === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 9, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 7, batchId = 10, compactInterval = 3))

    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 0, compactInterval = 3))
    assert(true === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 1, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 2, compactInterval = 3))
    assert(true === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 3, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 4, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 5, compactInterval = 3))
    assert(true === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 6, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 7, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 8, compactInterval = 3))
    // notice the following one, it should be false !!!
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 9, compactInterval = 3))
    for (batchId <- 10 until 20) {
      assert(false === isCompactionBatch(
        knownCompactionBatches, zeroBatch = 20, batchId = batchId, compactInterval = 3))
    }
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 20, compactInterval = 3))
    assert(false === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 21, compactInterval = 3))
    assert(true === isCompactionBatch(
      knownCompactionBatches, zeroBatch = 20, batchId = 22, compactInterval = 3))
  }

  test("nextCompactionBatchId") {
    assert(2 === nextCompactionBatchId(zeroBatch = 0, batchId = 0, compactInterval = 3))
    assert(2 === nextCompactionBatchId(zeroBatch = 0, batchId = 1, compactInterval = 3))
    assert(5 === nextCompactionBatchId(zeroBatch = 0, batchId = 2, compactInterval = 3))
    assert(5 === nextCompactionBatchId(zeroBatch = 0, batchId = 3, compactInterval = 3))
    assert(5 === nextCompactionBatchId(zeroBatch = 0, batchId = 4, compactInterval = 3))
    assert(8 === nextCompactionBatchId(zeroBatch = 0, batchId = 5, compactInterval = 3))
    assert(8 === nextCompactionBatchId(zeroBatch = 0, batchId = 6, compactInterval = 3))
    assert(9 === nextCompactionBatchId(zeroBatch = 7, batchId = 7, compactInterval = 3))
    assert(9 === nextCompactionBatchId(zeroBatch = 7, batchId = 8, compactInterval = 3))
    assert(12 === nextCompactionBatchId(zeroBatch = 7, batchId = 9, compactInterval = 3))
    assert(12 === nextCompactionBatchId(zeroBatch = 7, batchId = 10, compactInterval = 3))
    assert(12 === nextCompactionBatchId(zeroBatch = 7, batchId = 11, compactInterval = 3))
  }

  test("getValidBatchesBeforeCompactionBatch") {
    // test empty knownCompactionBatches cases
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        emptyKnownCompactionBatches, zeroBatch = 0, compactionBatchId = 0, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        emptyKnownCompactionBatches, zeroBatch = 0, compactionBatchId = 1, compactInterval = 3)
    }
    assert(Seq(0, 1) ===
      getValidBatchesBeforeCompactionBatch(
        emptyKnownCompactionBatches, zeroBatch = 0, compactionBatchId = 2, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        emptyKnownCompactionBatches, zeroBatch = 0, compactionBatchId = 3, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        emptyKnownCompactionBatches, zeroBatch = 0, compactionBatchId = 4, compactInterval = 3)
    }
    assert(Seq(2, 3, 4) ===
      getValidBatchesBeforeCompactionBatch(
        emptyKnownCompactionBatches, zeroBatch = 0, compactionBatchId = 5, compactInterval = 3))

    // test non-empty knownCompactionBatches cases
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 7, compactionBatchId = 7, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 7, compactionBatchId = 8, compactInterval = 3)
    }
    assert(Seq(6, 7, 8) ===
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 7, compactionBatchId = 9, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 7, compactionBatchId = 10, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 7, compactionBatchId = 11, compactInterval = 3)
    }
    assert(Seq(9, 10, 11) ===
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 7, compactionBatchId = 12, compactInterval = 3))

    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 20, compactionBatchId = 20, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 20, compactionBatchId = 21, compactInterval = 3)
    }
    assert((6 to 21) ===
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 20, compactionBatchId = 22, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 20, compactionBatchId = 23, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 20, compactionBatchId = 24, compactInterval = 3)
    }
    assert(Seq(22, 23, 24) ===
      getValidBatchesBeforeCompactionBatch(
        knownCompactionBatches, zeroBatch = 20, compactionBatchId = 25, compactInterval = 3))
  }

  test("getAllValidBatches") {
    // test empty knownCompactionBatches cases
    assert(
      Seq(0) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 0, compactInterval = 3))
    assert(
      Seq(0, 1) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 1, compactInterval = 3))
    assert(
      Seq(2) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 2, compactInterval = 3))
    assert(
      Seq(2, 3) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 3, compactInterval = 3))
    assert(
      Seq(2, 3, 4) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 4, compactInterval = 3))
    assert(
      Seq(5) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 5, compactInterval = 3))
    assert(
      Seq(5, 6) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 6, compactInterval = 3))
    assert(
      Seq(5, 6, 7) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 7, compactInterval = 3))
    assert(
      Seq(8) === getAllValidBatches(
        emptyKnownCompactionBatches, zeroBatch = 0, batchId = 8, compactInterval = 3))

    // test non-empty knownCompactionBatches cases
    assert(
      Seq(0) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 0, compactInterval = 3))
    assert(
      Seq(1) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 1, compactInterval = 3))
    assert(
      Seq(1, 2) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 2, compactInterval = 3))
    assert(
      Seq(3) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 3, compactInterval = 3))
    assert(
      Seq(3, 4) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 4, compactInterval = 3))
    assert(
      Seq(3, 4, 5) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 5, compactInterval = 3))
    assert(
      Seq(6) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 6, compactInterval = 3))
    assert(
      Seq(6, 7) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 7, compactInterval = 3))
    assert(
      Seq(6, 7, 8) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 8, compactInterval = 3))
    assert(
      Seq(9) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 9, compactInterval = 3))
    assert(
      Seq(9, 10) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 10, compactInterval = 3))
    assert(
      Seq(9, 10, 11) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 11, compactInterval = 3))
    assert(
      Seq(12) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 12, compactInterval = 3))
    assert(
      Seq(12, 13) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 13, compactInterval = 3))
    assert(
      Seq(12, 13, 14) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 14, compactInterval = 3))
    assert(
      Seq(15) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 7, batchId = 15, compactInterval = 3))

    assert(
      (6 to 20) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 20, compactInterval = 3))
    assert(
      (6 to 21) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 21, compactInterval = 3))
    assert(
      Seq(22) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 22, compactInterval = 3))
    assert(
      Seq(22, 23) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 23, compactInterval = 3))
    assert(
      Seq(22, 23, 24) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 24, compactInterval = 3))
    assert(
      Seq(25) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 25, compactInterval = 3))
    assert(
      Seq(25, 26) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 26, compactInterval = 3))
    assert(
      Seq(25, 26, 27) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 27, compactInterval = 3))
    assert(
      Seq(28) === getAllValidBatches(
        knownCompactionBatches, zeroBatch = 20, batchId = 28, compactInterval = 3))
  }

  /** -- testing of `object CompactibleFileStreamLog` ends -- */

  test("batchIdToPath") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      compactInterval = 3,
      existingFiles = Seq(),
      compactibleLog => {
        assert("0" === compactibleLog.batchIdToPath(0).getName)
        assert("1" === compactibleLog.batchIdToPath(1).getName)
        assert("2.compact" === compactibleLog.batchIdToPath(2).getName)
        assert("3" === compactibleLog.batchIdToPath(3).getName)
        assert("4" === compactibleLog.batchIdToPath(4).getName)
        assert("5.compact" === compactibleLog.batchIdToPath(5).getName)
      })

    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      compactInterval = 3,
      existingFiles = Seq(
        "0", "1.compact",
        "2", "3.compact",
        "4", "5"
      ),
      compactibleLog => {
        assert("0" === compactibleLog.batchIdToPath(0).getName)
        assert("1.compact" === compactibleLog.batchIdToPath(1).getName)
        assert("2" === compactibleLog.batchIdToPath(2).getName)
        assert("3.compact" === compactibleLog.batchIdToPath(3).getName)
        assert("4" === compactibleLog.batchIdToPath(4).getName)
        assert("5" === compactibleLog.batchIdToPath(5).getName)
        assert("6" === compactibleLog.batchIdToPath(6).getName)
        assert("7" === compactibleLog.batchIdToPath(7).getName)
        assert("8.compact" === compactibleLog.batchIdToPath(8).getName)
        assert("9" === compactibleLog.batchIdToPath(9).getName)
        assert("10" === compactibleLog.batchIdToPath(10).getName)
        assert("11.compact" === compactibleLog.batchIdToPath(11).getName)
      })
  }

  test("serialize") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      compactInterval = 3,
      existingFiles = Seq(),
      compactibleLog => {
        val logs = Array("entry_1", "entry_2", "entry_3")
        val expected = s"""${FakeCompactibleFileStreamLog.VERSION}
            |"entry_1"
            |"entry_2"
            |"entry_3"""".stripMargin
        val baos = new ByteArrayOutputStream()
        compactibleLog.serialize(logs, baos)
        assert(expected === baos.toString(UTF_8.name()))

        baos.reset()
        compactibleLog.serialize(Array(), baos)
        assert(FakeCompactibleFileStreamLog.VERSION === baos.toString(UTF_8.name()))
      })
  }

  test("deserialize") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      compactInterval = 3,
      existingFiles = Seq(),
      compactibleLog => {
        val logs = s"""${FakeCompactibleFileStreamLog.VERSION}
            |"entry_1"
            |"entry_2"
            |"entry_3"""".stripMargin
        val expected = Array("entry_1", "entry_2", "entry_3")
        assert(expected ===
          compactibleLog.deserialize(new ByteArrayInputStream(logs.getBytes(UTF_8))))

        assert(Nil ===
          compactibleLog.deserialize(
            new ByteArrayInputStream(FakeCompactibleFileStreamLog.VERSION.getBytes(UTF_8))))
      })
  }

  testWithUninterruptibleThread("compact") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      compactInterval = 3,
      existingFiles = Seq(),
      compactibleLog => {
        for (batchId <- 0 to 10) {
          compactibleLog.add(batchId, Array("some_path_" + batchId))
          val expectedFiles = (0 to batchId).map { id => "some_path_" + id }
          assert(compactibleLog.allFiles() === expectedFiles)
          if (isCompactionBatch(emptyKnownCompactionBatches, batchId, 0, 3)) {
            // Since batchId is a compaction batch, the batch log file should contain all logs
            assert(compactibleLog.get(batchId).getOrElse(Nil) === expectedFiles)
          }
        }
      })
  }

  testWithUninterruptibleThread("delete expired file") {
    // Set `fileCleanupDelayMs` to 0 so that we can detect the deleting behaviour deterministically
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = 0,
      compactInterval = 3,
      existingFiles = Seq(),
      compactibleLog => {
        val fs = compactibleLog.metadataPath.getFileSystem(spark.sessionState.newHadoopConf())

        def listBatchFiles(): Set[String] = {
          fs.listStatus(compactibleLog.metadataPath).map(_.getPath.getName).filter { fileName =>
            try {
              getBatchIdFromFileName(fileName)
              true
            } catch {
              case _: NumberFormatException => false
            }
          }.toSet
        }

        compactibleLog.add(0, Array("some_path_0"))
        assert(Set("0") === listBatchFiles())
        compactibleLog.add(1, Array("some_path_1"))
        assert(Set("0", "1") === listBatchFiles())
        compactibleLog.add(2, Array("some_path_2"))
        assert(Set("2.compact") === listBatchFiles())
        compactibleLog.add(3, Array("some_path_3"))
        assert(Set("2.compact", "3") === listBatchFiles())
        compactibleLog.add(4, Array("some_path_4"))
        assert(Set("2.compact", "3", "4") === listBatchFiles())
        compactibleLog.add(5, Array("some_path_5"))
        assert(Set("5.compact") === listBatchFiles())
      })
  }

  private def withFakeCompactibleFileStreamLog(
    fileCleanupDelayMs: Long,
    compactInterval: Int,
    existingFiles: Seq[String],
    f: FakeCompactibleFileStreamLog => Unit
  ): Unit = {
    withTempDir { dir =>
      val tmpLog = new FakeCompactibleFileStreamLog(
        fileCleanupDelayMs,
        compactInterval,
        spark,
        dir.getCanonicalPath)
      for (existingFile <- existingFiles) {
        tmpLog.serialize(Array(), new FileOutputStream(new File(dir, existingFile)))
      }
      val compactibleLog = new FakeCompactibleFileStreamLog(
        fileCleanupDelayMs,
        compactInterval,
        spark,
        dir.getCanonicalPath)
      f(compactibleLog)
    }
  }
}

object FakeCompactibleFileStreamLog {
  val VERSION = "test_version"
}

class FakeCompactibleFileStreamLog(
    _fileCleanupDelayMs: Long,
    _compactInterval: Int,
    sparkSession: SparkSession,
    path: String)
  extends CompactibleFileStreamLog[String](
    FakeCompactibleFileStreamLog.VERSION,
    sparkSession,
    path
  ) {

  override protected def fileCleanupDelayMs: Long = _fileCleanupDelayMs

  override protected def isDeletingExpiredLog: Boolean = true

  override protected def compactInterval: Int = _compactInterval

  override def compactLogs(logs: Seq[String]): Seq[String] = logs
}
