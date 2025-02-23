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
import java.util.ConcurrentModificationException

import scala.language.implicitConversions

import org.scalatest.concurrent.Waiters._
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.UninterruptibleThread

class HDFSMetadataLogSuite extends SharedSparkSession {

  private implicit def toOption[A](a: A): Option[A] = Option(a)

  test("SPARK-46339: Directory with number name should not be treated as metadata log") {
    withTempDir { temp =>
      val dir = new File(temp, "dir")
      val metadataLog = new HDFSMetadataLog[String](spark, dir.getAbsolutePath)
      assert(metadataLog.metadataPath.toString.endsWith("/dir"))

      // Create a directory with batch id 0
      new File(dir, "0").mkdir()
      assert(metadataLog.getLatest() === None)
    }
  }

  test("HDFSMetadataLog: basic") {
    withTempDir { temp =>
      val dir = new File(temp, "dir") // use non-existent directory to test whether log make the dir
      val metadataLog = new HDFSMetadataLog[String](spark, dir.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(None, Some(0)) === Array(0 -> "batch0"))

      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))

      // Adding the same batch does nothing
      metadataLog.add(1, "batch1-duplicated")
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }

  test("HDFSMetadataLog: purge") {
    testPurge()
  }

  Seq(
    classOf[FileSystemBasedCheckpointFileManager],
    classOf[FileContextBasedCheckpointFileManager]
  ).map(_.getCanonicalName).foreach { cls =>
    test(s"HDFSMetadataLog: purge - explicit file manager - $cls") {
      withSQLConf(SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> cls) {
        testPurge()
      }
    }
  }

  private def testPurge(): Unit = {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.add(2, "batch2"))
      assert(metadataLog.get(0).isDefined)
      assert(metadataLog.get(1).isDefined)
      assert(metadataLog.get(2).isDefined)
      assert(metadataLog.getLatest().get._1 == 2)

      metadataLog.purge(2)
      assert(metadataLog.get(0).isEmpty)
      assert(metadataLog.get(1).isEmpty)
      assert(metadataLog.get(2).isDefined)
      assert(metadataLog.getLatest().get._1 == 2)

      // There should be at most two files, called "2", and optionally crc file,
      // in the metadata directory.
      // This check also tests for regressions of SPARK-17475
      val allFiles = new File(metadataLog.metadataPath.toString).listFiles().toSeq
      assert(allFiles.size <= 2)
      assert(allFiles.exists(_.getName == "2"))
      if (allFiles.size == 2) {
        // there's possibly crc file being left as well
        assert(allFiles.exists(_.getName == ".2.crc"))
      }
    }
  }

  test("HDFSMetadataLog: parseVersion") {
    withTempDir { dir =>
      val metadataLog = new HDFSMetadataLog[String](spark, dir.getAbsolutePath)
      def assertLogFileMalformed(func: => Int): Unit = {
        val e = intercept[IllegalStateException] { func }
        assert(e.getMessage.contains(s"Log file was malformed: failed to read correct log version"))
      }
      assertLogFileMalformed { metadataLog.validateVersion("", 100) }
      assertLogFileMalformed { metadataLog.validateVersion("xyz", 100) }
      assertLogFileMalformed { metadataLog.validateVersion("v10.x", 100) }
      assertLogFileMalformed { metadataLog.validateVersion("10", 100) }
      assertLogFileMalformed { metadataLog.validateVersion("v0", 100) }
      assertLogFileMalformed { metadataLog.validateVersion("v-10", 100) }

      assert(metadataLog.validateVersion("v10", 10) === 10)
      assert(metadataLog.validateVersion("v10", 100) === 10)

      val e = intercept[IllegalStateException] { metadataLog.validateVersion("v200", 100) }
      Seq(
        "maximum supported log version is v100, but encountered v200",
        "produced by a newer version of Spark and cannot be read by this version"
      ).foreach { message =>
        assert(e.getMessage.contains(message))
      }
    }
  }

  test("HDFSMetadataLog: restart") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))

      val metadataLog2 = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.get(1) === Some("batch1"))
      assert(metadataLog2.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog2.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }

  testQuietly("HDFSMetadataLog: metadata directory collision") {
    withTempDir { temp =>
      val waiter = new Waiter()
      val maxBatchId = 10
      val numThreads = 5
      for (id <- 0 until numThreads) {
        new UninterruptibleThread(s"HDFSMetadataLog: metadata directory collision - thread $id") {
          override def run(): Unit = waiter {
            val metadataLog =
              new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
            try {
              var nextBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)
              nextBatchId += 1
              while (nextBatchId <= maxBatchId) {
                metadataLog.add(nextBatchId, nextBatchId.toString)
                nextBatchId += 1
              }
            } catch {
              case _: ConcurrentModificationException =>
              // This is expected since there are multiple writers
            } finally {
              waiter.dismiss()
            }
          }
        }.start()
      }

      waiter.await(timeout(10.seconds), dismissals(numThreads))
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.getLatest() === Some(maxBatchId -> maxBatchId.toString))
      assert(
        metadataLog.get(None, Some(maxBatchId)) === (0 to maxBatchId).map(i => (i, i.toString)))
    }
  }

  test("verifyBatchIds") {
    import HDFSMetadataLog.verifyBatchIds
    verifyBatchIds(Seq(1L, 2L, 3L), Some(1L), Some(3L))
    verifyBatchIds(Seq(1L), Some(1L), Some(1L))
    verifyBatchIds(Seq(1L, 2L, 3L), None, Some(3L))
    verifyBatchIds(Seq(1L, 2L, 3L), Some(1L), None)
    verifyBatchIds(Seq(1L, 2L, 3L), None, None)

    intercept[IllegalStateException](verifyBatchIds(Seq(), Some(1L), None))
    intercept[IllegalStateException](verifyBatchIds(Seq(), None, Some(1L)))
    intercept[IllegalStateException](verifyBatchIds(Seq(), Some(1L), Some(1L)))
    intercept[IllegalStateException](verifyBatchIds(Seq(2, 3, 4), Some(1L), None))
    intercept[IllegalStateException](verifyBatchIds(Seq(2, 3, 4), None, Some(5L)))
    intercept[IllegalStateException](verifyBatchIds(Seq(2, 3, 4), Some(1L), Some(5L)))
    intercept[IllegalStateException](verifyBatchIds(Seq(1, 2, 4, 5), Some(1L), Some(5L)))

    // Related to SPARK-26629, this captures the behavior for verifyBatchIds when startId > endId
    intercept[IllegalStateException](verifyBatchIds(Seq(), Some(2L), Some(1L)))
    intercept[AssertionError](verifyBatchIds(Seq(2), Some(2L), Some(1L)))
    intercept[AssertionError](verifyBatchIds(Seq(1), Some(2L), Some(1L)))
    intercept[AssertionError](verifyBatchIds(Seq(0), Some(2L), Some(1L)))
  }
}
