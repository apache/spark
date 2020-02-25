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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.{Long => JLong}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.util.Random

import org.apache.hadoop.fs.{FSDataInputStream, Path, RawLocalFileSystem}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class FileStreamSinkLogSuite extends SparkFunSuite with SharedSparkSession {

  import CompactibleFileStreamLog._
  import FileStreamSinkLog._

  def executeFuncWithMetadataVersion(metadataVersion: Int, func: => Any): Unit = {
    withSQLConf(
      Seq(SQLConf.FILE_SINK_LOG_WRITE_METADATA_VERSION.key -> metadataVersion.toString): _*) {
      func
    }
  }

  // This makes sure tests are passing for all supported versions on write version, where
  // the read version is set to the highest supported version. This ensures Spark can read
  // older versions of file stream sink metadata log.
  def testWithAllMetadataVersions(name: String)(func: => Any): Unit = {
    for (version <- FileStreamSinkLog.SUPPORTED_VERSIONS) {
      test(s"$name - metadata version $version") {
        executeFuncWithMetadataVersion(version, func)
      }
    }
  }

  testWithAllMetadataVersions("compactLogs") {
    withFileStreamSinkLog { sinkLog =>
      val logs = Seq(
        newFakeSinkFileStatus("/a/b/x", FileStreamSinkLog.ADD_ACTION),
        newFakeSinkFileStatus("/a/b/y", FileStreamSinkLog.ADD_ACTION),
        newFakeSinkFileStatus("/a/b/z", FileStreamSinkLog.ADD_ACTION))
      assert(logs === sinkLog.compactLogs(logs))

      val logs2 = Seq(
        newFakeSinkFileStatus("/a/b/m", FileStreamSinkLog.ADD_ACTION),
        newFakeSinkFileStatus("/a/b/n", FileStreamSinkLog.ADD_ACTION),
        newFakeSinkFileStatus("/a/b/z", FileStreamSinkLog.DELETE_ACTION))
      assert(logs.dropRight(1) ++ logs2.dropRight(1) === sinkLog.compactLogs(logs ++ logs2))
    }
  }

  testWithAllMetadataVersions("serialize & deserialize") {
    withFileStreamSinkLog { sinkLog =>
      val logs = Array(
        SinkFileStatus(
          path = "/a/b/x",
          size = 100L,
          isDir = false,
          modificationTime = 1000L,
          blockReplication = 1,
          blockSize = 10000L,
          action = FileStreamSinkLog.ADD_ACTION),
        SinkFileStatus(
          path = "/a/b/y",
          size = 200L,
          isDir = false,
          modificationTime = 2000L,
          blockReplication = 2,
          blockSize = 20000L,
          action = FileStreamSinkLog.DELETE_ACTION),
        SinkFileStatus(
          path = "/a/b/z",
          size = 300L,
          isDir = false,
          modificationTime = 3000L,
          blockReplication = 3,
          blockSize = 30000L,
          action = FileStreamSinkLog.ADD_ACTION))

      val baos = new ByteArrayOutputStream()
      sinkLog.serialize(logs, baos)

      val actualLogs = sinkLog.deserialize(new ByteArrayInputStream(baos.toByteArray))
      assert(actualLogs === logs)

      baos.reset()
      sinkLog.serialize(Array(), baos)
      val actualLogs2 = sinkLog.deserialize(new ByteArrayInputStream(baos.toByteArray))
      assert(actualLogs2.isEmpty)
    }
  }

  testWithAllMetadataVersions("compact") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
      withFileStreamSinkLog { sinkLog =>
        for (batchId <- 0 to 10) {
          sinkLog.add(
            batchId,
            Array(newFakeSinkFileStatus("/a/b/" + batchId, FileStreamSinkLog.ADD_ACTION)))
          val expectedFiles = (0 to batchId).map {
            id => newFakeSinkFileStatus("/a/b/" + id, FileStreamSinkLog.ADD_ACTION)
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

  testWithAllMetadataVersions("delete expired file") {
    // Set FILE_SINK_LOG_CLEANUP_DELAY to 0 so that we can detect the deleting behaviour
    // deterministically and one min batches to retain
    withSQLConf(
      SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3",
      SQLConf.FILE_SINK_LOG_CLEANUP_DELAY.key -> "0",
      SQLConf.MIN_BATCHES_TO_RETAIN.key -> "1") {
      withFileStreamSinkLog { sinkLog =>
        val fs = sinkLog.metadataPath.getFileSystem(spark.sessionState.newHadoopConf())

        def listBatchFiles(): Set[String] = {
          fs.listStatus(sinkLog.metadataPath).map(_.getPath.getName).filter { fileName =>
            try {
              getBatchIdFromFileName(fileName)
              true
            } catch {
              case _: NumberFormatException => false
            }
          }.toSet
        }

        sinkLog.add(0, Array(newFakeSinkFileStatus("/a/b/0", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0") === listBatchFiles())
        sinkLog.add(1, Array(newFakeSinkFileStatus("/a/b/1", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0", "1") === listBatchFiles())
        sinkLog.add(2, Array(newFakeSinkFileStatus("/a/b/2", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0", "1", "2.compact") === listBatchFiles())
        sinkLog.add(3, Array(newFakeSinkFileStatus("/a/b/3", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3") === listBatchFiles())
        sinkLog.add(4, Array(newFakeSinkFileStatus("/a/b/4", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4") === listBatchFiles())
        sinkLog.add(5, Array(newFakeSinkFileStatus("/a/b/5", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4", "5.compact") === listBatchFiles())
        sinkLog.add(6, Array(newFakeSinkFileStatus("/a/b/6", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("5.compact", "6") === listBatchFiles())
      }
    }

    withSQLConf(
      SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3",
      SQLConf.FILE_SINK_LOG_CLEANUP_DELAY.key -> "0",
      SQLConf.MIN_BATCHES_TO_RETAIN.key -> "2") {
      withFileStreamSinkLog { sinkLog =>
        val fs = sinkLog.metadataPath.getFileSystem(spark.sessionState.newHadoopConf())

        def listBatchFiles(): Set[String] = {
          fs.listStatus(sinkLog.metadataPath).map(_.getPath.getName).filter { fileName =>
            try {
              getBatchIdFromFileName(fileName)
              true
            } catch {
              case _: NumberFormatException => false
            }
          }.toSet
        }

        sinkLog.add(0, Array(newFakeSinkFileStatus("/a/b/0", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0") === listBatchFiles())
        sinkLog.add(1, Array(newFakeSinkFileStatus("/a/b/1", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0", "1") === listBatchFiles())
        sinkLog.add(2, Array(newFakeSinkFileStatus("/a/b/2", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0", "1", "2.compact") === listBatchFiles())
        sinkLog.add(3, Array(newFakeSinkFileStatus("/a/b/3", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0", "1", "2.compact", "3") === listBatchFiles())
        sinkLog.add(4, Array(newFakeSinkFileStatus("/a/b/4", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4") === listBatchFiles())
        sinkLog.add(5, Array(newFakeSinkFileStatus("/a/b/5", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4", "5.compact") === listBatchFiles())
        sinkLog.add(6, Array(newFakeSinkFileStatus("/a/b/6", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4", "5.compact", "6") === listBatchFiles())
        sinkLog.add(7, Array(newFakeSinkFileStatus("/a/b/7", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("5.compact", "6", "7") === listBatchFiles())
      }
    }
  }

  testWithAllMetadataVersions("read Spark 2.1.0 log format") {
    assert(readFromResource("file-sink-log-version-2.1.0") === Seq(
      // SinkFileStatus("/a/b/0", 100, false, 100, 1, 100, FileStreamSinkLog.ADD_ACTION), -> deleted
      SinkFileStatus("/a/b/1", 100, false, 100, 1, 100, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/2", 200, false, 200, 1, 100, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/3", 300, false, 300, 1, 100, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/4", 400, false, 400, 1, 100, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/5", 500, false, 500, 1, 100, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/6", 600, false, 600, 1, 100, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/7", 700, false, 700, 1, 100, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/8", 800, false, 800, 1, 100, FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus("/a/b/9", 900, false, 900, 3, 200, FileStreamSinkLog.ADD_ACTION)
    ))
  }

  testWithAllMetadataVersions("getLatestBatchId") {
    withCountOpenLocalFileSystemAsLocalFileSystem {
      val scheme = CountOpenLocalFileSystem.scheme
      withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
        withTempDir { dir =>
          val sinkLog = new FileStreamSinkLog(FileStreamSinkLog.VERSION, spark,
            s"$scheme:///${dir.getCanonicalPath}")
          for (batchId <- 0L to 2L) {
            sinkLog.add(
              batchId,
              Array(newFakeSinkFileStatus("/a/b/" + batchId, FileStreamSinkLog.ADD_ACTION)))
          }

          def getCountForOpenOnMetadataFile(batchId: Long): Long = {
            val path = sinkLog.batchIdToPath(batchId).toUri.getPath
            CountOpenLocalFileSystem.pathToNumOpenCalled.getOrDefault(path, 0L)
          }

          CountOpenLocalFileSystem.resetCount()

          assert(sinkLog.getLatestBatchId() === Some(2L))
          // getLatestBatchId doesn't open the latest metadata log file
          (0L to 2L).foreach { batchId =>
            assert(getCountForOpenOnMetadataFile(batchId) === 0L)
          }

          assert(sinkLog.getLatest().map(_._1).getOrElse(-1L) === 2L)
          (0L to 1L).foreach { batchId =>
            assert(getCountForOpenOnMetadataFile(batchId) === 0L)
          }
          // getLatest opens the latest metadata log file, which explains the needs on
          // having "getLatestBatchId".
          assert(getCountForOpenOnMetadataFile(2L) === 1L)
        }
      }
    }
  }

  /**
   * Create a fake SinkFileStatus using path and action. Most of tests don't care about other fields
   * in SinkFileStatus.
   */
  private def newFakeSinkFileStatus(path: String, action: String): SinkFileStatus = {
    SinkFileStatus(
      path = path,
      size = 100L,
      isDir = false,
      modificationTime = 100L,
      blockReplication = 1,
      blockSize = 100L,
      action = action)
  }

  private def withFileStreamSinkLog(f: FileStreamSinkLog => Unit): Unit = {
    withTempDir { file =>
      val sinkLog = new FileStreamSinkLog(FileStreamSinkLog.VERSION, spark, file.getCanonicalPath)
      f(sinkLog)
    }
  }

  private def readFromResource(dir: String): Seq[SinkFileStatus] = {
    val input = getClass.getResource(s"/structured-streaming/$dir")
    val log = new FileStreamSinkLog(FileStreamSinkLog.VERSION, spark, input.toString)
    log.allFiles()
  }

  private def withCountOpenLocalFileSystemAsLocalFileSystem(body: => Unit): Unit = {
    val optionKey = s"fs.${CountOpenLocalFileSystem.scheme}.impl"
    val originClassForLocalFileSystem = spark.conf.getOption(optionKey)
    try {
      spark.conf.set(optionKey, classOf[CountOpenLocalFileSystem].getName)
      body
    } finally {
      originClassForLocalFileSystem match {
        case Some(fsClazz) => spark.conf.set(optionKey, fsClazz)
        case _ => spark.conf.unset(optionKey)
      }
    }
  }
}

class CountOpenLocalFileSystem extends RawLocalFileSystem {
  import CountOpenLocalFileSystem._

  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val path = f.toUri.getPath
    pathToNumOpenCalled.compute(path, (_, v) => {
      if (v == null) 1L else v + 1
    })
    super.open(f, bufferSize)
  }
}

object CountOpenLocalFileSystem {
  val scheme = s"FileStreamSinkLogSuite${math.abs(Random.nextInt)}fs"
  val pathToNumOpenCalled = new ConcurrentHashMap[String, JLong]

  def resetCount(): Unit = pathToNumOpenCalled.clear()
}
