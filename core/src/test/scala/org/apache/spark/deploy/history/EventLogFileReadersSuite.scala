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

package org.apache.spark.deploy.history

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.zip.{ZipInputStream, ZipOutputStream}

import com.google.common.io.{ByteStreams, Files}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogTestHelper._
import org.apache.spark.deploy.history.RollingEventLogFilesWriter._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils


abstract class EventLogFileReadersSuite extends SparkFunSuite with LocalSparkContext
  with BeforeAndAfter with Logging {

  protected val fileSystem = Utils.getHadoopFileSystem("/", SparkHadoopUtil.get.conf)
  protected var testDir: File = _
  protected var testDirPath: Path = _

  before {
    testDir = Utils.createTempDir(namePrefix = s"event log")
    testDirPath = new Path(testDir.getAbsolutePath())
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Retrieve EventLogFileReader correctly") {
    def assertInstanceOfEventLogReader(
        expectedClazz: Option[Class[_ <: EventLogFileReader]],
        actual: Option[EventLogFileReader]): Unit = {
      if (expectedClazz.isEmpty) {
        assert(actual.isEmpty, s"Expected no EventLogFileReader instance but was " +
          s"${actual.map(_.getClass).getOrElse("<None>")}")
      } else {
        assert(actual.isDefined, s"Expected an EventLogFileReader instance but was empty")
        assert(expectedClazz.get.isAssignableFrom(actual.get.getClass),
          s"Expected ${expectedClazz.get} but was ${actual.get.getClass}")
      }
    }

    def testCreateEventLogReaderWithPath(
        path: Path,
        isFile: Boolean,
        expectedClazz: Option[Class[_ <: EventLogFileReader]]): Unit = {
      if (isFile) {
        Utils.tryWithResource(fileSystem.create(path)) { is =>
          is.writeInt(10)
        }
      } else {
        fileSystem.mkdirs(path)
        fileSystem.create(getAppStatusFilePath(path, "app", None, true))
        fileSystem.create(getEventLogFilePath(path, "app", None, 1, None))
      }

      val reader = EventLogFileReader(fileSystem, path)
      assertInstanceOfEventLogReader(expectedClazz, reader)
      val reader2 = EventLogFileReader(fileSystem,
        fileSystem.getFileStatus(path))
      assertInstanceOfEventLogReader(expectedClazz, reader2)
    }

    // path with no last index - single event log
    val reader1 = EventLogFileReader(fileSystem, new Path(testDirPath, "aaa"),
      None)
    assertInstanceOfEventLogReader(Some(classOf[SingleFileEventLogFileReader]), Some(reader1))

    // path with last index - rolling event log
    val reader2 = EventLogFileReader(fileSystem,
      new Path(testDirPath, s"${EVENT_LOG_DIR_NAME_PREFIX}aaa"), Some(3))
    assertInstanceOfEventLogReader(Some(classOf[RollingEventLogFilesFileReader]), Some(reader2))

    // path - file (both path and FileStatus)
    val eventLogFile = new Path(testDirPath, "bbb")
    testCreateEventLogReaderWithPath(eventLogFile, isFile = true,
      Some(classOf[SingleFileEventLogFileReader]))

    // path - file starting with "."
    val invalidEventLogFile = new Path(testDirPath, ".bbb")
    testCreateEventLogReaderWithPath(invalidEventLogFile, isFile = true, None)

    // path - directory with "eventlog_v2_" prefix
    val eventLogDir = new Path(testDirPath, s"${EVENT_LOG_DIR_NAME_PREFIX}ccc")
    testCreateEventLogReaderWithPath(eventLogDir, isFile = false,
      Some(classOf[RollingEventLogFilesFileReader]))

    // path - directory with no "eventlog_v2_" prefix
    val invalidEventLogDir = new Path(testDirPath, "ccc")
    testCreateEventLogReaderWithPath(invalidEventLogDir, isFile = false, None)
  }

  val allCodecs = Seq(None) ++
    CompressionCodec.ALL_COMPRESSION_CODECS.map { c => Some(CompressionCodec.getShortName(c)) }

  allCodecs.foreach { codecShortName =>
    test(s"get information, list event log files, zip log files - with codec $codecShortName") {
      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = getLoggingConf(testDirPath, codecShortName)
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf, hadoopConf)
      writer.start()

      // The test for writing events into EventLogFileWriter is covered to its own test suite.
      val dummyData = Seq("dummy1", "dummy2", "dummy3")
      dummyData.foreach(writer.writeEvent(_, flushLogger = true))

      val logPathIncompleted = getCurrentLogPath(writer.logPath, isCompleted = false)
      val readerOpt = EventLogFileReader(fileSystem, new Path(logPathIncompleted))
      assertAppropriateReader(readerOpt)
      val reader = readerOpt.get

      verifyReader(reader, new Path(logPathIncompleted), codecShortName, isCompleted = false)

      writer.stop()

      val logPathCompleted = getCurrentLogPath(writer.logPath, isCompleted = true)
      val readerOpt2 = EventLogFileReader(fileSystem, new Path(logPathCompleted))
      assertAppropriateReader(readerOpt2)
      val reader2 = readerOpt2.get

      verifyReader(reader2, new Path(logPathCompleted), codecShortName, isCompleted = true)
    }
  }

  protected def createWriter(
      appId: String,
      appAttemptId : Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter

  protected def getCurrentLogPath(logPath: String, isCompleted: Boolean): String

  protected def assertAppropriateReader(actualReader: Option[EventLogFileReader]): Unit

  protected def verifyReader(
      reader: EventLogFileReader,
      logPath: Path,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean): Unit
}

class SingleFileEventLogFileReaderSuite extends EventLogFileReadersSuite {
  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def assertAppropriateReader(actualReader: Option[EventLogFileReader]): Unit = {
    assert(actualReader.isDefined, s"Expected an EventLogReader instance but was empty")
    assert(actualReader.get.isInstanceOf[SingleFileEventLogFileReader],
      s"Expected SingleFileEventLogReader but was ${actualReader.get.getClass}")
  }

  override protected def getCurrentLogPath(logPath: String, isCompleted: Boolean): String = {
    if (!isCompleted) logPath + EventLogFileWriter.IN_PROGRESS else logPath
  }

  override protected def verifyReader(
      reader: EventLogFileReader,
      logPath: Path,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean): Unit = {
    val status = fileSystem.getFileStatus(logPath)

    assert(status.isFile)
    assert(reader.rootPath === fileSystem.makeQualified(logPath))
    assert(reader.lastIndex.isEmpty)
    assert(reader.fileSizeForLastIndex === status.getLen)
    assert(reader.completed === isCompleted)
    assert(reader.modificationTime === status.getModificationTime)
    assert(reader.listEventLogFiles.length === 1)
    assert(reader.listEventLogFiles.map(_.getPath.toUri.getPath) ===
      Seq(logPath.toUri.getPath))
    assert(reader.compressionCodec === compressionCodecShortName)
    assert(reader.totalSize === status.getLen)

    val underlyingStream = new ByteArrayOutputStream()
    Utils.tryWithResource(new ZipOutputStream(underlyingStream)) { os =>
      reader.zipEventLogFiles(os)
    }

    Utils.tryWithResource(new ZipInputStream(
        new ByteArrayInputStream(underlyingStream.toByteArray))) { is =>

      val entry = is.getNextEntry
      assert(entry != null)
      val actual = new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8)
      val expected = Files.asCharSource(new File(logPath.toString), StandardCharsets.UTF_8).read()
      assert(actual === expected)
      assert(is.getNextEntry === null)
    }
  }
}

class RollingEventLogFilesReaderSuite extends EventLogFileReadersSuite {
  test("SPARK-46012: appStatus file should exist") {
    withTempDir { dir =>
      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = getLoggingConf(testDirPath)
      conf.set(EVENT_LOG_ENABLE_ROLLING, true)
      conf.set(EVENT_LOG_ROLLING_MAX_FILE_SIZE.key, "10m")

      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))

      writer.start()
      val dummyStr = "dummy" * 1024
      writeTestEvents(writer, dummyStr, 1024 * 1024 * 20)
      writer.stop()

      // Verify a healthy rolling event log directory
      val logPathCompleted = getCurrentLogPath(writer.logPath, isCompleted = true)
      val readerOpt = EventLogFileReader(fileSystem, new Path(logPathCompleted))
      assert(readerOpt.get.isInstanceOf[RollingEventLogFilesFileReader])
      assert(readerOpt.get.listEventLogFiles.length === 3)

      // Make unhealthy rolling event directory by removing appStatus file.
      val appStatusFile = fileSystem.listStatus(new Path(logPathCompleted))
        .find(RollingEventLogFilesWriter.isAppStatusFile).get.getPath
      fileSystem.delete(appStatusFile, false)
      assert(EventLogFileReader(fileSystem, new Path(logPathCompleted)).isEmpty)
    }
  }

  allCodecs.foreach { codecShortName =>
    test(s"rolling event log files - codec $codecShortName") {
      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = getLoggingConf(testDirPath, codecShortName)
      conf.set(EVENT_LOG_ENABLE_ROLLING, true)
      conf.set(EVENT_LOG_ROLLING_MAX_FILE_SIZE.key, "10m")

      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))

      writer.start()

      // write log more than 20m (intended to roll over to 3 files)
      val dummyStr = "dummy" * 1024
      writeTestEvents(writer, dummyStr, 1024 * 1024 * 20)

      val logPathIncompleted = getCurrentLogPath(writer.logPath, isCompleted = false)
      val readerOpt = EventLogFileReader(fileSystem,
        new Path(logPathIncompleted))
      verifyReader(readerOpt.get, new Path(logPathIncompleted), codecShortName, isCompleted = false)
      assert(readerOpt.get.listEventLogFiles.length === 3)

      writer.stop()

      val logPathCompleted = getCurrentLogPath(writer.logPath, isCompleted = true)
      val readerOpt2 = EventLogFileReader(fileSystem, new Path(logPathCompleted))
      verifyReader(readerOpt2.get, new Path(logPathCompleted), codecShortName, isCompleted = true)
      assert(readerOpt2.get.listEventLogFiles.length === 3)
    }
  }

  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def assertAppropriateReader(actualReader: Option[EventLogFileReader]): Unit = {
    assert(actualReader.isDefined, s"Expected an EventLogReader instance but was empty")
    assert(actualReader.get.isInstanceOf[RollingEventLogFilesFileReader],
      s"Expected RollingEventLogFilesReader but was ${actualReader.get.getClass}")
  }

  override protected def getCurrentLogPath(logPath: String, isCompleted: Boolean): String = logPath

  override protected def verifyReader(
      reader: EventLogFileReader,
      logPath: Path,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean): Unit = {
    import RollingEventLogFilesWriter._

    val status = fileSystem.getFileStatus(logPath)
    assert(status.isDirectory)

    val statusInDir = fileSystem.listStatus(logPath)
    val eventFiles = statusInDir.filter(isEventLogFile).sortBy { s =>
      getEventLogFileIndex(s.getPath.getName)
    }
    assert(eventFiles.nonEmpty)
    val lastEventFile = eventFiles.last
    val allLen = eventFiles.map(_.getLen).sum

    assert(reader.rootPath === fileSystem.makeQualified(logPath))
    assert(reader.lastIndex === Some(getEventLogFileIndex(lastEventFile.getPath.getName)))
    assert(reader.fileSizeForLastIndex === lastEventFile.getLen)
    assert(reader.completed === isCompleted)
    assert(reader.modificationTime === lastEventFile.getModificationTime)
    assert(reader.listEventLogFiles.length === eventFiles.length)
    assert(reader.listEventLogFiles.map(_.getPath) === eventFiles.map(_.getPath))
    assert(reader.compressionCodec === compressionCodecShortName)
    assert(reader.totalSize === allLen)

    val underlyingStream = new ByteArrayOutputStream()
    Utils.tryWithResource(new ZipOutputStream(underlyingStream)) { os =>
      reader.zipEventLogFiles(os)
    }

    Utils.tryWithResource(new ZipInputStream(
      new ByteArrayInputStream(underlyingStream.toByteArray))) { is =>

      val entry = is.getNextEntry
      assert(entry != null)

      // directory
      assert(entry.getName === logPath.getName + "/")

      val allFileNames = fileSystem.listStatus(logPath).map(_.getPath.getName).toSet

      var count = 0
      var noMoreEntry = false
      while (!noMoreEntry) {
        val entry = is.getNextEntry
        if (entry == null) {
          noMoreEntry = true
        } else {
          count += 1

          assert(entry.getName.startsWith(logPath.getName + "/"))
          val fileName = entry.getName.stripPrefix(logPath.getName + "/")
          assert(allFileNames.contains(fileName))

          val actual = new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8)
          val expected = Files.asCharSource(
            new File(logPath.toString, fileName), StandardCharsets.UTF_8).read()
          assert(actual === expected)
        }
      }

      assert(count === allFileNames.size)
    }
  }
}
