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

import java.io.{File, FileOutputStream, IOException}
import java.net.URI

import scala.collection.mutable
import scala.io.{Codec, Source}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogTestHelper._
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils


abstract class EventLogFileWritersSuite extends SparkFunSuite with LocalSparkContext
  with BeforeAndAfter {

  protected val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  protected var testDir: File = _
  protected var testDirPath: Path = _

  before {
    testDir = Utils.createTempDir(namePrefix = s"event log")
    testDirPath = new Path(testDir.getAbsolutePath())
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("create EventLogFileWriter with enable/disable rolling") {
    def buildWriterAndVerify(conf: SparkConf, expectedClazz: Class[_]): Unit = {
      val writer = EventLogFileWriter(
        getUniqueApplicationId, None, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))
      val writerClazz = writer.getClass
      assert(expectedClazz === writerClazz)
    }

    val conf = new SparkConf
    conf.set(EVENT_LOG_ENABLED, true)
    conf.set(EVENT_LOG_DIR, testDir.toString)

    // default config
    buildWriterAndVerify(conf, classOf[RollingEventLogFilesWriter])

    conf.set(EVENT_LOG_ENABLE_ROLLING, true)
    buildWriterAndVerify(conf, classOf[RollingEventLogFilesWriter])

    conf.set(EVENT_LOG_ENABLE_ROLLING, false)
    buildWriterAndVerify(conf, classOf[SingleEventLogFileWriter])
  }

  val allCodecs = Seq(None) ++
    CompressionCodec.ALL_COMPRESSION_CODECS.map(c => Some(CompressionCodec.getShortName(c)))

  allCodecs.foreach { codecShortName =>
    test(s"initialize, write, stop - with codec $codecShortName") {
      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = getLoggingConf(testDirPath, codecShortName)
      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))

      writer.start()

      // snappy stream throws exception on empty stream, so we should provide some data to test.
      val dummyData = Seq("dummy1", "dummy2", "dummy3")
      dummyData.foreach(writer.writeEvent(_, flushLogger = true))

      writer.stop()

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri, codecShortName, dummyData)
    }
  }

  test("Use the default value of spark.eventLog.compression.codec") {
    val conf = new SparkConf
    conf.set(EVENT_LOG_COMPRESS, true)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

    val appId = "test"
    val appAttemptId = None

    val writer = createWriter(appId, appAttemptId, testDirPath.toUri, conf, hadoopConf)
    assert(writer.compressionCodecName === EVENT_LOG_COMPRESSION_CODEC.defaultValue)
  }

  protected def readLinesFromEventLogFile(log: Path, fs: FileSystem): List[String] = {
    val logDataStream = EventLogFileReader.openEventLog(log, fs)
    try {
      Source.fromInputStream(logDataStream)(Codec.UTF8).getLines().toList
    } finally {
      logDataStream.close()
    }
  }

  protected def createWriter(
      appId: String,
      appAttemptId : Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter

  /**
   * This should be called with "closed" event log file; No guarantee on reading event log file
   * which is being written, especially the file is compressed. SHS also does the best it can.
   */
  protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId : Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      expectedLines: Seq[String] = Seq.empty): Unit
}

class SingleEventLogFileWriterSuite extends EventLogFileWritersSuite {

  test("Log overwriting") {
    val appId = "test"
    val appAttemptId = None
    val logUri = SingleEventLogFileWriter.getLogPath(testDir.toURI, appId, appAttemptId)

    val conf = getLoggingConf(testDirPath)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val writer = createWriter(appId, appAttemptId, testDir.toURI, conf, hadoopConf)

    val logPath = new Path(logUri).toUri.getPath
    writer.start()

    val dummyData = Seq("dummy1", "dummy2", "dummy3")
    dummyData.foreach(writer.writeEvent(_, flushLogger = true))

    // Create file before writing the event log
    new FileOutputStream(new File(logPath)).close()
    // Expected IOException, since we haven't enabled log overwrite.
    intercept[IOException] { writer.stop() }

    // Try again, but enable overwriting.
    conf.set(EVENT_LOG_OVERWRITE, true)
    val writer2 = createWriter(appId, appAttemptId, testDir.toURI, conf, hadoopConf)
    writer2.start()
    dummyData.foreach(writer2.writeEvent(_, flushLogger = true))
    writer2.stop()
  }

  test("Event log name") {
    val baseDirUri = Utils.resolveURI("/base-dir")
    // without compression
    assert(s"${baseDirUri.toString}/app1" === SingleEventLogFileWriter.getLogPath(
      baseDirUri, "app1", None, None))
    // with compression
    assert(s"${baseDirUri.toString}/app1.lzf" ===
      SingleEventLogFileWriter.getLogPath(baseDirUri, "app1", None, Some(CompressionCodec.LZF)))
    // illegal characters in app ID
    assert(s"${baseDirUri.toString}/a-fine-mind_dollar_bills__1" ===
      SingleEventLogFileWriter.getLogPath(baseDirUri,
        "a fine:mind$dollar{bills}.1", None, None))
    // illegal characters in app ID with compression
    assert(s"${baseDirUri.toString}/a-fine-mind_dollar_bills__1.lz4" ===
      SingleEventLogFileWriter.getLogPath(baseDirUri,
        "a fine:mind$dollar{bills}.1", None, Some(CompressionCodec.LZ4)))
  }

  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      expectedLines: Seq[String]): Unit = {
    // read single event log file
    val logPath = SingleEventLogFileWriter.getLogPath(logBaseDir, appId, appAttemptId,
      compressionCodecShortName)

    val finalLogPath = new Path(logPath)
    assert(fileSystem.exists(finalLogPath) && fileSystem.getFileStatus(finalLogPath).isFile)
    assert(expectedLines === readLinesFromEventLogFile(finalLogPath, fileSystem))
  }
}

class RollingEventLogFilesWriterSuite extends EventLogFileWritersSuite {
  import RollingEventLogFilesWriter._

  test("Event log names") {
    val baseDirUri = Utils.resolveURI("/base-dir")
    val appId = "app1"
    val appAttemptId = None

    // happy case with app ID
    val logDir = RollingEventLogFilesWriter.getAppEventLogDirPath(baseDirUri, appId, None)
    assert(s"${baseDirUri.toString}/${EVENT_LOG_DIR_NAME_PREFIX}${appId}" === logDir.toString)

    // appstatus: inprogress or completed
    assert(s"$logDir/${APPSTATUS_FILE_NAME_PREFIX}${appId}${EventLogFileWriter.IN_PROGRESS}" ===
      RollingEventLogFilesWriter.getAppStatusFilePath(logDir, appId, appAttemptId,
        inProgress = true).toString)
    assert(s"$logDir/${APPSTATUS_FILE_NAME_PREFIX}${appId}" ===
      RollingEventLogFilesWriter.getAppStatusFilePath(logDir, appId, appAttemptId,
        inProgress = false).toString)

    // without compression
    assert(s"$logDir/${EVENT_LOG_FILE_NAME_PREFIX}1_${appId}" ===
      RollingEventLogFilesWriter.getEventLogFilePath(logDir, appId, appAttemptId, 1, None).toString)

    // with compression
    assert(s"$logDir/${EVENT_LOG_FILE_NAME_PREFIX}1_${appId}.lzf" ===
      RollingEventLogFilesWriter.getEventLogFilePath(logDir, appId, appAttemptId,
        1, Some(CompressionCodec.LZF)).toString)

    // illegal characters in app ID
    assert(s"${baseDirUri.toString}/${EVENT_LOG_DIR_NAME_PREFIX}a-fine-mind_dollar_bills__1" ===
      RollingEventLogFilesWriter.getAppEventLogDirPath(baseDirUri,
        "a fine:mind$dollar{bills}.1", None).toString)
  }

  test("Log overwriting") {
    val appId = "test"
    val appAttemptId = None
    val logDirPath = RollingEventLogFilesWriter.getAppEventLogDirPath(testDir.toURI, appId,
      appAttemptId)

    val conf = getLoggingConf(testDirPath)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val writer = createWriter(appId, appAttemptId, testDir.toURI, conf, hadoopConf)

    val logPath = logDirPath.toUri.getPath

    // Create file before writing the event log directory
    // it doesn't matter whether the existing one is file or directory
    new FileOutputStream(new File(logPath)).close()

    // Expected IOException, since we haven't enabled log overwrite.
    // Note that the place IOException is thrown is different from single event log file.
    intercept[IOException] { writer.start() }

    // Try again, but enable overwriting.
    conf.set(EVENT_LOG_OVERWRITE, true)

    val writer2 = createWriter(appId, appAttemptId, testDir.toURI, conf, hadoopConf)
    writer2.start()
    val dummyData = Seq("dummy1", "dummy2", "dummy3")
    dummyData.foreach(writer2.writeEvent(_, flushLogger = true))
    writer2.stop()
  }

  allCodecs.foreach { codecShortName =>
    test(s"rolling event log files - codec $codecShortName") {
      def assertEventLogFilesIndex(
          eventLogFiles: Seq[FileStatus],
          expectedLastIndex: Int,
          expectedMaxSizeBytes: Long): Unit = {
        assert(eventLogFiles.forall(f => f.getLen <= expectedMaxSizeBytes))
        assert((1 to expectedLastIndex) ===
          eventLogFiles.map(f => getEventLogFileIndex(f.getPath.getName)))
      }

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
      val expectedLines = writeTestEvents(writer, dummyStr, 1024 * 1024 * 21)

      val logDirPath = getAppEventLogDirPath(testDirPath.toUri, appId, attemptId)

      val eventLogFiles = listEventLogFiles(logDirPath)
      assertEventLogFilesIndex(eventLogFiles, 3, 1024 * 1024 * 10)

      writer.stop()

      val eventLogFiles2 = listEventLogFiles(logDirPath)
      assertEventLogFilesIndex(eventLogFiles2, 3, 1024 * 1024 * 10)

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri,
        codecShortName, expectedLines)
    }
  }

  test(s"rolling event log files - the max size of event log file size less than lower limit") {
    val appId = getUniqueApplicationId
    val attemptId = None

    val conf = getLoggingConf(testDirPath, None)
    conf.set(EVENT_LOG_ENABLE_ROLLING, true)
    conf.set(EVENT_LOG_ROLLING_MAX_FILE_SIZE.key, "9m")

    val e = intercept[IllegalArgumentException] {
      createWriter(appId, attemptId, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))
    }
    assert(e.getMessage.contains("should be configured to be at least"))
  }

  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      expectedLines: Seq[String]): Unit = {
    val logDirPath = getAppEventLogDirPath(logBaseDir, appId, appAttemptId)

    assert(fileSystem.exists(logDirPath) && fileSystem.getFileStatus(logDirPath).isDirectory)

    val appStatusFile = getAppStatusFilePath(logDirPath, appId, appAttemptId, inProgress = false)
    assert(fileSystem.exists(appStatusFile) && fileSystem.getFileStatus(appStatusFile).isFile)

    val eventLogFiles = listEventLogFiles(logDirPath)
    val allLines = mutable.ArrayBuffer[String]()
    eventLogFiles.foreach { file =>
      allLines.appendAll(readLinesFromEventLogFile(file.getPath, fileSystem))
    }

    assert(expectedLines === allLines)
  }

  private def listEventLogFiles(logDirPath: Path): Seq[FileStatus] = {
    fileSystem.listStatus(logDirPath).filter(isEventLogFile)
      .sortBy { fs => getEventLogFileIndex(fs.getPath.getName) }.toImmutableArraySeq
  }
}
