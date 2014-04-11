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

package org.apache.spark.scheduler

import scala.collection.mutable
import scala.io.Source

import org.apache.hadoop.fs.{FileStatus, Path}
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.{JsonProtocol, Utils}
import org.apache.spark.io.CompressionCodec

/**
 * Test for whether EventLoggingListener logs events properly.
 *
 * This checks whether special files are created using the specified configurations, and whether
 * logged events can be read back into memory as expected.
 */
class EventLoggingListenerSuite extends FunSuite with BeforeAndAfter {

  private val fileSystem = Utils.getHadoopFileSystem("/")
  private val allCompressionCodecs = Seq[String](
    "org.apache.spark.io.LZFCompressionCodec",
    "org.apache.spark.io.SnappyCompressionCodec"
  )

  test("Parse names of special files") {
    testParsingFileName()
  }

  test("Verify special files exist") {
    testSpecialFilesExist()
    testSpecialFilesExist(logDirPath = Some("/tmp/spark-foo"))
    allCompressionCodecs.foreach { codec =>
      testSpecialFilesExist(compressionCodec = Some(codec))
    }
  }

  test("Parse event logging info") {
    testParsingLogInfo()
    testParsingLogInfo(logDirPath = Some("/tmp/spark-foo"))
    allCompressionCodecs.foreach { codec =>
      testParsingLogInfo(compressionCodec = Some(codec))
    }
  }

  test("Basic event logging") {
    testEventLogging()
    testEventLogging(logDirPath = Some("/tmp/spark-foo"))
    allCompressionCodecs.foreach { codec =>
      testEventLogging(compressionCodec = Some(codec))
    }
  }

  test("End-to-end event logging") {
    testApplicationEventLogging()
    testApplicationEventLogging(logDirPath = Some("/tmp/spark-foo"))
    allCompressionCodecs.foreach { codec =>
      testApplicationEventLogging(compressionCodec = Some(codec))
    }
  }


  /**
   * Test whether names of special files are correctly identified and parsed.
   */
  private def testParsingFileName() {
    val logPrefix = EventLoggingListener.LOG_PREFIX
    val sparkVersionPrefix = EventLoggingListener.SPARK_VERSION_PREFIX
    val compressionCodecPrefix = EventLoggingListener.COMPRESSION_CODEC_PREFIX
    val applicationComplete = EventLoggingListener.APPLICATION_COMPLETE
    assert(EventLoggingListener.isEventLogFile(logPrefix + "0"))
    assert(EventLoggingListener.isEventLogFile(logPrefix + "100"))
    assert(EventLoggingListener.isEventLogFile(logPrefix + "ANYTHING"))
    assert(EventLoggingListener.isSparkVersionFile(sparkVersionPrefix + "0.9.1"))
    assert(EventLoggingListener.isSparkVersionFile(sparkVersionPrefix + "1.0.0"))
    assert(EventLoggingListener.isSparkVersionFile(sparkVersionPrefix + "ANYTHING"))
    assert(EventLoggingListener.isApplicationCompleteFile(applicationComplete))
    allCompressionCodecs.foreach { codec =>
      assert(EventLoggingListener.isCompressionCodecFile(compressionCodecPrefix + codec))
    }

    // Negatives
    assert(!EventLoggingListener.isEventLogFile("The greatest man of all mankind"))
    assert(!EventLoggingListener.isSparkVersionFile("Will never falter in the face of death!"))
    assert(!EventLoggingListener.isCompressionCodecFile("Unless he chooses to leave behind"))
    assert(!EventLoggingListener.isApplicationCompleteFile("The very treasure he calls Macbeth"))

    // Verify that parsing is correct
    assert(EventLoggingListener.parseSparkVersion(sparkVersionPrefix + "1.0.0") === "1.0.0")
    allCompressionCodecs.foreach { codec =>
      assert(EventLoggingListener.parseCompressionCodec(compressionCodecPrefix + codec) === codec)
    }
  }

  /**
   * Test whether the special files produced by EventLoggingListener exist.
   *
   * There should be exactly one event log and one spark version file throughout the entire
   * execution. If a compression codec is specified, then the compression codec file should
   * also exist. Only after the application has completed does the test expect the application
   * completed file to be present.
   */
  private def testSpecialFilesExist(
      logDirPath: Option[String] = None,
      compressionCodec: Option[String] = None) {

    def assertFilesExist(logFiles: Array[FileStatus], loggerStopped: Boolean) {
      val numCompressionCodecFiles = if (compressionCodec.isDefined) 1 else 0
      val numApplicationCompleteFiles = if (loggerStopped) 1 else 0
      assert(logFiles.size === 2 + numCompressionCodecFiles + numApplicationCompleteFiles)
      assert(eventLogsExist(logFiles))
      assert(sparkVersionExists(logFiles))
      assert(compressionCodecExists(logFiles) === compressionCodec.isDefined)
      assert(applicationCompleteExists(logFiles) === loggerStopped)
      assertSparkVersionIsValid(logFiles)
      compressionCodec.foreach { codec =>
        assertCompressionCodecIsValid(logFiles, codec)
      }
    }

    // Verify logging directory exists
    val conf = getLoggingConf(logDirPath, compressionCodec)
    val eventLogger = new EventLoggingListener("test", conf)
    eventLogger.start()
    val logPath = new Path(eventLogger.logDir)
    val logDir = fileSystem.getFileStatus(logPath)
    assert(logDir.isDir)

    // Verify special files are as expected before stop()
    var logFiles = fileSystem.listStatus(logPath)
    assert(logFiles != null)
    assertFilesExist(logFiles, loggerStopped = false)

    // Verify special files are as expected after stop()
    eventLogger.stop()
    logFiles = fileSystem.listStatus(logPath)
    assertFilesExist(logFiles, loggerStopped = true)
  }

  /**
   * Test whether EventLoggingListener correctly parses the correct information from the logs.
   *
   * This includes whether it returns the correct Spark version, compression codec (if any),
   * and the application's completion status.
   */
  private def testParsingLogInfo(
      logDirPath: Option[String] = None,
      compressionCodec: Option[String] = None) {

    def assertInfoCorrect(info: EventLoggingInfo, loggerStopped: Boolean) {
      assert(info.logPaths.size > 0)
      assert(info.sparkVersion === SparkContext.SPARK_VERSION)
      assert(info.compressionCodec.isDefined === compressionCodec.isDefined)
      info.compressionCodec.foreach { codec =>
        assert(compressionCodec.isDefined)
        val expectedCodec = compressionCodec.get.split('.').last
        assert(codec.getClass.getSimpleName === expectedCodec)
      }
      assert(info.applicationComplete === loggerStopped)
    }

    // Verify that all information is correctly parsed before stop()
    val conf = getLoggingConf(logDirPath, compressionCodec)
    val eventLogger = new EventLoggingListener("test", conf)
    eventLogger.start()
    val fileSystem = Utils.getHadoopFileSystem(eventLogger.logDir)
    var eventLoggingInfo = EventLoggingListener.parseLoggingInfo(eventLogger.logDir, fileSystem)
    assertInfoCorrect(eventLoggingInfo, loggerStopped = false)

    // Verify that all information is correctly parsed after stop()
    eventLogger.stop()
    eventLoggingInfo = EventLoggingListener.parseLoggingInfo(eventLogger.logDir, fileSystem)
    assertInfoCorrect(eventLoggingInfo, loggerStopped = true)
  }

  /**
   * Test basic event logging functionality.
   *
   * This creates two simple events, posts them to the EventLoggingListener, and verifies that
   * exactly these two events are logged in the expected file.
   */
  private def testEventLogging(
      logDirPath: Option[String] = None,
      compressionCodec: Option[String] = None) {
    val conf = getLoggingConf(logDirPath, compressionCodec)
    val eventLogger = new EventLoggingListener("test", conf)
    val listenerBus = new LiveListenerBus
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", 125L, "Mickey")
    val applicationEnd = SparkListenerApplicationEnd(1000L)

    // A comprehensive test on JSON de/serialization of all events is in JsonProtocolSuite
    eventLogger.start()
    listenerBus.start()
    listenerBus.addListener(eventLogger)
    listenerBus.postToAll(applicationStart)
    listenerBus.postToAll(applicationEnd)

    // Verify file contains exactly the two events logged
    val fileSystem = Utils.getHadoopFileSystem(eventLogger.logDir)
    val eventLoggingInfo = EventLoggingListener.parseLoggingInfo(eventLogger.logDir, fileSystem)
    assert(eventLoggingInfo.logPaths.size > 0)
    val fileStream = {
      val stream = fileSystem.open(eventLoggingInfo.logPaths.head)
      eventLoggingInfo.compressionCodec.map { codec =>
        codec.compressedInputStream(stream)
      }.getOrElse(stream)
    }
    val lines = Source.fromInputStream(fileStream).getLines().toSeq
    assert(lines.size === 2)
    assert(lines(0).contains("SparkListenerApplicationStart"))
    assert(lines(1).contains("SparkListenerApplicationEnd"))
    assert(JsonProtocol.sparkEventFromJson(parse(lines(0))) === applicationStart)
    assert(JsonProtocol.sparkEventFromJson(parse(lines(1))) === applicationEnd)
    eventLogger.stop()
  }

  /**
   * Test end-to-end event logging functionality in an application.
   */
  private def testApplicationEventLogging(
      logDirPath: Option[String] = None,
      compressionCodec: Option[String] = None) {

    val conf = getLoggingConf(logDirPath, compressionCodec)
    val sc = new SparkContext("local", "test", conf)
    assert(sc.eventLogger.isDefined)
    val eventLogger = sc.eventLogger.get
    val fileSystem = Utils.getHadoopFileSystem(eventLogger.logDir)
    val expectedLogDir = logDirPath.getOrElse(EventLoggingListener.DEFAULT_LOG_DIR)
    assert(eventLogger.logDir.startsWith(expectedLogDir))

    // Assert all specified events are found in the event log
    def assertEventExists(events: Seq[String]) {
      val eventLoggingInfo = EventLoggingListener.parseLoggingInfo(eventLogger.logDir, fileSystem)
      val logPath = eventLoggingInfo.logPaths.head
      val fileStream = {
        val stream = fileSystem.open(logPath)
        eventLoggingInfo.compressionCodec.map { codec =>
          codec.compressedInputStream(stream)
        }.getOrElse(stream)
      }
      val lines = Source.fromInputStream(fileStream).getLines()
      val eventSet = mutable.Set(events: _*)
      lines.foreach { line =>
        eventSet.foreach { event =>
          if (line.contains(event) &&
              JsonProtocol.sparkEventFromJson(parse(line)).getClass.getSimpleName == event) {
            eventSet.remove(event)
          }
        }
      }
      assert(eventSet.isEmpty, "The following events are missing: " + eventSet.toSeq)
    }

    // SparkListenerEvents are posted in a separate thread
    class AssertEventListener extends SparkListener {
      var jobStarted = false
      var jobEnded = false
      var appEnded = false

      override def onJobStart(jobStart: SparkListenerJobStart) {
        assertEventExists(Seq[String](
          "SparkListenerApplicationStart",
          "SparkListenerBlockManagerAdded",
          "SparkListenerEnvironmentUpdate"
        ))
        jobStarted = true
      }

      override def onJobEnd(jobEnd: SparkListenerJobEnd) {
        assertEventExists(Seq[String](
          "SparkListenerJobStart",
          "SparkListenerJobEnd",
          "SparkListenerStageSubmitted",
          "SparkListenerStageCompleted",
          "SparkListenerTaskStart",
          "SparkListenerTaskEnd"
        ))
        jobEnded = true
      }

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
        assertEventExists(Seq[String]("SparkListenerApplicationEnd"))
        appEnded = true
      }
    }
    val assertEventListener = new AssertEventListener
    sc.addSparkListener(assertEventListener)

    // Trigger callbacks
    sc.parallelize(1 to 10000).count()
    sc.stop()
    assert(assertEventListener.jobStarted, "JobStart callback not invoked!")
    assert(assertEventListener.jobEnded, "JobEnd callback not invoked!")
    assert(assertEventListener.appEnded, "ApplicationEnd callback not invoked!")
  }

  /* Helper methods for validating state of the special files. */

  private def eventLogsExist(logFiles: Array[FileStatus]): Boolean = {
    logFiles.map(_.getPath.getName).exists(EventLoggingListener.isEventLogFile)
  }

  private def sparkVersionExists(logFiles: Array[FileStatus]): Boolean = {
    logFiles.map(_.getPath.getName).exists(EventLoggingListener.isSparkVersionFile)
  }

  private def compressionCodecExists(logFiles: Array[FileStatus]): Boolean = {
    logFiles.map(_.getPath.getName).exists(EventLoggingListener.isCompressionCodecFile)
  }

  private def applicationCompleteExists(logFiles: Array[FileStatus]): Boolean = {
    logFiles.map(_.getPath.getName).exists(EventLoggingListener.isApplicationCompleteFile)
  }

  private def assertSparkVersionIsValid(logFiles: Array[FileStatus]) {
    val file = logFiles.map(_.getPath.getName).find(EventLoggingListener.isSparkVersionFile)
    assert(file.isDefined)
    assert(EventLoggingListener.parseSparkVersion(file.get) === SparkContext.SPARK_VERSION)
  }

  private def assertCompressionCodecIsValid(logFiles: Array[FileStatus], compressionCodec: String) {
    val file = logFiles.map(_.getPath.getName).find(EventLoggingListener.isCompressionCodecFile)
    assert(file.isDefined)
    assert(EventLoggingListener.parseCompressionCodec(file.get) === compressionCodec)
  }

  /** Get a SparkConf with event logging enabled. */
  private def getLoggingConf(
      logDir: Option[String] = None,
      compressionCodec: Option[String] = None) = {
    val conf = new SparkConf
    conf.set("spark.eventLog.enabled", "true")
    logDir.foreach { dir =>
      conf.set("spark.eventLog.dir", dir)
    }
    compressionCodec.foreach { codec =>
      conf.set("spark.eventLog.compress", "true")
      conf.set("spark.io.compression.codec", codec)
    }
    conf
  }

}