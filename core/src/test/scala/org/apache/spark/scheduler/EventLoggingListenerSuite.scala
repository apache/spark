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

import com.google.common.io.Files
import org.apache.hadoop.fs.{FileStatus, Path}
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{Logging, SparkConf, SparkContext, SPARK_VERSION}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

import java.io.File

/**
 * Test whether EventLoggingListener logs events properly.
 *
 * This tests whether EventLoggingListener actually log files with expected name patterns while
 * logging events, whether the parsing of the file names is correct, and whether the logged events
 * can be read and deserialized into actual SparkListenerEvents.
 */
class EventLoggingListenerSuite extends FunSuite with BeforeAndAfter with Logging {
  private val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  private val allCompressionCodecs = Seq[String](
    "org.apache.spark.io.LZFCompressionCodec",
    "org.apache.spark.io.SnappyCompressionCodec"
  )
  private var testDir: File = _
  private var testDirPath: Path = _

  before {
    testDir = Files.createTempDir()
    testDir.deleteOnExit()
    testDirPath = new Path(testDir.getAbsolutePath())
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Parse names of log files") {
    testParsingFileName()
  }

  test("Verify log file exist") {
    testLogFileExists()
  }

  test("Verify log file names contain compression codec info") {
    allCompressionCodecs.foreach { codec =>
      testLogFileExists(compressionCodec = Some(codec))
    }
  }

  test("Parse event logging info") {
    testParsingLogInfo()
  }

  test("Parse event logging info with compression") {
    allCompressionCodecs.foreach { codec =>
      testParsingLogInfo(compressionCodec = Some(codec))
    }
  }

  test("Basic event logging") {
    testEventLogging()
  }

  test("Basic event logging with compression") {
    allCompressionCodecs.foreach { codec =>
      testEventLogging(compressionCodec = Some(codec))
    }
  }

  test("End-to-end event logging") {
    testApplicationEventLogging()
  }

  test("End-to-end event logging with compression") {
    allCompressionCodecs.foreach { codec =>
      testApplicationEventLogging(compressionCodec = Some(codec))
    }
  }


  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  import EventLoggingListenerSuite._

  /**
   * Test whether names of log files are correctly identified and parsed.
   */
  private def testParsingFileName() {
    var tests = List(
      // Log file name, Spark version, Compression codec, in progress
      ("app1-1234-1.0.inprogress", "1.0", None, true),
      ("app2-1234-0.9.1", "0.9.1", None, false),
      ("app-with-dashes-in-name-1234-1.0.1.inprogress", "1.0.1", None, true),
      ("app3-1234-0.9-org.apache.spark.io.LZFCompressionCodec", "0.9",
        Some(classOf[LZFCompressionCodec]), false),
      ("app-123456-1234-0.8-org.apache.spark.io.SnappyCompressionCodec.inprogress", "0.8",
        Some(classOf[SnappyCompressionCodec]), true)
      )

    tests.foreach({ case (fname, version, codec, inProgress) =>
      logWarning(s"Testing: $fname")
      val info = EventLoggingListener.parseLoggingInfo(new Path(fname))
      assert(info != null)
      assert(version === info.sparkVersion)
      assert(!inProgress === info.applicationComplete)

      val actualCodec = if (!info.compressionCodec.isEmpty) {
          info.compressionCodec.get.getClass()
        } else null
      assert(codec.getOrElse(null) === actualCodec)
      })

    var invalid = List("app1", "app1-1.0", "app1-1234", "app1-abc-1.0",
      "app1-1234-1.0-org.invalid.compression.Codec",
      "app1-1234-1.0.not_in_progress")

    invalid.foreach(fname => assert(EventLoggingListener.parseLoggingInfo(new Path(fname)) == null))
  }

  /**
   * Test whether the log file produced by EventLoggingListener exists and matches the expected
   * name pattern.
   */
  private def testLogFileExists(compressionCodec: Option[String] = None) {
    // Verify logging directory exists
    val conf = getLoggingConf(testDirPath, compressionCodec)
    val eventLogger = new EventLoggingListener("test", conf)
    eventLogger.start()

    val logPath = new Path(eventLogger.logPath + EventLoggingListener.IN_PROGRESS)
    assert(fileSystem.exists(logPath))
    val logStatus = fileSystem.getFileStatus(logPath)
    assert(logStatus.isFile)
    assert(EventLoggingListener.LOG_FILE_NAME_REGEX.pattern.matcher(
      logPath.getName()).matches())

    // Verify log is renamed after stop()
    eventLogger.stop()
    assert(fileSystem.getFileStatus(new Path(eventLogger.logPath)).isFile())
  }

  /**
   * Test whether EventLoggingListener correctly parses the correct information from the logs.
   *
   * This includes whether it returns the correct Spark version, compression codec (if any),
   * and the application's completion status.
   */
  private def testParsingLogInfo(compressionCodec: Option[String] = None) {

    def assertInfoCorrect(info: EventLoggingInfo, loggerStopped: Boolean) {
      assert(info != null)
      assert(info.sparkVersion === SPARK_VERSION)
      assert(info.compressionCodec.isDefined === compressionCodec.isDefined)
      info.compressionCodec.foreach { codec =>
        assert(compressionCodec.isDefined)
        val expectedCodec = compressionCodec.get.split('.').last
        assert(codec.getClass.getSimpleName === expectedCodec)
      }
      assert(info.applicationComplete === loggerStopped)
    }

    // Verify that all information is correctly parsed before stop()
    val conf = getLoggingConf(testDirPath, compressionCodec)
    val eventLogger = new EventLoggingListener("test", conf)
    eventLogger.start()
    var eventLoggingInfo = EventLoggingListener.parseLoggingInfo(
      new Path(eventLogger.logPath + EventLoggingListener.IN_PROGRESS))
    assertInfoCorrect(eventLoggingInfo, loggerStopped = false)

    // Verify that all information is correctly parsed after stop()
    eventLogger.stop()
    eventLoggingInfo = EventLoggingListener.parseLoggingInfo(new Path(eventLogger.logPath))
    assertInfoCorrect(eventLoggingInfo, loggerStopped = true)
  }

  /**
   * Test basic event logging functionality.
   *
   * This creates two simple events, posts them to the EventLoggingListener, and verifies that
   * exactly these two events are logged in the expected file.
   */
  private def testEventLogging(compressionCodec: Option[String] = None) {
    val conf = getLoggingConf(testDirPath, compressionCodec)
    val eventLogger = new EventLoggingListener("test", conf)
    val listenerBus = new LiveListenerBus
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", None,
      125L, "Mickey")
    val applicationEnd = SparkListenerApplicationEnd(1000L)

    // A comprehensive test on JSON de/serialization of all events is in JsonProtocolSuite
    eventLogger.start()
    listenerBus.start()
    listenerBus.addListener(eventLogger)
    listenerBus.postToAll(applicationStart)
    listenerBus.postToAll(applicationEnd)
    eventLogger.stop()

    // Verify file contains exactly the two events logged
    val eventLoggingInfo = EventLoggingListener.parseLoggingInfo(new Path(eventLogger.logPath))
    assert(eventLoggingInfo != null)
    val lines = readFileLines(eventLoggingInfo.path, eventLoggingInfo.compressionCodec)
    assert(lines.size === 2)
    assert(lines(0).contains("SparkListenerApplicationStart"))
    assert(lines(1).contains("SparkListenerApplicationEnd"))
    assert(JsonProtocol.sparkEventFromJson(parse(lines(0))) === applicationStart)
    assert(JsonProtocol.sparkEventFromJson(parse(lines(1))) === applicationEnd)
  }

  /**
   * Test end-to-end event logging functionality in an application.
   * This runs a simple Spark job and asserts that the expected events are logged when expected.
   */
  private def testApplicationEventLogging(compressionCodec: Option[String] = None) {
    val conf = getLoggingConf(testDirPath, compressionCodec)
    val sc = new SparkContext("local", "test", conf)
    assert(sc.eventLogger.isDefined)
    val eventLogger = sc.eventLogger.get
    val expectedLogDir = testDir.getAbsolutePath()
    assert(eventLogger.logPath.startsWith(expectedLogDir + "/"))

    // Begin listening for events that trigger asserts
    val eventExistenceListener = new EventExistenceListener(eventLogger)
    sc.addSparkListener(eventExistenceListener)

    // Trigger asserts for whether the expected events are actually logged
    sc.parallelize(1 to 10000).count()
    sc.stop()

    // Ensure all asserts have actually been triggered
    eventExistenceListener.assertAllCallbacksInvoked()

    // Make sure expected events exist in the log file.
    val eventLoggingInfo = EventLoggingListener.parseLoggingInfo(new Path(eventLogger.logPath))
    val lines = readFileLines(eventLoggingInfo.path, eventLoggingInfo.compressionCodec)
    val eventSet = mutable.Set(
      Utils.getFormattedClassName(SparkListenerApplicationStart),
      Utils.getFormattedClassName(SparkListenerBlockManagerAdded),
      Utils.getFormattedClassName(SparkListenerEnvironmentUpdate),
      Utils.getFormattedClassName(SparkListenerJobStart),
      Utils.getFormattedClassName(SparkListenerJobEnd),
      Utils.getFormattedClassName(SparkListenerStageSubmitted),
      Utils.getFormattedClassName(SparkListenerStageCompleted),
      Utils.getFormattedClassName(SparkListenerTaskStart),
      Utils.getFormattedClassName(SparkListenerTaskEnd),
      Utils.getFormattedClassName(SparkListenerApplicationEnd))
    lines.foreach { line =>
      eventSet.foreach { event =>
        if (line.contains(event)) {
          val parsedEvent = JsonProtocol.sparkEventFromJson(parse(line))
          val eventType = Utils.getFormattedClassName(parsedEvent)
          if (eventType == event) {
            eventSet.remove(event)
          }
        }
      }
    }
    assert(eventSet.isEmpty, "The following events are missing: " + eventSet.toSeq)
  }

  /**
   * Read all lines from the file specified by the given path.
   * If a compression codec is specified, use it to read the file.
   */
  private def readFileLines(
      filePath: Path,
      compressionCodec: Option[CompressionCodec]): Seq[String] = {
    val fstream = fileSystem.open(filePath)
    val cstream =
      compressionCodec.map { codec =>
        codec.compressedInputStream(fstream)
      }.getOrElse(fstream)
    Source.fromInputStream(cstream).getLines().toSeq
  }

  /**
   * A listener that asserts certain events are logged by the given EventLoggingListener.
   * This is necessary because events are posted asynchronously in a different thread.
   */
  private class EventExistenceListener(eventLogger: EventLoggingListener) extends SparkListener {
    var jobStarted = false
    var jobEnded = false
    var appEnded = false

    override def onJobStart(jobStart: SparkListenerJobStart) {
      jobStarted = true
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd) {
      jobEnded = true
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
      appEnded = true
    }

    def assertAllCallbacksInvoked() {
      assert(jobStarted, "JobStart callback not invoked!")
      assert(jobEnded, "JobEnd callback not invoked!")
      assert(appEnded, "ApplicationEnd callback not invoked!")
    }
  }

}


object EventLoggingListenerSuite {

  /** Get a SparkConf with event logging enabled. */
  def getLoggingConf(logDir: Path, compressionCodec: Option[String] = None) = {
    val conf = new SparkConf
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.testing", "true")
    conf.set("spark.eventLog.dir", logDir.toString)
    compressionCodec.foreach { codec =>
      conf.set("spark.eventLog.compress", "true")
      conf.set("spark.io.compression.codec", codec)
    }
    conf
  }
}
