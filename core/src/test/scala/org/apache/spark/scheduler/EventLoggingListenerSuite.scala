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

import java.io.{File, FileOutputStream, InputStream, IOException}

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.mockito.Mockito
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.io._
import org.apache.spark.metrics.{MetricGetter, MetricsSystem}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.util.{JsonProtocol, Utils}


/**
 * Test whether EventLoggingListener logs events properly.
 *
 * This tests whether EventLoggingListener actually log files with expected name patterns while
 * logging events, whether the parsing of the file names is correct, and whether the logged events
 * can be read and deserialized into actual SparkListenerEvents.
 */
class EventLoggingListenerSuite extends SparkFunSuite with LocalSparkContext with BeforeAndAfter
  with Logging {

  import EventLoggingListenerSuite._

  private val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  private var testDir: File = _
  private var testDirPath: Path = _

  before {
    testDir = Utils.createTempDir(namePrefix = s"history log")
    testDir.deleteOnExit()
    testDirPath = new Path(testDir.getAbsolutePath())
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Verify log file exist") {
    // Verify logging directory exists
    val conf = getLoggingConf(testDirPath)
    val eventLogger = new EventLoggingListener("test", None, testDirPath.toUri(), conf)
    eventLogger.start()

    val logPath = new Path(eventLogger.logPath + EventLoggingListener.IN_PROGRESS)
    assert(fileSystem.exists(logPath))
    val logStatus = fileSystem.getFileStatus(logPath)
    assert(!logStatus.isDirectory)

    // Verify log is renamed after stop()
    eventLogger.stop()
    assert(!fileSystem.getFileStatus(new Path(eventLogger.logPath)).isDirectory)
  }

  test("Basic event logging") {
    testEventLogging()
  }

  test("Basic event logging with compression") {
    CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codec =>
      testEventLogging(compressionCodec = Some(CompressionCodec.getShortName(codec)))
    }
  }

  test("End-to-end event logging") {
    testApplicationEventLogging()
  }

  test("End-to-end event logging with compression") {
    CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codec =>
      testApplicationEventLogging(compressionCodec = Some(CompressionCodec.getShortName(codec)))
    }
  }

  test("Event logging with password redaction") {
    val key = "spark.executorEnv.HADOOP_CREDSTORE_PASSWORD"
    val secretPassword = "secret_password"
    val conf = getLoggingConf(testDirPath, None)
      .set(key, secretPassword)
    val eventLogger = new EventLoggingListener("test", None, testDirPath.toUri(), conf)
    val envDetails = SparkEnv.environmentDetails(conf, "FIFO", Seq.empty, Seq.empty)
    val event = SparkListenerEnvironmentUpdate(envDetails)
    val redactedProps = eventLogger.redactEvent(event).environmentDetails("Spark Properties").toMap
    assert(redactedProps(key) == "*********(redacted)")
  }

  test("Log overwriting") {
    val logUri = EventLoggingListener.getLogPath(testDir.toURI, "test", None)
    val logPath = new Path(logUri).toUri.getPath
    // Create file before writing the event log
    new FileOutputStream(new File(logPath)).close()
    // Expected IOException, since we haven't enabled log overwrite.
    intercept[IOException] { testEventLogging() }
    // Try again, but enable overwriting.
    testEventLogging(extraConf = Map("spark.eventLog.overwrite" -> "true"))
  }

  test("Event log name") {
    val baseDirUri = Utils.resolveURI("/base-dir")
    // without compression
    assert(s"${baseDirUri.toString}/app1" === EventLoggingListener.getLogPath(
      baseDirUri, "app1", None))
    // with compression
    assert(s"${baseDirUri.toString}/app1.lzf" ===
      EventLoggingListener.getLogPath(baseDirUri, "app1", None, Some("lzf")))
    // illegal characters in app ID
    assert(s"${baseDirUri.toString}/a-fine-mind_dollar_bills__1" ===
      EventLoggingListener.getLogPath(baseDirUri,
        "a fine:mind$dollar{bills}.1", None))
    // illegal characters in app ID with compression
    assert(s"${baseDirUri.toString}/a-fine-mind_dollar_bills__1.lz4" ===
      EventLoggingListener.getLogPath(baseDirUri,
        "a fine:mind$dollar{bills}.1", None, Some("lz4")))
  }

  test("Executor metrics update") {
    testExecutorMetricsUpdateEventLogging()
  }

  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  import EventLoggingListenerSuite._

  /**
   * Test basic event logging functionality.
   *
   * This creates two simple events, posts them to the EventLoggingListener, and verifies that
   * exactly these two events are logged in the expected file.
   */
  private def testEventLogging(
      compressionCodec: Option[String] = None,
      extraConf: Map[String, String] = Map()) {
    val conf = getLoggingConf(testDirPath, compressionCodec)
    extraConf.foreach { case (k, v) => conf.set(k, v) }
    val logName = compressionCodec.map("test-" + _).getOrElse("test")
    val eventLogger = new EventLoggingListener(logName, None, testDirPath.toUri(), conf)
    val listenerBus = new LiveListenerBus(conf)
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", None,
      125L, "Mickey", None)
    val applicationEnd = SparkListenerApplicationEnd(1000L)

    // A comprehensive test on JSON de/serialization of all events is in JsonProtocolSuite
    eventLogger.start()
    listenerBus.start(Mockito.mock(classOf[SparkContext]), Mockito.mock(classOf[MetricsSystem]))
    listenerBus.addToEventLogQueue(eventLogger)
    listenerBus.post(applicationStart)
    listenerBus.post(applicationEnd)
    listenerBus.stop()
    eventLogger.stop()

    // Verify file contains exactly the two events logged
    val logData = EventLoggingListener.openEventLog(new Path(eventLogger.logPath), fileSystem)
    try {
      val lines = readLines(logData)
      val logStart = SparkListenerLogStart(SPARK_VERSION)
      assert(lines.size === 3)
      assert(lines(0).contains("SparkListenerLogStart"))
      assert(lines(1).contains("SparkListenerApplicationStart"))
      assert(lines(2).contains("SparkListenerApplicationEnd"))
      assert(JsonProtocol.sparkEventFromJson(parse(lines(0))) === logStart)
      assert(JsonProtocol.sparkEventFromJson(parse(lines(1))) === applicationStart)
      assert(JsonProtocol.sparkEventFromJson(parse(lines(2))) === applicationEnd)
    } finally {
      logData.close()
    }
  }

  /**
   * Test end-to-end event logging functionality in an application.
   * This runs a simple Spark job and asserts that the expected events are logged when expected.
   */
  private def testApplicationEventLogging(compressionCodec: Option[String] = None) {
    // Set defaultFS to something that would cause an exception, to make sure we don't run
    // into SPARK-6688.
    val conf = getLoggingConf(testDirPath, compressionCodec)
      .set("spark.hadoop.fs.defaultFS", "unsupported://example.com")
    sc = new SparkContext("local-cluster[2,2,1024]", "test", conf)
    assert(sc.eventLogger.isDefined)
    val eventLogger = sc.eventLogger.get
    val eventLogPath = eventLogger.logPath
    val expectedLogDir = testDir.toURI()
    assert(eventLogPath === EventLoggingListener.getLogPath(
      expectedLogDir, sc.applicationId, None, compressionCodec.map(CompressionCodec.getShortName)))

    // Begin listening for events that trigger asserts
    val eventExistenceListener = new EventExistenceListener(eventLogger)
    sc.addSparkListener(eventExistenceListener)

    // Trigger asserts for whether the expected events are actually logged
    sc.parallelize(1 to 10000).count()
    sc.stop()

    // Ensure all asserts have actually been triggered
    eventExistenceListener.assertAllCallbacksInvoked()

    // Make sure expected events exist in the log file.
    val logData = EventLoggingListener.openEventLog(new Path(eventLogger.logPath), fileSystem)
    val eventSet = mutable.Set(
      SparkListenerApplicationStart,
      SparkListenerBlockManagerAdded,
      SparkListenerExecutorAdded,
      SparkListenerEnvironmentUpdate,
      SparkListenerJobStart,
      SparkListenerJobEnd,
      SparkListenerStageSubmitted,
      SparkListenerStageCompleted,
      SparkListenerTaskStart,
      SparkListenerTaskEnd,
      SparkListenerBlockUpdated,
      SparkListenerApplicationEnd).map(Utils.getFormattedClassName)
    Utils.tryWithSafeFinally {
      val logStart = SparkListenerLogStart(SPARK_VERSION)
      val lines = readLines(logData)
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
      assert(JsonProtocol.sparkEventFromJson(parse(lines(0))) === logStart)
      assert(eventSet.isEmpty, "The following events are missing: " + eventSet.toSeq)
    } {
      logData.close()
    }
  }

  /**
   * Test executor metrics update logging functionality. This checks that a
   * SparkListenerExecutorMetricsUpdate event is added to the Spark history
   * log if one of the executor metrics is larger than any previously
   * recorded value for the metric, per executor per stage. The task metrics
   * should not be added.
   */
  private def testExecutorMetricsUpdateEventLogging() {
    val conf = getLoggingConf(testDirPath, None)
    val logName = "executorMetricsUpdated-test"
    val eventLogger = new EventLoggingListener(logName, None, testDirPath.toUri(), conf)
    val listenerBus = new LiveListenerBus(conf)

    // expected ExecutorMetricsUpdate, for the given stage id and executor id
    val expectedMetricsEvents: Map[(Int, String), SparkListenerExecutorMetricsUpdate] =
      Map(
        ((0, "1"),
          createExecutorMetricsUpdateEvent(1,
            new ExecutorMetrics(-1L,
              Array(5000L, 50L, 50L, 20L, 50L, 10L, 100L, 30L, 70L, 20L)))),
        ((0, "2"),
          createExecutorMetricsUpdateEvent(2,
            new ExecutorMetrics(-1L,
              Array(7000L, 70L, 50L, 20L, 10L, 10L, 50L, 30L, 80L, 40L)))),
        ((1, "1"),
          createExecutorMetricsUpdateEvent(1,
            new ExecutorMetrics(-1L,
              Array(7000L, 70L, 50L, 30L, 60L, 30L, 80L, 55L, 50L, 0L)))),
        ((1, "2"),
          createExecutorMetricsUpdateEvent(2,
            new ExecutorMetrics(-1L,
              Array(7000L, 70L, 50L, 40L, 10L, 30L, 50L, 60L, 40L, 40L)))))

    // Events to post.
    val events = Array(
      SparkListenerApplicationStart("executionMetrics", None,
        1L, "update", None),
      createExecutorAddedEvent(1),
      createExecutorAddedEvent(2),
      createStageSubmittedEvent(0),
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(10L,
          Array(4000L, 50L, 20L, 0L, 40L, 0L, 60L, 0L, 70L, 20L))),
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(10L,
          Array(1500L, 50L, 20L, 0L, 0L, 0L, 20L, 0L, 70L, 0L))),
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(15L,
          Array(4000L, 50L, 50L, 0L, 50L, 0L, 100L, 0L, 70L, 20L))),
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(15L,
          Array(2000L, 50L, 10L, 0L, 10L, 0L, 30L, 0L, 70L, 0L))),
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(20L,
          Array(2000L, 40L, 50L, 0L, 40L, 10L, 90L, 10L, 50L, 0L))),
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(20L,
          Array(3500L, 50L, 15L, 0L, 10L, 10L, 35L, 10L, 80L, 0L))),
      createStageSubmittedEvent(1),
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(25L,
          Array(5000L, 30L, 50L, 20L, 30L, 10L, 80L, 30L, 50L, 0L))),
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(25L,
          Array(7000L, 70L, 50L, 20L, 0L, 10L, 50L, 30L, 10L, 40L))),
      createStageCompletedEvent(0),
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(30L,
          Array(6000L, 70L, 20L, 30L, 10L, 0L, 30L, 30L, 30L, 0L))),
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(30L,
          Array(5500L, 30L, 20L, 40L, 10L, 0L, 30L, 40L, 40L, 20L))),
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(35L,
          Array(7000L, 70L, 5L, 25L, 60L, 30L, 65L, 55L, 30L, 0L))),
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(35L,
          Array(5500L, 40L, 25L, 30L, 10L, 30L, 35L, 60L, 0L, 20L))),
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(40L,
          Array(5500L, 70L, 15L, 20L, 55L, 20L, 70L, 40L, 20L, 0L))),
      createExecutorRemovedEvent(1),
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(40L,
          Array(4000L, 20L, 25L, 30L, 10L, 30L, 35L, 60L, 0L, 0L))),
      createStageCompletedEvent(1),
      SparkListenerApplicationEnd(1000L))

    // play the events for the event logger
    eventLogger.start()
    listenerBus.start(Mockito.mock(classOf[SparkContext]), Mockito.mock(classOf[MetricsSystem]))
    listenerBus.addToEventLogQueue(eventLogger)
    events.foreach(event => listenerBus.post(event))
    listenerBus.stop()
    eventLogger.stop()

    // Verify the log file contains the expected events.
    // Posted events should be logged, except for ExecutorMetricsUpdate events -- these
    // are consolidated, and the peak values for each stage are logged at stage end.
    val logData = EventLoggingListener.openEventLog(new Path(eventLogger.logPath), fileSystem)
    try {
      val lines = readLines(logData)
      val logStart = SparkListenerLogStart(SPARK_VERSION)
      assert(lines.size === 14)
      assert(lines(0).contains("SparkListenerLogStart"))
      assert(lines(1).contains("SparkListenerApplicationStart"))
      assert(JsonProtocol.sparkEventFromJson(parse(lines(0))) === logStart)
      var logIdx = 1
      events.foreach {event =>
        event match {
          case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
          case stageCompleted: SparkListenerStageCompleted =>
            (1 to 2).foreach { _ =>
              checkExecutorMetricsUpdate(lines(logIdx), stageCompleted.stageInfo.stageId,
                expectedMetricsEvents)
              logIdx += 1
            }
            checkEvent(lines(logIdx), event)
            logIdx += 1
        case _ =>
          checkEvent(lines(logIdx), event)
          logIdx += 1
        }
      }
    } finally {
      logData.close()
    }
  }

  private def createStageSubmittedEvent(stageId: Int) = {
    SparkListenerStageSubmitted(new StageInfo(stageId, 0, stageId.toString, 0,
      Seq.empty, Seq.empty, "details"))
  }

  private def createStageCompletedEvent(stageId: Int) = {
    SparkListenerStageCompleted(new StageInfo(stageId, 0, stageId.toString, 0,
      Seq.empty, Seq.empty, "details"))
  }

  private def createExecutorAddedEvent(executorId: Int) = {
    SparkListenerExecutorAdded(0L, executorId.toString, new ExecutorInfo("host1", 1, Map.empty))
  }

  private def createExecutorRemovedEvent(executorId: Int) = {
    SparkListenerExecutorRemoved(0L, executorId.toString, "test")
  }

  private def createExecutorMetricsUpdateEvent(
      executorId: Int,
      executorMetrics: ExecutorMetrics): SparkListenerExecutorMetricsUpdate = {
    val taskMetrics = TaskMetrics.empty
    taskMetrics.incDiskBytesSpilled(111)
    taskMetrics.incMemoryBytesSpilled(222)
    val accum = Array((333L, 1, 1, taskMetrics.accumulators().map(AccumulatorSuite.makeInfo)))
    SparkListenerExecutorMetricsUpdate(executorId.toString, accum, Some(executorMetrics))
  }

  /** Check that the two ExecutorMetrics match */
  private def checkExecutorMetrics(
      executorMetrics1: Option[ExecutorMetrics],
      executorMetrics2: Option[ExecutorMetrics]) = {
    (executorMetrics1, executorMetrics2) match {
      case (Some(e1), Some(e2)) =>
        assert(e1.timestamp === e2.timestamp)
        (0 until MetricGetter.values.length).foreach { idx =>
          assert(e1.metrics(idx) === e2.metrics(idx))
        }
      case (None, None) =>
      case _ =>
        assert(false)
    }
  }

  /** Check that the Spark history log line matches the expected event. */
  private def checkEvent(line: String, event: SparkListenerEvent): Unit = {
    assert(line.contains(event.getClass.toString.split("\\.").last))
    val parsed = JsonProtocol.sparkEventFromJson(parse(line))
    assert(parsed.getClass === event.getClass)
    (event, parsed) match {
      case (expected: SparkListenerStageSubmitted, actual: SparkListenerStageSubmitted) =>
        // accumulables can be different, so only check the stage Id
        assert(expected.stageInfo.stageId == actual.stageInfo.stageId)
      case (expected: SparkListenerStageCompleted, actual: SparkListenerStageCompleted) =>
        // accumulables can be different, so only check the stage Id
        assert(expected.stageInfo.stageId == actual.stageInfo.stageId)
      case (expected: SparkListenerEvent, actual: SparkListenerEvent) =>
        assert(expected === actual)
    }
  }

  /**
   * Check that the Spark history log line is an ExecutorMetricsUpdate, and matches the expected
   * value for the stage and executor.
   *
   * @param line the Spark history log line
   * @param stageId the stage ID the ExecutorMetricsUpdate is associated with
   * @param expectedEvents map of expected ExecutorMetricsUpdate events, for (stageId, executorId)
   */
  private def checkExecutorMetricsUpdate(
      line: String,
      stageId: Int,
      expectedEvents: Map[(Int, String), SparkListenerExecutorMetricsUpdate]): Unit = {
    JsonProtocol.sparkEventFromJson(parse(line)) match {
      case executorMetrics: SparkListenerExecutorMetricsUpdate =>
          expectedEvents.get((stageId, executorMetrics.execId)) match {
            case Some(expectedMetrics) =>
              assert(executorMetrics.accumUpdates.isEmpty)
              checkExecutorMetrics(executorMetrics.executorUpdates, expectedMetrics.executorUpdates)
            case None =>
              assert(false)
        }
      case _ =>
        fail("expecting SparkListenerExecutorMetricsUpdate")
    }
  }

  private def readLines(in: InputStream): Seq[String] = {
    Source.fromInputStream(in).getLines().toSeq
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
  def getLoggingConf(logDir: Path, compressionCodec: Option[String] = None): SparkConf = {
    val conf = new SparkConf
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.logBlockUpdates.enabled", "true")
    conf.set("spark.eventLog.testing", "true")
    conf.set("spark.eventLog.dir", logDir.toString)
    compressionCodec.foreach { codec =>
      conf.set("spark.eventLog.compress", "true")
      conf.set("spark.io.compression.codec", codec)
    }
    conf
  }

  def getUniqueApplicationId: String = "test-" + System.currentTimeMillis
}
