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
import scala.collection.mutable.Set
import scala.io.Source

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.mockito.Mockito
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io._
import org.apache.spark.metrics.{ExecutorMetricType, MetricsSystem}
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
    val hadoopconf = SparkHadoopUtil.get.newConfiguration(new SparkConf())
    val eventLogger = new EventLoggingListener("test", None, testDirPath.toUri(), conf)
    val envDetails = SparkEnv.environmentDetails(conf, hadoopconf, "FIFO", Seq.empty, Seq.empty)
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
    testEventLogging(extraConf = Map(EVENT_LOG_OVERWRITE.key -> "true"))
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
    testStageExecutorMetricsEventLogging()
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
   * Test stage executor metrics logging functionality. This checks that peak
   * values from SparkListenerExecutorMetricsUpdate events during a stage are
   * logged in a StageExecutorMetrics event for each executor at stage completion.
   */
  private def testStageExecutorMetricsEventLogging() {
    val conf = getLoggingConf(testDirPath, None)
    val logName = "stageExecutorMetrics-test"
    val eventLogger = new EventLoggingListener(logName, None, testDirPath.toUri(), conf)
    val listenerBus = new LiveListenerBus(conf)

    // Events to post.
    val events = Array(
      SparkListenerApplicationStart("executionMetrics", None,
        1L, "update", None),
      createExecutorAddedEvent(1),
      createExecutorAddedEvent(2),
      createStageSubmittedEvent(0),
      // receive 3 metric updates from each executor with just stage 0 running,
      // with different peak updates for each executor
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(Array(4000L, 50L, 20L, 0L, 40L, 0L, 60L, 0L, 70L, 20L, 7500L, 3500L,
          6500L, 2500L, 5500L, 1500L, 10L, 90L, 2L, 20L))),
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(Array(1500L, 50L, 20L, 0L, 0L, 0L, 20L, 0L, 70L, 0L, 8500L, 3500L,
          7500L, 2500L, 6500L, 1500L, 10L, 90L, 2L, 20L))),
      // exec 1: new stage 0 peaks for metrics at indexes:  2, 4, 6
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(Array(4000L, 50L, 50L, 0L, 50L, 0L, 100L, 0L, 70L, 20L, 8000L, 4000L,
          7000L, 3000L, 6000L, 2000L, 10L, 90L, 2L, 20L))),
      // exec 2: new stage 0 peaks for metrics at indexes: 0, 4, 6
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(Array(2000L, 50L, 10L, 0L, 10L, 0L, 30L, 0L, 70L, 0L, 9000L, 4000L,
          8000L, 3000L, 7000L, 2000L, 10L, 90L, 2L, 20L))),
      // exec 1: new stage 0 peaks for metrics at indexes: 5, 7
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(Array(2000L, 40L, 50L, 0L, 40L, 10L, 90L, 10L, 50L, 0L, 8000L, 3500L,
          7000L, 2500L, 6000L, 1500L, 10L, 90L, 2L, 20L))),
      // exec 2: new stage 0 peaks for metrics at indexes: 0, 5, 6, 7, 8
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(Array(3500L, 50L, 15L, 0L, 10L, 10L, 35L, 10L, 80L, 0L, 8500L, 3500L,
          7500L, 2500L, 6500L, 1500L, 10L, 90L, 2L, 20L))),
      // now start stage 1, one more metric update for each executor, and new
      // peaks for some stage 1 metrics (as listed), initialize stage 1 peaks
      createStageSubmittedEvent(1),
      // exec 1: new stage 0 peaks for metrics at indexes: 0, 3, 7; initialize stage 1 peaks
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(Array(5000L, 30L, 50L, 20L, 30L, 10L, 80L, 30L, 50L,
          0L, 5000L, 3000L, 4000L, 2000L, 3000L, 1000L, 10L, 90L, 2L, 20L))),
      // exec 2: new stage 0 peaks for metrics at indexes: 0, 1, 3, 6, 7, 9;
      // initialize stage 1 peaks
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(Array(7000L, 70L, 50L, 20L, 0L, 10L, 50L, 30L, 10L,
          40L, 8000L, 4000L, 7000L, 3000L, 6000L, 2000L, 10L, 90L, 2L, 20L))),
      // complete stage 0, and 3 more updates for each executor with just
      // stage 1 running
      createStageCompletedEvent(0),
      // exec 1: new stage 1 peaks for metrics at indexes: 0, 1, 3
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(Array(6000L, 70L, 20L, 30L, 10L, 0L, 30L, 30L, 30L, 0L, 5000L, 3000L,
          4000L, 2000L, 3000L, 1000L, 10L, 90L, 2L, 20L))),
      // exec 2: new stage 1 peaks for metrics at indexes: 3, 4, 7, 8
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(Array(5500L, 30L, 20L, 40L, 10L, 0L, 30L, 40L, 40L,
          20L, 8000L, 5000L, 7000L, 4000L, 6000L, 3000L, 10L, 90L, 2L, 20L))),
      // exec 1: new stage 1 peaks for metrics at indexes: 0, 4, 5, 7
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(Array(7000L, 70L, 5L, 25L, 60L, 30L, 65L, 55L, 30L, 0L, 3000L, 2500L,
          2000L, 1500L, 1000L, 500L, 10L, 90L, 2L, 20L))),
      // exec 2: new stage 1 peak for metrics at index: 7
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(Array(5500L, 40L, 25L, 30L, 10L, 30L, 35L, 60L, 0L,
          20L, 7000L, 3000L, 6000L, 2000L, 5000L, 1000L, 10L, 90L, 2L, 20L))),
      // exec 1: no new stage 1 peaks
      createExecutorMetricsUpdateEvent(1,
        new ExecutorMetrics(Array(5500L, 70L, 15L, 20L, 55L, 20L, 70L, 40L, 20L,
          0L, 4000L, 2500L, 3000L, 1500L, 2000L, 500L, 10L, 90L, 2L, 20L))),
      createExecutorRemovedEvent(1),
      // exec 2: new stage 1 peak for metrics at index: 6
      createExecutorMetricsUpdateEvent(2,
        new ExecutorMetrics(Array(4000L, 20L, 25L, 30L, 10L, 30L, 35L, 60L, 0L, 0L, 7000L,
          4000L, 6000L, 3000L, 5000L, 2000L, 10L, 90L, 2L, 20L))),
      createStageCompletedEvent(1),
      SparkListenerApplicationEnd(1000L))

    // play the events for the event logger
    eventLogger.start()
    listenerBus.start(Mockito.mock(classOf[SparkContext]), Mockito.mock(classOf[MetricsSystem]))
    listenerBus.addToEventLogQueue(eventLogger)
    events.foreach(event => listenerBus.post(event))
    listenerBus.stop()
    eventLogger.stop()

    // expected StageExecutorMetrics, for the given stage id and executor id
    val expectedMetricsEvents: Map[(Int, String), SparkListenerStageExecutorMetrics] =
    Map(
      ((0, "1"),
        new SparkListenerStageExecutorMetrics("1", 0, 0,
          new ExecutorMetrics(Array(5000L, 50L, 50L, 20L, 50L, 10L, 100L, 30L,
            70L, 20L, 8000L, 4000L, 7000L, 3000L, 6000L, 2000L, 10L, 90L, 2L, 20L)))),
      ((0, "2"),
        new SparkListenerStageExecutorMetrics("2", 0, 0,
          new ExecutorMetrics(Array(7000L, 70L, 50L, 20L, 10L, 10L, 50L, 30L,
            80L, 40L, 9000L, 4000L, 8000L, 3000L, 7000L, 2000L, 10L, 90L, 2L, 20L)))),
      ((1, "1"),
        new SparkListenerStageExecutorMetrics("1", 1, 0,
          new ExecutorMetrics(Array(7000L, 70L, 50L, 30L, 60L, 30L, 80L, 55L,
            50L, 0L, 5000L, 3000L, 4000L, 2000L, 3000L, 1000L, 10L, 90L, 2L, 20L)))),
      ((1, "2"),
        new SparkListenerStageExecutorMetrics("2", 1, 0,
          new ExecutorMetrics(Array(7000L, 70L, 50L, 40L, 10L, 30L, 50L, 60L,
            40L, 40L, 8000L, 5000L, 7000L, 4000L, 6000L, 3000L, 10L, 90L, 2L, 20L)))))
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
            val execIds = Set[String]()
            (1 to 2).foreach { _ =>
              val execId = checkStageExecutorMetrics(lines(logIdx),
                stageCompleted.stageInfo.stageId, expectedMetricsEvents)
              execIds += execId
              logIdx += 1
            }
            assert(execIds.size == 2) // check that each executor was logged
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
    SparkListenerExecutorAdded(0L, executorId.toString,
      new ExecutorInfo("host1", 1, Map.empty, Map.empty))
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
   * Check that the Spark history log line is an StageExecutorMetrics event, and matches the
   * expected value for the stage and executor.
   *
   * @param line the Spark history log line
   * @param stageId the stage ID the ExecutorMetricsUpdate is associated with
   * @param expectedEvents map of expected ExecutorMetricsUpdate events, for (stageId, executorId)
   */
  private def checkStageExecutorMetrics(
      line: String,
      stageId: Int,
      expectedEvents: Map[(Int, String), SparkListenerStageExecutorMetrics]): String = {
    JsonProtocol.sparkEventFromJson(parse(line)) match {
      case executorMetrics: SparkListenerStageExecutorMetrics =>
          expectedEvents.get((stageId, executorMetrics.execId)) match {
            case Some(expectedMetrics) =>
              assert(executorMetrics.execId === expectedMetrics.execId)
              assert(executorMetrics.stageId === expectedMetrics.stageId)
              assert(executorMetrics.stageAttemptId === expectedMetrics.stageAttemptId)
              ExecutorMetricType.metricToOffset.foreach { metric =>
                assert(executorMetrics.executorMetrics.getMetricValue(metric._1) ===
                  expectedMetrics.executorMetrics.getMetricValue(metric._1))
              }
            case None =>
              assert(false)
        }
        executorMetrics.execId
      case _ =>
        fail("expecting SparkListenerStageExecutorMetrics")
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
    conf.set(EVENT_LOG_ENABLED, true)
    conf.set(EVENT_LOG_BLOCK_UPDATES, true)
    conf.set(EVENT_LOG_TESTING, true)
    conf.set(EVENT_LOG_DIR, logDir.toString)
    compressionCodec.foreach { codec =>
      conf.set(EVENT_LOG_COMPRESS, true)
      conf.set(IO_COMPRESSION_CODEC, codec)
    }
    conf.set(EVENT_LOG_STAGE_EXECUTOR_METRICS, true)
    conf
  }

  def getUniqueApplicationId: String = "test-" + System.currentTimeMillis
}
