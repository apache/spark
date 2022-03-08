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

import java.io.{File, InputStream}
import java.util.{Arrays, Properties}

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.Set
import scala.io.{Codec, Source}

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.mockito.Mockito
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.{EventLogFileReader, SingleEventLogFileWriter}
import org.apache.spark.deploy.history.EventLogTestHelper._
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{EVENT_LOG_DIR, EVENT_LOG_ENABLED}
import org.apache.spark.io._
import org.apache.spark.metrics.{ExecutorMetricType, MetricsSystem}
import org.apache.spark.resource.ResourceProfile
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
    val envDetails = SparkEnv.environmentDetails(
      conf, hadoopconf, "FIFO", Seq.empty, Seq.empty, Seq.empty)
    val event = SparkListenerEnvironmentUpdate(envDetails)
    val redactedProps = EventLoggingListener
      .redactEvent(conf, event).environmentDetails("Spark Properties").toMap
    assert(redactedProps(key) == "*********(redacted)")
  }

  test("Spark-33504 sensitive attributes redaction in properties") {
    val (secretKey, secretPassword) = ("spark.executorEnv.HADOOP_CREDSTORE_PASSWORD",
      "secret_password")
    val (customKey, customValue) = ("parse_token", "secret_password")

    val conf = getLoggingConf(testDirPath, None).set(secretKey, secretPassword)

    val properties = new Properties()
    properties.setProperty(secretKey, secretPassword)
    properties.setProperty(customKey, customValue)

    val logName = "properties-reaction-test"
    val eventLogger = new EventLoggingListener(logName, None, testDirPath.toUri(), conf)
    val listenerBus = new LiveListenerBus(conf)

    val stageId = 1
    val jobId = 1
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0,
      Seq.empty, Seq.empty, "details",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)

    val events = Array(SparkListenerStageSubmitted(stageInfo, properties),
      SparkListenerJobStart(jobId, 0, Seq(stageInfo), properties))

    eventLogger.start()
    listenerBus.start(Mockito.mock(classOf[SparkContext]), Mockito.mock(classOf[MetricsSystem]))
    listenerBus.addToEventLogQueue(eventLogger)
    events.foreach(event => listenerBus.post(event))
    listenerBus.stop()
    eventLogger.stop()

    val logData = EventLogFileReader.openEventLog(new Path(eventLogger.logWriter.logPath),
      fileSystem)
    try {
      val lines = readLines(logData)
      val logStart = SparkListenerLogStart(SPARK_VERSION)
      assert(lines.size === 3)
      assert(lines(0).contains("SparkListenerLogStart"))
      assert(lines(1).contains("SparkListenerStageSubmitted"))
      assert(lines(2).contains("SparkListenerJobStart"))

      lines.foreach{
        line => JsonProtocol.sparkEventFromJson(parse(line)) match {
          case logStartEvent: SparkListenerLogStart =>
            assert(logStartEvent == logStart)

          case stageSubmittedEvent: SparkListenerStageSubmitted =>
            assert(stageSubmittedEvent.properties.getProperty(secretKey) == "*********(redacted)")
            assert(stageSubmittedEvent.properties.getProperty(customKey) ==  customValue)

          case jobStartEvent : SparkListenerJobStart =>
            assert(jobStartEvent.properties.getProperty(secretKey) == "*********(redacted)")
            assert(jobStartEvent.properties.getProperty(customKey) ==  customValue)

          case _ => assert(false)
        }
      }
    } finally {
      logData.close()
    }
  }

  test("Executor metrics update") {
    testStageExecutorMetricsEventLogging()
  }

  test("SPARK-31764: isBarrier should be logged in event log") {
    val conf = new SparkConf()
    conf.set(EVENT_LOG_ENABLED, true)
    conf.set(EVENT_LOG_DIR, testDirPath.toString)
    val sc = new SparkContext("local", "test-SPARK-31764", conf)
    val appId = sc.applicationId

    sc.parallelize(1 to 10)
      .barrier()
      .mapPartitions(_.map(elem => (elem, elem)))
      .filter(elem => elem._1 % 2 == 0)
      .reduceByKey(_ + _)
      .collect
    sc.stop()

    val eventLogStream = EventLogFileReader.openEventLog(new Path(testDirPath, appId), fileSystem)
    val events = readLines(eventLogStream).map(line => JsonProtocol.sparkEventFromJson(parse(line)))
    val jobStartEvents = events
      .filter(event => event.isInstanceOf[SparkListenerJobStart])
      .map(_.asInstanceOf[SparkListenerJobStart])

    assert(jobStartEvents.size === 1)
    val stageInfos = jobStartEvents.head.stageInfos
    assert(stageInfos.size === 2)

    val stage0 = stageInfos(0)
    val rddInfosInStage0 = stage0.rddInfos
    assert(rddInfosInStage0.size === 3)
    val sortedRddInfosInStage0 = rddInfosInStage0.sortBy(_.scope.get.name)
    assert(sortedRddInfosInStage0(0).scope.get.name === "filter")
    assert(sortedRddInfosInStage0(0).isBarrier === true)
    assert(sortedRddInfosInStage0(1).scope.get.name === "mapPartitions")
    assert(sortedRddInfosInStage0(1).isBarrier === true)
    assert(sortedRddInfosInStage0(2).scope.get.name === "parallelize")
    assert(sortedRddInfosInStage0(2).isBarrier === false)

    val stage1 = stageInfos(1)
    val rddInfosInStage1 = stage1.rddInfos
    assert(rddInfosInStage1.size === 1)
    assert(rddInfosInStage1(0).scope.get.name === "reduceByKey")
    assert(rddInfosInStage1(0).isBarrier === false) // reduceByKey
  }

  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  /**
   * Test basic event logging functionality.
   *
   * This creates two simple events, posts them to the EventLoggingListener, and verifies that
   * exactly these two events are logged in the expected file.
   */
  private def testEventLogging(
      compressionCodec: Option[String] = None,
      extraConf: Map[String, String] = Map()): Unit = {
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
    val logPath = eventLogger.logWriter.logPath
    val logData = EventLogFileReader.openEventLog(new Path(logPath), fileSystem)
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
  private def testApplicationEventLogging(compressionCodec: Option[String] = None): Unit = {
    // Set defaultFS to something that would cause an exception, to make sure we don't run
    // into SPARK-6688.
    val conf = getLoggingConf(testDirPath, compressionCodec)
      .set("spark.hadoop.fs.defaultFS", "unsupported://example.com")
    sc = new SparkContext("local-cluster[2,2,1024]", "test", conf)
    assert(sc.eventLogger.isDefined)
    val eventLogger = sc.eventLogger.get

    val eventLogPath = eventLogger.logWriter.logPath
    val expectedLogDir = testDir.toURI()
    assert(eventLogPath === SingleEventLogFileWriter.getLogPath(
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
    val logData = EventLogFileReader.openEventLog(new Path(eventLogger.logWriter.logPath),
      fileSystem)
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
   * values from SparkListenerExecutorMetricsUpdate events during a stage and
   * from SparkListenerTaskEnd events for tasks belonging to the stage are
   * logged in a StageExecutorMetrics event for each executor at stage completion.
   */
  private def testStageExecutorMetricsEventLogging(): Unit = {
    val conf = getLoggingConf(testDirPath, None)
    val logName = "stageExecutorMetrics-test"
    val eventLogger = new EventLoggingListener(logName, None, testDirPath.toUri(), conf)
    val listenerBus = new LiveListenerBus(conf)

    // Executor metrics
    // driver
    val md_1 = Array(4000L, 50L, 0L, 0L, 40L, 0L, 40L, 0L, 70L, 0L, 7500L, 3500L,
      0L, 0L, 0L, 0L, 10L, 90L, 2L, 20L, 110L)
    val md_2 = Array(4500L, 50L, 0L, 0L, 40L, 0L, 40L, 0L, 70L, 0L, 8000L, 3500L,
      0L, 0L, 0L, 0L, 10L, 90L, 3L, 20L, 110L)
    val md_3 = Array(4200L, 50L, 0L, 0L, 40L, 0L, 40L, 0L, 70L, 0L, 7800L, 3500L,
      0L, 0L, 0L, 0L, 15L, 100L, 5L, 20L, 120L)

    // executors 1 and 2
    val m1_1 = Array(4000L, 50L, 20L, 0L, 40L, 0L, 60L, 0L, 70L, 20L, 7500L, 3500L,
      6500L, 2500L, 5500L, 1500L, 10L, 90L, 2L, 20L, 110L)
    val m2_1 = Array(1500L, 50L, 20L, 0L, 0L, 0L, 20L, 0L, 70L, 0L, 8500L, 3500L,
      7500L, 2500L, 6500L, 1500L, 10L, 90L, 2L, 20L, 110L)
    val m1_2 = Array(4000L, 50L, 50L, 0L, 50L, 0L, 100L, 0L, 70L, 20L, 8000L, 4000L,
      7000L, 3000L, 6000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val m2_2 = Array(2000L, 50L, 10L, 0L, 10L, 0L, 30L, 0L, 70L, 0L, 9000L, 4000L,
      8000L, 3000L, 7000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val m1_3 = Array(2000L, 40L, 50L, 0L, 40L, 10L, 90L, 10L, 50L, 0L, 8000L, 3500L,
      7000L, 2500L, 6000L, 1500L, 10L, 90L, 2L, 20L, 110L)
    val m2_3 = Array(3500L, 50L, 15L, 0L, 10L, 10L, 35L, 10L, 80L, 0L, 8500L, 3500L,
      7500L, 2500L, 6500L, 1500L, 10L, 90L, 2L, 20L, 110L)
    val m1_4 = Array(5000L, 30L, 50L, 20L, 30L, 10L, 80L, 30L, 50L,
      0L, 5000L, 3000L, 4000L, 2000L, 3000L, 1000L, 10L, 90L, 2L, 20L, 110L)
    val m2_4 = Array(7000L, 70L, 50L, 20L, 0L, 10L, 50L, 30L, 10L,
      40L, 8000L, 4000L, 7000L, 3000L, 6000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val m1_5 = Array(6000L, 70L, 20L, 30L, 10L, 0L, 30L, 30L, 30L, 0L, 5000L, 3000L,
      4000L, 2000L, 3000L, 1000L, 10L, 90L, 2L, 20L, 110L)
    val m2_5 = Array(5500L, 30L, 20L, 40L, 10L, 0L, 30L, 40L, 40L,
      20L, 8000L, 5000L, 7000L, 4000L, 6000L, 3000L, 10L, 90L, 2L, 20L, 110L)
    val m1_6 = Array(7000L, 70L, 5L, 25L, 60L, 30L, 65L, 55L, 30L, 0L, 3000L, 2500L,
      2000L, 1500L, 1000L, 500L, 10L, 90L, 2L, 20L, 110L)
    val m2_6 = Array(5500L, 40L, 25L, 30L, 10L, 30L, 35L, 60L, 0L,
      20L, 7000L, 3000L, 6000L, 2000L, 5000L, 1000L, 10L, 90L, 2L, 20L, 110L)
    val m1_7 = Array(5500L, 70L, 15L, 20L, 55L, 20L, 70L, 40L, 20L,
      0L, 4000L, 2500L, 3000L, 1500L, 2000L, 500L, 10L, 90L, 2L, 20L, 110L)
    val m2_7 = Array(4000L, 20L, 25L, 30L, 10L, 30L, 35L, 60L, 0L, 0L, 7000L,
      4000L, 6000L, 3000L, 5000L, 2000L, 10L, 90L, 2L, 20L, 110L)

    // tasks
    val t1 = Array(4500L, 60L, 50L, 0L, 50L, 10L, 100L, 10L, 70L, 20L,
      8000L, 4000L, 7000L, 3000L, 6000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val t2 = Array(3500L, 50L, 20L, 0L, 10L, 10L, 35L, 10L, 80L, 0L,
      9000L, 4000L, 8000L, 3000L, 7000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val t3 = Array(5000L, 60L, 50L, 20L, 50L, 10L, 100L, 30L, 70L, 20L,
      8000L, 4000L, 7000L, 3000L, 6000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val t4 = Array(7000L, 70L, 50L, 20L, 10L, 10L, 50L, 30L, 80L, 40L,
      9000L, 4000L, 8000L, 3000L, 7000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val t5 = Array(7000L, 100L, 50L, 30L, 60L, 30L, 80L, 55L, 50L, 0L,
      5000L, 3000L, 4000L, 2000L, 3000L, 1000L, 10L, 90L, 2L, 20L, 110L)
    val t6 = Array(7200L, 70L, 50L, 40L, 10L, 30L, 50L, 60L, 40L, 40L,
      8000L, 5000L, 7000L, 4000L, 6000L, 3000L, 10L, 90L, 2L, 20L, 110L)

    def max(a: Array[Long], b: Array[Long]): Array[Long] =
      (a, b).zipped.map(Math.max).toArray

    // calculated metric peaks per stage per executor
    // metrics sent during stage 0 for each executor
    val cp0_1 = Seq(m1_1, m1_2, m1_3, t1, m1_4, t3).reduceLeft(max)
    val cp0_2 = Seq(m2_1, m2_2, m2_3, t2, m2_4, t4).reduceLeft(max)
    val cp0_d = Seq(md_1, md_2).reduceLeft(max)
    // metrics sent during stage 1 for each executor
    val cp1_1 = Seq(m1_4, m1_5, m1_6, m1_7, t5).reduceLeft(max)
    val cp1_2 = Seq(m2_4, m2_5, m2_6, m2_7, t6).reduceLeft(max)
    val cp1_d = Seq(md_2, md_3).reduceLeft(max)

    // expected metric peaks per stage per executor
    val p0_1 = Array(5000L, 60L, 50L, 20L, 50L, 10L, 100L, 30L,
      70L, 20L, 8000L, 4000L, 7000L, 3000L, 6000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val p0_2 = Array(7000L, 70L, 50L, 20L, 10L, 10L, 50L, 30L,
      80L, 40L, 9000L, 4000L, 8000L, 3000L, 7000L, 2000L, 10L, 90L, 2L, 20L, 110L)
    val p0_d = Array(4500L, 50L, 0L, 0L, 40L, 0L, 40L, 0L,
      70L, 0L, 8000L, 3500L, 0L, 0L, 0L, 0L, 10L, 90L, 3L, 20L, 110L)
    val p1_1 = Array(7000L, 100L, 50L, 30L, 60L, 30L, 80L, 55L,
      50L, 0L, 5000L, 3000L, 4000L, 2000L, 3000L, 1000L, 10L, 90L, 2L, 20L, 110L)
    val p1_2 = Array(7200L, 70L, 50L, 40L, 10L, 30L, 50L, 60L,
      40L, 40L, 8000L, 5000L, 7000L, 4000L, 6000L, 3000L, 10L, 90L, 2L, 20L, 110L)
    val p1_d = Array(4500L, 50L, 0L, 0L, 40L, 0L, 40L, 0L,
      70L, 0L, 8000L, 3500L, 0L, 0L, 0L, 0L, 15L, 100L, 5L, 20L, 120L)

    assert(Arrays.equals(p0_1, cp0_1))
    assert(Arrays.equals(p0_2, cp0_2))
    assert(Arrays.equals(p0_d, cp0_d))
    assert(Arrays.equals(p1_1, cp1_1))
    assert(Arrays.equals(p1_2, cp1_2))
    assert(Arrays.equals(p1_d, cp1_d))

    // Events to post.
    val events = Array(
      SparkListenerApplicationStart("executionMetrics", None,
        1L, "update", None),
      createExecutorAddedEvent(1),
      createExecutorAddedEvent(2),
      createStageSubmittedEvent(0),
      // receive 3 metric updates from each executor with just stage 0 running,
      // with different peak updates for each executor
      // also, receive 1 metric update from the driver
      createExecutorMetricsUpdateEvent(List(0), "1", new ExecutorMetrics(m1_1)),
      createExecutorMetricsUpdateEvent(List(0), "2", new ExecutorMetrics(m2_1)),
      // exec 1: new stage 0 peaks for metrics at indexes: 2, 4, 6
      createExecutorMetricsUpdateEvent(List(0), "1", new ExecutorMetrics(m1_2)),
      // exec 2: new stage 0 peaks for metrics at indexes: 0, 4, 6
      createExecutorMetricsUpdateEvent(List(0), "2", new ExecutorMetrics(m2_2)),
      // driver
      createExecutorMetricsUpdateEvent(List(-1), "driver", new ExecutorMetrics(md_1)),
      // exec 1: new stage 0 peaks for metrics at indexes: 5, 7
      createExecutorMetricsUpdateEvent(List(0), "1", new ExecutorMetrics(m1_3)),
      // exec 2: new stage 0 peaks for metrics at indexes: 0, 5, 6, 7, 8
      createExecutorMetricsUpdateEvent(List(0), "2", new ExecutorMetrics(m2_3)),
      // stage 0: task 1 (on exec 1) and task 2 (on exec 2) end
      createTaskEndEvent(1L, 0, "1", 0, "ShuffleMapTask", new ExecutorMetrics(t1)),
      createTaskEndEvent(2L, 0, "2", 0, "ShuffleMapTask", new ExecutorMetrics(t2)),
      // now start stage 1, one more metric update for each executor, and new
      // peaks for some stage 1 metrics (as listed), initialize stage 1 peaks
      createStageSubmittedEvent(1),
      // exec 1: new stage 0 peaks for metrics at indexes: 0, 3, 7; initialize stage 1 peaks
      createExecutorMetricsUpdateEvent(List(0, 1), "1", new ExecutorMetrics(m1_4)),
      // exec 2: new stage 0 peaks for metrics at indexes: 0, 1, 3, 6, 7, 9;
      // initialize stage 1 peaks
      createExecutorMetricsUpdateEvent(List(0, 1), "2", new ExecutorMetrics(m2_4)),
      // driver
      createExecutorMetricsUpdateEvent(List(-1), "driver", new ExecutorMetrics(md_2)),
      // stage 0: task 3 (on exec 1) and task 4 (on exec 2) end
      createTaskEndEvent(3L, 1, "1", 0, "ShuffleMapTask", new ExecutorMetrics(t3)),
      createTaskEndEvent(4L, 1, "2", 0, "ShuffleMapTask", new ExecutorMetrics(t4)),
      // complete stage 0, and 3 more updates for each executor with just
      // stage 1 running
      createStageCompletedEvent(0),
      // exec 1: new stage 1 peaks for metrics at indexes: 0, 1, 3
      createExecutorMetricsUpdateEvent(List(1), "1", new ExecutorMetrics(m1_5)),
      // exec 2: new stage 1 peaks for metrics at indexes: 3, 4, 7, 8
      createExecutorMetricsUpdateEvent(List(1), "2", new ExecutorMetrics(m2_5)),
      // exec 1: new stage 1 peaks for metrics at indexes: 0, 4, 5, 7
      createExecutorMetricsUpdateEvent(List(1), "1", new ExecutorMetrics(m1_6)),
      // exec 2: new stage 1 peak for metrics at index: 7
      createExecutorMetricsUpdateEvent(List(1), "2", new ExecutorMetrics(m2_6)),
      // driver
      createExecutorMetricsUpdateEvent(List(-1), "driver", new ExecutorMetrics(md_3)),
      // exec 1: no new stage 1 peaks
      createExecutorMetricsUpdateEvent(List(1), "1", new ExecutorMetrics(m1_7)),
      // stage 1: task 5 (on exec 1) end; new stage 1 peaks at index: 1
      createTaskEndEvent(5L, 2, "1", 1, "ResultTask", new ExecutorMetrics(t5)),
      createExecutorRemovedEvent(1),
      // exec 2: new stage 1 peak for metrics at index: 6
      createExecutorMetricsUpdateEvent(List(1), "2", new ExecutorMetrics(m2_7)),
      // stage 1: task 6 (on exec 2) end; new stage 2 peaks at index: 0
      createTaskEndEvent(6L, 2, "2", 1, "ResultTask", new ExecutorMetrics(t6)),
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
        new SparkListenerStageExecutorMetrics("1", 0, 0, new ExecutorMetrics(p0_1))),
      ((0, "2"),
        new SparkListenerStageExecutorMetrics("2", 0, 0, new ExecutorMetrics(p0_2))),
      ((0, "driver"),
        new SparkListenerStageExecutorMetrics("driver", 0, 0, new ExecutorMetrics(p0_d))),
      ((1, "1"),
        new SparkListenerStageExecutorMetrics("1", 1, 0, new ExecutorMetrics(p1_1))),
      ((1, "2"),
        new SparkListenerStageExecutorMetrics("2", 1, 0, new ExecutorMetrics(p1_2))),
      ((1, "driver"),
        new SparkListenerStageExecutorMetrics("driver", 1, 0, new ExecutorMetrics(p1_d))))
    // Verify the log file contains the expected events.
    // Posted events should be logged, except for ExecutorMetricsUpdate events -- these
    // are consolidated, and the peak values for each stage are logged at stage end.
    val logData = EventLogFileReader.openEventLog(new Path(eventLogger.logWriter.logPath),
      fileSystem)
    try {
      val lines = readLines(logData)
      val logStart = SparkListenerLogStart(SPARK_VERSION)
      assert(lines.size === 25)
      assert(lines(0).contains("SparkListenerLogStart"))
      assert(lines(1).contains("SparkListenerApplicationStart"))
      assert(JsonProtocol.sparkEventFromJson(parse(lines(0))) === logStart)
      var logIdx = 1
      events.foreach { event =>
        event match {
          case metricsUpdate: SparkListenerExecutorMetricsUpdate
            if metricsUpdate.execId != SparkContext.DRIVER_IDENTIFIER =>
          case stageCompleted: SparkListenerStageCompleted =>
            val execIds = Set[String]()
            (1 to 3).foreach { _ =>
              val execId = checkStageExecutorMetrics(lines(logIdx),
                stageCompleted.stageInfo.stageId, expectedMetricsEvents)
              execIds += execId
              logIdx += 1
            }
            assert(execIds.size == 3) // check that each executor/driver was logged
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
      Seq.empty, Seq.empty, "details",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
  }

  private def createStageCompletedEvent(stageId: Int) = {
    SparkListenerStageCompleted(new StageInfo(stageId, 0, stageId.toString, 0,
      Seq.empty, Seq.empty, "details",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
  }

  private def createExecutorAddedEvent(executorId: Int) = {
    SparkListenerExecutorAdded(0L, executorId.toString,
      new ExecutorInfo("host1", 1, Map.empty, Map.empty))
  }

  private def createExecutorRemovedEvent(executorId: Int) = {
    SparkListenerExecutorRemoved(0L, executorId.toString, "test")
  }

  /**
   * Helper to create a SparkListenerExecutorMetricsUpdate event.
   * For the driver (executorId == "driver"), the executorUpdates contain a single entry with
   * the key (-1, -1). There should be a single stageId passed in stageIds, namely -1.
   * For the executors, for each stage, we assume there is a single stage attempt (attempt 0);
   * the executorUpdates contain an entry for each stageId passed in stageIds, with the key
   * (stageId, 0).
   * The same executorMetrics are associated to each key in the executorUpdates.
   */
  private def createExecutorMetricsUpdateEvent(
      stageIds: Seq[Int],
      executorId: String,
      executorMetrics: ExecutorMetrics): SparkListenerExecutorMetricsUpdate = {
    val taskMetrics = TaskMetrics.empty
    taskMetrics.incDiskBytesSpilled(111)
    taskMetrics.incMemoryBytesSpilled(222)
    val accum = Array((333L, 1, 1, taskMetrics.accumulators().map(AccumulatorSuite.makeInfo)))
    val executorUpdates =
      if (executorId == "driver") {
        stageIds.map(id => (id, -1) -> executorMetrics).toMap
      } else {
        stageIds.map(id => (id, 0) -> executorMetrics).toMap
      }
    SparkListenerExecutorMetricsUpdate(executorId, accum, executorUpdates)
  }

  private def createTaskEndEvent(
      taskId: Long,
      taskIndex: Int,
      executorId: String,
      stageId: Int,
      taskType: String,
      executorMetrics: ExecutorMetrics): SparkListenerTaskEnd = {
    val taskInfo = new TaskInfo(taskId, taskIndex, 0, 1553291556000L, executorId, "executor",
      TaskLocality.NODE_LOCAL, false)
    val taskMetrics = TaskMetrics.empty
    SparkListenerTaskEnd(stageId, 0, taskType, Success, taskInfo, executorMetrics, taskMetrics)
  }

  /** Check that the Spark history log line matches the expected event. */
  private def checkEvent(line: String, event: SparkListenerEvent): Unit = {
    assert(line.contains(event.getClass.toString.split("\\.").last))
    val parsed = JsonProtocol.sparkEventFromJson(parse(line))
    assert(parsed.getClass === event.getClass)
    (event, parsed) match {
      case (expected: SparkListenerStageSubmitted, actual: SparkListenerStageSubmitted) =>
        // accumulables can be different, so only check the stage Id
        assert(expected.stageInfo.stageId === actual.stageInfo.stageId)
      case (expected: SparkListenerStageCompleted, actual: SparkListenerStageCompleted) =>
        // accumulables can be different, so only check the stage Id
        assert(expected.stageInfo.stageId === actual.stageInfo.stageId)
      case (expected: SparkListenerTaskEnd, actual: SparkListenerTaskEnd) =>
        assert(expected.stageId === actual.stageId)
      case (expected: SparkListenerExecutorMetricsUpdate,
          actual: SparkListenerExecutorMetricsUpdate) =>
        assert(expected.execId == actual.execId)
        assert(expected.execId == SparkContext.DRIVER_IDENTIFIER)
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
    Source.fromInputStream(in)(Codec.UTF8).getLines().toSeq
  }

  /**
   * A listener that asserts certain events are logged by the given EventLoggingListener.
   * This is necessary because events are posted asynchronously in a different thread.
   */
  private class EventExistenceListener(eventLogger: EventLoggingListener) extends SparkListener {
    var jobStarted = false
    var jobEnded = false
    var appEnded = false

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      jobStarted = true
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      jobEnded = true
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      appEnded = true
    }

    def assertAllCallbacksInvoked(): Unit = {
      assert(jobStarted, "JobStart callback not invoked!")
      assert(jobEnded, "JobEnd callback not invoked!")
      assert(appEnded, "ApplicationEnd callback not invoked!")
    }
  }

}
