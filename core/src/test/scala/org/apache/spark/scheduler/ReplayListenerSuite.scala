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

import java.io.{File, PrintWriter}

import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{SparkConf, SparkContext, SPARK_VERSION}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * Test whether ReplayListenerBus replays events from logs correctly.
 */
class ReplayListenerSuite extends FunSuite with BeforeAndAfter {
  private val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  private var testDir: File = _

  before {
    testDir = Utils.createTempDir()
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Simple replay") {
    val logFilePath = Utils.getFilePath(testDir, "events.txt")
    val fstream = fileSystem.create(logFilePath)
    val writer = new PrintWriter(fstream)
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", None,
      125L, "Mickey")
    val applicationEnd = SparkListenerApplicationEnd(1000L)
    writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationStart))))
    writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationEnd))))
    writer.close()

    val conf = EventLoggingListenerSuite.getLoggingConf(logFilePath)
    val logData = fileSystem.open(logFilePath)
    val eventMonster = new EventMonster(conf)
    try {
      val replayer = new ReplayListenerBus()
      replayer.addListener(eventMonster)
      replayer.replay(logData, logFilePath.toString)
    } finally {
      logData.close()
    }
    assert(eventMonster.loggedEvents.size === 2)
    assert(eventMonster.loggedEvents(0) === JsonProtocol.sparkEventToJson(applicationStart))
    assert(eventMonster.loggedEvents(1) === JsonProtocol.sparkEventToJson(applicationEnd))
  }

  // This assumes the correctness of EventLoggingListener
  test("End-to-end replay") {
    testApplicationReplay()
  }

  // This assumes the correctness of EventLoggingListener
  test("End-to-end replay with compression") {
    CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codec =>
      testApplicationReplay(Some(codec))
    }
  }


  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  /**
   * Test end-to-end replaying of events.
   *
   * This test runs a few simple jobs with event logging enabled, and compares each emitted
   * event to the corresponding event replayed from the event logs. This test makes the
   * assumption that the event logging behavior is correct (tested in a separate suite).
   */
  private def testApplicationReplay(codecName: Option[String] = None) {
    val logDirPath = Utils.getFilePath(testDir, "test-replay")
    fileSystem.mkdirs(logDirPath)

    val conf = EventLoggingListenerSuite.getLoggingConf(logDirPath, codecName)
    val sc = new SparkContext("local-cluster[2,1,512]", "Test replay", conf)

    // Run a few jobs
    sc.parallelize(1 to 100, 1).count()
    sc.parallelize(1 to 100, 2).map(i => (i, i)).count()
    sc.parallelize(1 to 100, 3).map(i => (i, i)).groupByKey().count()
    sc.parallelize(1 to 100, 4).map(i => (i, i)).groupByKey().persist().count()
    sc.stop()

    // Prepare information needed for replay
    val applications = fileSystem.listStatus(logDirPath)
    assert(applications != null && applications.size > 0)
    val eventLog = applications.sortBy(_.getModificationTime).last
    assert(!eventLog.isDir)

    // Replay events
    val logData = EventLoggingListener.openEventLog(eventLog.getPath(), fileSystem)
    val eventMonster = new EventMonster(conf)
    try {
      val replayer = new ReplayListenerBus()
      replayer.addListener(eventMonster)
      replayer.replay(logData, eventLog.getPath().toString)
    } finally {
      logData.close()
    }

    // Verify the same events are replayed in the same order
    assert(sc.eventLogger.isDefined)
    val originalEvents = sc.eventLogger.get.loggedEvents
    val replayedEvents = eventMonster.loggedEvents
    originalEvents.zip(replayedEvents).foreach { case (e1, e2) => assert(e1 === e2) }
  }

  /**
   * A simple listener that buffers all the events it receives.
   *
   * The event buffering functionality must be implemented within EventLoggingListener itself.
   * This is because of the following race condition: the event may be mutated between being
   * processed by one listener and being processed by another. Thus, in order to establish
   * a fair comparison between the original events and the replayed events, both functionalities
   * must be implemented within one listener (i.e. the EventLoggingListener).
   *
   * This child listener inherits only the event buffering functionality, but does not actually
   * log the events.
   */
  private class EventMonster(conf: SparkConf)
    extends EventLoggingListener("test", "testdir", conf) {

    override def start() { }

  }
}
