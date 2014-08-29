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

import com.google.common.io.Files
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * Test whether ReplayListenerBus replays events from logs correctly.
 */
class ReplayListenerSuite extends FunSuite with BeforeAndAfter {
  private val fileSystem = Utils.getHadoopFileSystem("/")
  private val allCompressionCodecs = CompressionCodec.ALL_COMPRESSION_CODECS
  private var testDir: File = _

  before {
    testDir = Files.createTempDir()
    testDir.deleteOnExit()
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Simple replay") {
    testSimpleReplay()
  }

  test("Simple replay with compression") {
    allCompressionCodecs.foreach { codec =>
      testSimpleReplay(Some(codec))
    }
  }

  // This assumes the correctness of EventLoggingListener
  test("End-to-end replay") {
    testApplicationReplay()
  }

  // This assumes the correctness of EventLoggingListener
  test("End-to-end replay with compression") {
    allCompressionCodecs.foreach { codec =>
      testApplicationReplay(Some(codec))
    }
  }


  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  /**
   * Test simple replaying of events.
   */
  private def testSimpleReplay(codecName: Option[String] = None) {
    val logFilePath = Utils.getFilePath(testDir, "events.txt")
    val codec = codecName.map(getCompressionCodec)
    val fstream = fileSystem.create(logFilePath)
    val cstream = codec.map(_.compressedOutputStream(fstream)).getOrElse(fstream)
    val writer = new PrintWriter(cstream)
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", 125L, "Mickey")
    val applicationEnd = SparkListenerApplicationEnd(1000L)
    writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationStart))))
    writer.println(compact(render(JsonProtocol.sparkEventToJson(applicationEnd))))
    writer.close()
    val replayer = new ReplayListenerBus(Seq(logFilePath), fileSystem, codec)
    val conf = EventLoggingListenerSuite.getLoggingConf(logFilePath, codecName)
    val eventMonster = new EventMonster(conf)
    replayer.addListener(eventMonster)
    replayer.replay()
    assert(eventMonster.loggedEvents.size === 2)
    assert(eventMonster.loggedEvents(0) === JsonProtocol.sparkEventToJson(applicationStart))
    assert(eventMonster.loggedEvents(1) === JsonProtocol.sparkEventToJson(applicationEnd))
  }

  /**
   * Test end-to-end replaying of events.
   *
   * This test runs a few simple jobs with event logging enabled, and compares each emitted
   * event to the corresponding event replayed from the event logs. This test makes the
   * assumption that the event logging behavior is correct (tested in a separate suite).
   */
  private def testApplicationReplay(codecName: Option[String] = None) {
    val logDirPath = Utils.getFilePath(testDir, "test-replay")
    val conf = EventLoggingListenerSuite.getLoggingConf(logDirPath, codecName)
    val sc = new SparkContext("local-cluster[2,1,512]", "Test replay", conf)

    // Run a few jobs
    sc.parallelize(1 to 100, 1).count()
    sc.parallelize(1 to 100, 2).map(i => (i, i)).count()
    sc.parallelize(1 to 100, 3).map(i => (i, i)).groupByKey().count()
    sc.parallelize(1 to 100, 4).map(i => (i, i)).groupByKey().persist().count()
    sc.stop()

    // Prepare information needed for replay
    val codec = codecName.map(getCompressionCodec)
    val applications = fileSystem.listStatus(logDirPath)
    assert(applications != null && applications.size > 0)
    val eventLogDir = applications.sortBy(_.getAccessTime).last
    assert(eventLogDir.isDir)
    val logFiles = fileSystem.listStatus(eventLogDir.getPath)
    assert(logFiles != null && logFiles.size > 0)
    val logFile = logFiles.find(_.getPath.getName.startsWith("EVENT_LOG_"))
    assert(logFile.isDefined)
    val logFilePath = logFile.get.getPath

    // Replay events
    val replayer = new ReplayListenerBus(Seq(logFilePath), fileSystem, codec)
    val eventMonster = new EventMonster(conf)
    replayer.addListener(eventMonster)
    replayer.replay()

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
  private class EventMonster(conf: SparkConf) extends EventLoggingListener("test", conf) {
    logger.close()
  }

  private def getCompressionCodec(codecName: String) = {
    val conf = new SparkConf
    conf.set("spark.io.compression.codec", codecName)
    CompressionCodec.createCodec(conf)
  }

}
