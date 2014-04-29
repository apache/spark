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

import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.{JsonProtocol, Utils}
import org.apache.spark.io.CompressionCodec

/**
 * Test for whether ReplayListenerBus replays events from logs correctly.
 */
class ReplayListenerSuite extends FunSuite with BeforeAndAfter {
  private val fileSystem = Utils.getHadoopFileSystem("/")
  private val allCompressionCodecs = Seq[String](
    "org.apache.spark.io.LZFCompressionCodec",
    "org.apache.spark.io.SnappyCompressionCodec"
  )

  after {

  }

  test("Simple replay") {
    testSimpleReplay()
  }

  test("Simple replay with compression") {
    allCompressionCodecs.foreach { codec =>
      testSimpleReplay(Some(codec))
    }
  }

  test("End-to-end replay") {
    testApplicationReplay()
  }


  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  /**
   * Test simple replaying of events.
   */
  private def testSimpleReplay(codecName: Option[String] = None) {
    val logFilePath = new Path("/tmp/events.txt")
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
    val eventKeeper = new EventKeeper
    replayer.addListener(eventKeeper)
    replayer.replay()
    assert(eventKeeper.events.size === 2)
    assert(eventKeeper.events(0) === applicationStart)
    assert(eventKeeper.events(1) === applicationEnd)
  }

  /**
   *
   */
  private def testApplicationReplay(codecName: Option[String] = None) {
    val logDir = "/tmp/test-replay"
    val logDirPath = new Path(logDir)
    val conf = EventLoggingListenerSuite.getLoggingConf(Some(logDir), codecName)
    val sc = new SparkContext("local-cluster[2,1,512]", "Test replay", conf)
    val eventKeeper = new EventKeeper
    sc.addSparkListener(eventKeeper)

    // Run a job
    sc.parallelize(1 to 100, 4).map(i => (i, i)).groupByKey().cache().count()
    sc.stop()

    // Find the log file
    val applications = fileSystem.listStatus(logDirPath)
    assert(applications != null && applications.size > 0)
    val eventLogDir =
      applications.filter(_.getPath.getName.startsWith("test-replay")).sortBy(_.getAccessTime).last
    assert(eventLogDir.isDir)
    val logFiles = fileSystem.listStatus(eventLogDir.getPath)
    assert(logFiles != null && logFiles.size > 0)
    val logFile = logFiles.find(_.getPath.getName.startsWith("EVENT_LOG_"))
    assert(logFile.isDefined)
    val codec = codecName.map(getCompressionCodec)

    // Replay events
    val replayer = new ReplayListenerBus(Seq(logFile.get.getPath), fileSystem, codec)
    val replayEventKeeper = new EventKeeper
    replayer.addListener(replayEventKeeper)
    replayer.replay()

    // Verify the same events are replayed in the same order
    val filteredEvents = filterSchedulerEvents(eventKeeper.events)
    val filteredReplayEvents = filterSchedulerEvents(replayEventKeeper.events)
    assert(filteredEvents.size === filteredReplayEvents.size)
    filteredEvents.zip(filteredReplayEvents).foreach { case (e1, e2) =>
      assert(JsonProtocol.sparkEventToJson(e1) === JsonProtocol.sparkEventToJson(e2))
    }
  }

  /**
   * A simple listener that keeps all events it receives
   */
  private class EventKeeper extends SparkListener {
    val events = new ArrayBuffer[SparkListenerEvent]
    override def onStageSubmitted(e: SparkListenerStageSubmitted) { events += e }
    override def onStageCompleted(e: SparkListenerStageCompleted) { events += e }
    override def onTaskStart(e: SparkListenerTaskStart) { events += e }
    override def onTaskGettingResult(e: SparkListenerTaskGettingResult) { events += e }
    override def onTaskEnd(e: SparkListenerTaskEnd) { events += e }
    override def onJobStart(e: SparkListenerJobStart) { events += e }
    override def onJobEnd(e: SparkListenerJobEnd) { events += e }
    override def onEnvironmentUpdate(e: SparkListenerEnvironmentUpdate) { events += e }
    override def onBlockManagerAdded(e: SparkListenerBlockManagerAdded) = { events += e }
    override def onBlockManagerRemoved(e: SparkListenerBlockManagerRemoved) = { events += e }
    override def onUnpersistRDD(e: SparkListenerUnpersistRDD) { events += e }
    override def onApplicationStart(e: SparkListenerApplicationStart) { events += e }
    override def onApplicationEnd(e: SparkListenerApplicationEnd) { events += e }
  }

  private def filterSchedulerEvents(events: Seq[SparkListenerEvent]): Seq[SparkListenerEvent] = {
    events.collect {
      case e: SparkListenerStageSubmitted => e
      case e: SparkListenerStageCompleted => e
      case e: SparkListenerTaskStart => e
      case e: SparkListenerTaskGettingResult => e
      case e: SparkListenerTaskEnd => e
      case e: SparkListenerJobStart => e
      case e: SparkListenerJobEnd => e
    }
  }

  private def getCompressionCodec(codecName: String) = {
    val conf = new SparkConf
    conf.set("spark.io.compression.codec", codecName)
    CompressionCodec.createCodec(conf)
  }

}
