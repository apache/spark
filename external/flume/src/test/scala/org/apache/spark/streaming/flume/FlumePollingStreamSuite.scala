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

package org.apache.spark.streaming.flume

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, TestOutputStream}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.util.{ManualClock, Utils}

class FlumePollingStreamSuite extends SparkFunSuite with BeforeAndAfterAll with Logging {

  val maxAttempts = 5
  val batchDuration = Seconds(1)

  @transient private var _sc: SparkContext = _

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")

  val utils = new PollingFlumeTestUtils

  override def beforeAll(): Unit = {
    _sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
  }

  test("flume polling test") {
    testMultipleTimes(testFlumePolling)
  }

  test("flume polling test multiple hosts") {
    testMultipleTimes(testFlumePollingMultipleHost)
  }

  /**
   * Run the given test until no more java.net.BindException's are thrown.
   * Do this only up to a certain attempt limit.
   */
  private def testMultipleTimes(test: () => Unit): Unit = {
    var testPassed = false
    var attempt = 0
    while (!testPassed && attempt < maxAttempts) {
      try {
        test()
        testPassed = true
      } catch {
        case e: Exception if Utils.isBindCollision(e) =>
          logWarning("Exception when running flume polling test: " + e)
          attempt += 1
      }
    }
    assert(testPassed, s"Test failed after $attempt attempts!")
  }

  private def testFlumePolling(): Unit = {
    try {
      val port = utils.startSingleSink()

      writeAndVerify(Seq(port))
      utils.assertChannelsAreEmpty()
    } finally {
      utils.close()
    }
  }

  private def testFlumePollingMultipleHost(): Unit = {
    try {
      val ports = utils.startMultipleSinks()
      writeAndVerify(ports)
      utils.assertChannelsAreEmpty()
    } finally {
      utils.close()
    }
  }

  def writeAndVerify(sinkPorts: Seq[Int]): Unit = {
    // Set up the streaming context and input streams
    val ssc = new StreamingContext(_sc, batchDuration)
    val addresses = sinkPorts.map(port => new InetSocketAddress("localhost", port))
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] =
      FlumeUtils.createPollingStream(ssc, addresses, StorageLevel.MEMORY_AND_DISK,
        utils.eventsPerBatch, 5)
    val outputQueue = new ConcurrentLinkedQueue[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputQueue)
    outputStream.register()

    ssc.start()
    try {
      utils.sendDataAndEnsureAllDataHasBeenReceived()
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      clock.advance(batchDuration.milliseconds)

      // The eventually is required to ensure that all data in the batch has been processed.
      eventually(timeout(10 seconds), interval(100 milliseconds)) {
        val flattenOutput = outputQueue.asScala.toSeq.flatten
        val headers = flattenOutput.map(_.event.getHeaders.asScala.map {
          case (key, value) => (key.toString, value.toString)
        }).map(_.asJava)
        val bodies = flattenOutput.map(e => JavaUtils.bytesToString(e.event.getBody))
        utils.assertOutput(headers.asJava, bodies.asJava)
      }
    } finally {
      // here stop ssc only, but not underlying sparkcontext
      ssc.stop(false)
    }
  }

}
