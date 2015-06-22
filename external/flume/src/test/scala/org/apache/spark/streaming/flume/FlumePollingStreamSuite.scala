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
import java.util.concurrent._

import scala.collection.JavaConversions._
import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.flume.Context
import org.apache.flume.channel.MemoryChannel
import org.apache.flume.conf.Configurables
import org.apache.flume.event.EventBuilder
import org.scalatest.concurrent.Eventually._

import org.scalatest.BeforeAndAfter

import org.apache.spark.{Logging, SparkConf, SparkFunSuite}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, TestOutputStream, StreamingContext}
import org.apache.spark.streaming.flume.sink._
import org.apache.spark.util.{ManualClock, Utils}

class FlumePollingStreamSuite extends SparkFunSuite with BeforeAndAfter with Logging {

  val batchCount = 5
  val eventsPerBatch = 100
  val totalEventsPerChannel = batchCount * eventsPerBatch
  val channelCapacity = 5000
  val maxAttempts = 5
  val batchDuration = Seconds(1)

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)

  def beforeFunction() {
    logInfo("Using manual clock")
    conf.set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
  }

  before(beforeFunction())

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
    // Start the channel and sink.
    val context = new Context()
    context.put("capacity", channelCapacity.toString)
    context.put("transactionCapacity", "1000")
    context.put("keep-alive", "0")
    val channel = new MemoryChannel()
    Configurables.configure(channel, context)

    val sink = new SparkSink()
    context.put(SparkSinkConfig.CONF_HOSTNAME, "localhost")
    context.put(SparkSinkConfig.CONF_PORT, String.valueOf(0))
    Configurables.configure(sink, context)
    sink.setChannel(channel)
    sink.start()

    writeAndVerify(Seq(sink), Seq(channel))
    assertChannelIsEmpty(channel)
    sink.stop()
    channel.stop()
  }

  private def testFlumePollingMultipleHost(): Unit = {
    // Start the channel and sink.
    val context = new Context()
    context.put("capacity", channelCapacity.toString)
    context.put("transactionCapacity", "1000")
    context.put("keep-alive", "0")
    val channel = new MemoryChannel()
    Configurables.configure(channel, context)

    val channel2 = new MemoryChannel()
    Configurables.configure(channel2, context)

    val sink = new SparkSink()
    context.put(SparkSinkConfig.CONF_HOSTNAME, "localhost")
    context.put(SparkSinkConfig.CONF_PORT, String.valueOf(0))
    Configurables.configure(sink, context)
    sink.setChannel(channel)
    sink.start()

    val sink2 = new SparkSink()
    context.put(SparkSinkConfig.CONF_HOSTNAME, "localhost")
    context.put(SparkSinkConfig.CONF_PORT, String.valueOf(0))
    Configurables.configure(sink2, context)
    sink2.setChannel(channel2)
    sink2.start()
    try {
      writeAndVerify(Seq(sink, sink2), Seq(channel, channel2))
      assertChannelIsEmpty(channel)
      assertChannelIsEmpty(channel2)
    } finally {
      sink.stop()
      sink2.stop()
      channel.stop()
      channel2.stop()
    }
  }

  def writeAndVerify(sinks: Seq[SparkSink], channels: Seq[MemoryChannel]) {
    // Set up the streaming context and input streams
    val ssc = new StreamingContext(conf, batchDuration)
    val addresses = sinks.map(sink => new InetSocketAddress("localhost", sink.getPort()))
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] =
      FlumeUtils.createPollingStream(ssc, addresses, StorageLevel.MEMORY_AND_DISK,
        eventsPerBatch, 5)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumeEvent]]
      with SynchronizedBuffer[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    outputStream.register()

    ssc.start()
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val executor = Executors.newCachedThreadPool()
    val executorCompletion = new ExecutorCompletionService[Void](executor)

    val latch = new CountDownLatch(batchCount * channels.size)
    sinks.foreach(_.countdownWhenBatchReceived(latch))

    channels.foreach(channel => {
      executorCompletion.submit(new TxnSubmitter(channel, clock))
    })

    for (i <- 0 until channels.size) {
      executorCompletion.take()
    }

    latch.await(15, TimeUnit.SECONDS) // Ensure all data has been received.
    clock.advance(batchDuration.milliseconds)

    // The eventually is required to ensure that all data in the batch has been processed.
    eventually(timeout(10 seconds), interval(100 milliseconds)) {
      val flattenedBuffer = outputBuffer.flatten
      assert(flattenedBuffer.size === totalEventsPerChannel * channels.size)
      var counter = 0
      for (k <- 0 until channels.size; i <- 0 until totalEventsPerChannel) {
        val eventToVerify = EventBuilder.withBody((channels(k).getName + " - " +
          String.valueOf(i)).getBytes("utf-8"),
          Map[String, String]("test-" + i.toString -> "header"))
        var found = false
        var j = 0
        while (j < flattenedBuffer.size && !found) {
          val strToCompare = new String(flattenedBuffer(j).event.getBody.array(), "utf-8")
          if (new String(eventToVerify.getBody, "utf-8") == strToCompare &&
            eventToVerify.getHeaders.get("test-" + i.toString)
              .equals(flattenedBuffer(j).event.getHeaders.get("test-" + i.toString))) {
            found = true
            counter += 1
          }
          j += 1
        }
      }
      assert(counter === totalEventsPerChannel * channels.size)
    }
    ssc.stop()
  }

  def assertChannelIsEmpty(channel: MemoryChannel): Unit = {
    val queueRemaining = channel.getClass.getDeclaredField("queueRemaining")
    queueRemaining.setAccessible(true)
    val m = queueRemaining.get(channel).getClass.getDeclaredMethod("availablePermits")
    assert(m.invoke(queueRemaining.get(channel)).asInstanceOf[Int] === 5000)
  }

  private class TxnSubmitter(channel: MemoryChannel, clock: ManualClock) extends Callable[Void] {
    override def call(): Void = {
      var t = 0
      for (i <- 0 until batchCount) {
        val tx = channel.getTransaction
        tx.begin()
        for (j <- 0 until eventsPerBatch) {
          channel.put(EventBuilder.withBody((channel.getName + " - " + String.valueOf(t)).getBytes(
            "utf-8"),
            Map[String, String]("test-" + t.toString -> "header")))
          t += 1
        }
        tx.commit()
        tx.close()
        Thread.sleep(500) // Allow some time for the events to reach
      }
      null
    }
  }

}
