/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.streaming.flume

import java.net.InetSocketAddress
import java.util.concurrent.{Callable, ExecutorCompletionService, Executors}

import scala.collection.JavaConversions._
import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer}

import org.apache.flume.Context
import org.apache.flume.channel.MemoryChannel
import org.apache.flume.conf.Configurables
import org.apache.flume.event.EventBuilder

import org.apache.spark.flume.sink.{SparkSinkConfig, SparkSink}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.util.ManualClock
import org.apache.spark.streaming.{TestSuiteBase, TestOutputStream, StreamingContext}

class FlumePollingReceiverSuite extends TestSuiteBase {

  val testPort = 9999

  test("flume polling test") {
    // Set up the streaming context and input streams
    val ssc = new StreamingContext(conf, batchDuration)
    val flumeStream: ReceiverInputDStream[SparkFlumePollingEvent] =
      FlumeUtils.createPollingStream(ssc, Seq(new InetSocketAddress("localhost", testPort)), 100, 5,
        StorageLevel.MEMORY_AND_DISK)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumePollingEvent]]
      with SynchronizedBuffer[Seq[SparkFlumePollingEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    outputStream.register()

    // Start the channel and sink.
    val context = new Context()
    context.put("capacity", "5000")
    context.put("transactionCapacity", "1000")
    context.put("keep-alive", "0")
    val channel = new MemoryChannel()
    Configurables.configure(channel, context)

    val sink = new SparkSink()
    context.put(SparkSinkConfig.CONF_HOSTNAME, "localhost")
    context.put(SparkSinkConfig.CONF_PORT, String.valueOf(testPort))
    Configurables.configure(sink, context)
    sink.setChannel(channel)
    sink.start()
    ssc.start()
    writeAndVerify(Seq(channel), ssc, outputBuffer)
    sink.stop()
    channel.stop()
  }

  test("flume polling test multiple hosts") {
    // Set up the streaming context and input streams
    val ssc = new StreamingContext(conf, batchDuration)
    val flumeStream: ReceiverInputDStream[SparkFlumePollingEvent] =
      FlumeUtils.createPollingStream(ssc, Seq(new InetSocketAddress("localhost", testPort),
        new InetSocketAddress("localhost", testPort + 1)), 100, 5,
        StorageLevel.MEMORY_AND_DISK)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumePollingEvent]]
      with SynchronizedBuffer[Seq[SparkFlumePollingEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    outputStream.register()

    // Start the channel and sink.
    val context = new Context()
    context.put("capacity", "5000")
    context.put("transactionCapacity", "1000")
    context.put("keep-alive", "0")
    val channel = new MemoryChannel()
    Configurables.configure(channel, context)

    val channel2 = new MemoryChannel()
    Configurables.configure(channel2, context)

    val sink = new SparkSink()
    context.put(SparkSinkConfig.CONF_HOSTNAME, "localhost")
    context.put(SparkSinkConfig.CONF_PORT, String.valueOf(testPort))
    Configurables.configure(sink, context)
    sink.setChannel(channel)
    sink.start()

    val sink2 = new SparkSink()
    context.put(SparkSinkConfig.CONF_HOSTNAME, "localhost")
    context.put(SparkSinkConfig.CONF_PORT, String.valueOf(testPort + 1))
    Configurables.configure(sink2, context)
    sink2.setChannel(channel2)
    sink2.start()
    ssc.start()
    writeAndVerify(Seq(channel, channel2), ssc, outputBuffer)
    sink.stop()
    channel.stop()

  }

  def writeAndVerify(channels: Seq[MemoryChannel], ssc: StreamingContext,
    outputBuffer: ArrayBuffer[Seq[SparkFlumePollingEvent]]) {
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val executor = Executors.newCachedThreadPool()
    val executorCompletion = new ExecutorCompletionService[Void](executor)
    channels.map(channel => {
      executorCompletion.submit(new TxnSubmitter(channel, clock))
    })
    for (i <- 0 until channels.size) {
      executorCompletion.take()
    }
    val startTime = System.currentTimeMillis()
    while (outputBuffer.size < 5 * channels.size &&
      System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      logInfo("output.size = " + outputBuffer.size)
      Thread.sleep(100)
    }
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    logInfo("Stopping context")
    ssc.stop()

    val flattenedBuffer = outputBuffer.flatten
    assert(flattenedBuffer.size === 25 * channels.size)
    var counter = 0
    for (k <- 0 until channels.size; i <- 0 until 25) {
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
    assert(counter === 25 * channels.size)
  }

  private class TxnSubmitter(channel: MemoryChannel, clock: ManualClock) extends Callable[Void] {
    override def call(): Void = {
      var t = 0
      for (i <- 0 until 5) {
        val tx = channel.getTransaction
        tx.begin()
        for (j <- 0 until 5) {
          channel.put(EventBuilder.withBody((channel.getName + " - " + String.valueOf(t)).getBytes(
            "utf-8"),
            Map[String, String]("test-" + t.toString -> "header")))
          t += 1
        }
        tx.commit()
        tx.close()
        Thread.sleep(500) // Allow some time for the events to reach
        clock.addToTime(batchDuration.milliseconds)
      }
      null
    }
  }

}
