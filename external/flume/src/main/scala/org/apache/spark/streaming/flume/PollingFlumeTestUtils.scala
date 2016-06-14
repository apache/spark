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

import java.nio.charset.StandardCharsets
import java.util.{Collections, List => JList, Map => JMap}
import java.util.concurrent._

import scala.collection.mutable.ArrayBuffer

import org.apache.flume.event.EventBuilder
import org.apache.flume.Context
import org.apache.flume.channel.MemoryChannel
import org.apache.flume.conf.Configurables

import org.apache.spark.streaming.flume.sink.{SparkSink, SparkSinkConfig}

/**
 * Share codes for Scala and Python unit tests
 */
private[flume] class PollingFlumeTestUtils {

  private val batchCount = 5
  val eventsPerBatch = 100
  private val totalEventsPerChannel = batchCount * eventsPerBatch
  private val channelCapacity = 5000

  def getTotalEvents: Int = totalEventsPerChannel * channels.size

  private val channels = new ArrayBuffer[MemoryChannel]
  private val sinks = new ArrayBuffer[SparkSink]

  /**
   * Start a sink and return the port of this sink
   */
  def startSingleSink(): Int = {
    channels.clear()
    sinks.clear()

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

    channels += (channel)
    sinks += sink

    sink.getPort()
  }

  /**
   * Start 2 sinks and return the ports
   */
  def startMultipleSinks(): Seq[Int] = {
    channels.clear()
    sinks.clear()

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

    sinks += sink
    sinks += sink2
    channels += channel
    channels += channel2

    sinks.map(_.getPort())
  }

  /**
   * Send data and wait until all data has been received
   */
  def sendDataAndEnsureAllDataHasBeenReceived(): Unit = {
    val executor = Executors.newCachedThreadPool()
    val executorCompletion = new ExecutorCompletionService[Void](executor)

    val latch = new CountDownLatch(batchCount * channels.size)
    sinks.foreach(_.countdownWhenBatchReceived(latch))

    channels.foreach { channel =>
      executorCompletion.submit(new TxnSubmitter(channel))
    }

    for (i <- 0 until channels.size) {
      executorCompletion.take()
    }

    latch.await(15, TimeUnit.SECONDS) // Ensure all data has been received.
  }

  /**
   * A Python-friendly method to assert the output
   */
  def assertOutput(
      outputHeaders: JList[JMap[String, String]], outputBodies: JList[String]): Unit = {
    require(outputHeaders.size == outputBodies.size)
    val eventSize = outputHeaders.size
    if (eventSize != totalEventsPerChannel * channels.size) {
      throw new AssertionError(
        s"Expected ${totalEventsPerChannel * channels.size} events, but was $eventSize")
    }
    var counter = 0
    for (k <- 0 until channels.size; i <- 0 until totalEventsPerChannel) {
      val eventBodyToVerify = s"${channels(k).getName}-$i"
      val eventHeaderToVerify: JMap[String, String] = Collections.singletonMap(s"test-$i", "header")
      var found = false
      var j = 0
      while (j < eventSize && !found) {
        if (eventBodyToVerify == outputBodies.get(j) &&
          eventHeaderToVerify == outputHeaders.get(j)) {
          found = true
          counter += 1
        }
        j += 1
      }
    }
    if (counter != totalEventsPerChannel * channels.size) {
      throw new AssertionError(
        s"111 Expected ${totalEventsPerChannel * channels.size} events, but was $counter")
    }
  }

  def assertChannelsAreEmpty(): Unit = {
    channels.foreach(assertChannelIsEmpty)
  }

  private def assertChannelIsEmpty(channel: MemoryChannel): Unit = {
    val queueRemaining = channel.getClass.getDeclaredField("queueRemaining")
    queueRemaining.setAccessible(true)
    val m = queueRemaining.get(channel).getClass.getDeclaredMethod("availablePermits")
    if (m.invoke(queueRemaining.get(channel)).asInstanceOf[Int] != channelCapacity) {
      throw new AssertionError(s"Channel ${channel.getName} is not empty")
    }
  }

  def close(): Unit = {
    sinks.foreach(_.stop())
    sinks.clear()
    channels.foreach(_.stop())
    channels.clear()
  }

  private class TxnSubmitter(channel: MemoryChannel) extends Callable[Void] {
    override def call(): Void = {
      var t = 0
      for (i <- 0 until batchCount) {
        val tx = channel.getTransaction
        tx.begin()
        for (j <- 0 until eventsPerBatch) {
          channel.put(EventBuilder.withBody(
            s"${channel.getName}-$t".getBytes(StandardCharsets.UTF_8),
            Collections.singletonMap(s"test-$t", "header")))
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
