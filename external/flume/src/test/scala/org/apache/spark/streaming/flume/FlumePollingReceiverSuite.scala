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

import org.apache.spark.streaming.{TestSuiteBase, TestOutputStream, StreamingContext}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import org.apache.spark.streaming.util.ManualClock
import java.nio.charset.Charset
import org.apache.flume.channel.MemoryChannel
import org.apache.flume.Context
import org.apache.flume.conf.Configurables
import org.apache.spark.flume.sink.{SparkSinkConfig, SparkSink}
import scala.collection.JavaConversions._
import org.apache.flume.event.EventBuilder
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class FlumePollingReceiverSuite extends TestSuiteBase {

  val testPort = 9999

  test("flume polling test") {
    // Set up the streaming context and input streams
    val ssc = new StreamingContext(conf, batchDuration)
    val flumeStream: ReceiverInputDStream[SparkPollingEvent] =
      FlumeUtils.createPollingStream(ssc, "localhost", testPort, 100, 5,
        StorageLevel.MEMORY_AND_DISK)
    val outputBuffer = new ArrayBuffer[Seq[SparkPollingEvent]]
      with SynchronizedBuffer[Seq[SparkPollingEvent]]
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

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3, 4, 5)
    for (i <- 0 until 5) {
      val tx = channel.getTransaction
      tx.begin()
      for (j <- 0 until input.size) {
        channel.put(EventBuilder.withBody(
          (String.valueOf(i) + input(j)).getBytes("utf-8"),
          Map[String, String]("test-" + input(j).toString -> "header")))
      }
      tx.commit()
      tx.close()
      Thread.sleep(500) // Allow some time for the events to reach
      clock.addToTime(batchDuration.milliseconds)
    }
    val startTime = System.currentTimeMillis()
    while (outputBuffer.size < 5 && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      logInfo("output.size = " + outputBuffer.size)
      Thread.sleep(100)
    }
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    logInfo("Stopping context")
    ssc.stop()

    val decoder = Charset.forName("UTF-8").newDecoder()

    assert(outputBuffer.size === 5)
    var counter = 0
    for (i <- 0 until outputBuffer.size;
         j <- 0 until outputBuffer(i).size) {
      counter += 1
      val eventToVerify = outputBuffer(i)(j).event
      val str = decoder.decode(eventToVerify.getBody)
      assert(str.toString === (String.valueOf(i) + input(j)))
      assert(eventToVerify.getHeaders.get("test-" + input(j).toString) === "header")
    }
  }

}
