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

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.apache.flume.source.avro.{AvroFlumeEvent, AvroSourceProtocol}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{TestOutputStream, StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.util.ManualClock

class FlumeStreamSuite extends TestSuiteBase {

  val testPort = 9999

  test("flume input stream") {
    // Set up the streaming context and input streams
    val ssc = new StreamingContext(conf, batchDuration)
    val flumeStream = FlumeUtils.createStream(ssc, "localhost", testPort, StorageLevel.MEMORY_AND_DISK)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumeEvent]]
      with SynchronizedBuffer[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    outputStream.register()
    ssc.start()

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3, 4, 5)
    Thread.sleep(1000)
    val transceiver = new NettyTransceiver(new InetSocketAddress("localhost", testPort))
    val client = SpecificRequestor.getClient(
      classOf[AvroSourceProtocol], transceiver)

    for (i <- 0 until input.size) {
      val event = new AvroFlumeEvent
      event.setBody(ByteBuffer.wrap(input(i).toString.getBytes()))
      event.setHeaders(Map[CharSequence, CharSequence]("test" -> "header"))
      client.append(event)
      Thread.sleep(500)
      clock.addToTime(batchDuration.milliseconds)
    }

    val startTime = System.currentTimeMillis()
    while (outputBuffer.size < input.size && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      logInfo("output.size = " + outputBuffer.size + ", input.size = " + input.size)
      Thread.sleep(100)
    }
    Thread.sleep(1000)
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    logInfo("Stopping context")
    ssc.stop()

    val decoder = Charset.forName("UTF-8").newDecoder()

    assert(outputBuffer.size === input.length)
    for (i <- 0 until outputBuffer.size) {
      assert(outputBuffer(i).size === 1)
      val str = decoder.decode(outputBuffer(i).head.event.getBody)
      assert(str.toString === input(i).toString)
      assert(outputBuffer(i).head.event.getHeaders.get("test") === "header")
    }
  }
}
