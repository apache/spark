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

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.base.Charsets
import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.apache.commons.lang3.RandomUtils
import org.apache.flume.source.avro
import org.apache.flume.source.avro.{AvroFlumeEvent, AvroSourceProtocol}
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.socket.SocketChannel
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.compression._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext, TestOutputStream}
import org.apache.spark.util.Utils

class FlumeStreamSuite extends FunSuite with BeforeAndAfter with Matchers with Logging {
  val conf = new SparkConf().setMaster("local[4]").setAppName("FlumeStreamSuite")

  var ssc: StreamingContext = null
  var transceiver: NettyTransceiver = null

  after {
    if (ssc != null) {
      ssc.stop()
    }
    if (transceiver != null) {
      transceiver.close()
    }
  }

  test("flume input stream") {
    testFlumeStream(testCompression = false)
  }

  test("flume input compressed stream") {
    testFlumeStream(testCompression = true)
  }

  /** Run test on flume stream */
  private def testFlumeStream(testCompression: Boolean): Unit = {
    val input = (1 to 100).map { _.toString }
    val testPort = findFreePort()
    val outputBuffer = startContext(testPort, testCompression)
    writeAndVerify(input, testPort, outputBuffer, testCompression)
  }

  /** Find a free port */
  private def findFreePort(): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, conf)._2
  }

  /** Setup and start the streaming context */
  private def startContext(
      testPort: Int, testCompression: Boolean): (ArrayBuffer[Seq[SparkFlumeEvent]]) = {
    ssc = new StreamingContext(conf, Milliseconds(200))
    val flumeStream = FlumeUtils.createStream(
      ssc, "localhost", testPort, StorageLevel.MEMORY_AND_DISK, testCompression)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumeEvent]]
      with SynchronizedBuffer[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    outputStream.register()
    ssc.start()
    outputBuffer
  }

  /** Send data to the flume receiver and verify whether the data was received */
  private def writeAndVerify(
      input: Seq[String],
      testPort: Int,
      outputBuffer: ArrayBuffer[Seq[SparkFlumeEvent]],
      enableCompression: Boolean
    ) {
    val testAddress = new InetSocketAddress("localhost", testPort)

    val inputEvents = input.map { item =>
      val event = new AvroFlumeEvent
      event.setBody(ByteBuffer.wrap(item.getBytes(Charsets.UTF_8)))
      event.setHeaders(Map[CharSequence, CharSequence]("test" -> "header"))
      event
    }

    eventually(timeout(10 seconds), interval(100 milliseconds)) {
      // if last attempted transceiver had succeeded, close it
      if (transceiver != null) {
        transceiver.close()
        transceiver = null
      }

      // Create transceiver
      transceiver = {
        if (enableCompression) {
          new NettyTransceiver(testAddress, new CompressionChannelFactory(6))
        } else {
          new NettyTransceiver(testAddress)
        }
      }

      // Create Avro client with the transceiver
      val client = SpecificRequestor.getClient(classOf[AvroSourceProtocol], transceiver)
      client should not be null

      // Send data
      val status = client.appendBatch(inputEvents.toList)
      status should be (avro.Status.OK)
    }
    
    eventually(timeout(10 seconds), interval(100 milliseconds)) {
      val outputEvents = outputBuffer.flatten.map { _.event }
      outputEvents.foreach {
        event =>
          event.getHeaders.get("test") should be("header")
      }
      val output = outputEvents.map(event => new String(event.getBody.array(), Charsets.UTF_8))
      output should be (input)
    }
  }

  /** Class to create socket channel with compression */
  private class CompressionChannelFactory(compressionLevel: Int) extends NioClientSocketChannelFactory {
    override def newChannel(pipeline: ChannelPipeline): SocketChannel = {
      val encoder = new ZlibEncoder(compressionLevel)
      pipeline.addFirst("deflater", encoder)
      pipeline.addFirst("inflater", new ZlibDecoder())
      super.newChannel(pipeline)
    }
  }
}
