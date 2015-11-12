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
import java.util.{List => JList}
import java.util.Collections

import scala.collection.JavaConverters._

import com.google.common.base.Charsets.UTF_8
import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.apache.commons.lang3.RandomUtils
import org.apache.flume.source.avro
import org.apache.flume.source.avro.{AvroSourceProtocol, AvroFlumeEvent}
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.socket.SocketChannel
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.compression.{ZlibDecoder, ZlibEncoder}

import org.apache.spark.util.Utils
import org.apache.spark.SparkConf

/**
 * Share codes for Scala and Python unit tests
 */
private[flume] class FlumeTestUtils {

  private var transceiver: NettyTransceiver = null

  private val testPort: Int = findFreePort()

  def getTestPort(): Int = testPort

  /** Find a free port */
  private def findFreePort(): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, new SparkConf())._2
  }

  /** Send data to the flume receiver */
  def writeInput(input: JList[String], enableCompression: Boolean): Unit = {
    val testAddress = new InetSocketAddress("localhost", testPort)

    val inputEvents = input.asScala.map { item =>
      val event = new AvroFlumeEvent
      event.setBody(ByteBuffer.wrap(item.getBytes(UTF_8)))
      event.setHeaders(Collections.singletonMap("test", "header"))
      event
    }

    // if last attempted transceiver had succeeded, close it
    close()

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
    if (client == null) {
      throw new AssertionError("Cannot create client")
    }

    // Send data
    val status = client.appendBatch(inputEvents.asJava)
    if (status != avro.Status.OK) {
      throw new AssertionError("Sent events unsuccessfully")
    }
  }

  def close(): Unit = {
    if (transceiver != null) {
      transceiver.close()
      transceiver = null
    }
  }

  /** Class to create socket channel with compression */
  private class CompressionChannelFactory(compressionLevel: Int)
    extends NioClientSocketChannelFactory {

    override def newChannel(pipeline: ChannelPipeline): SocketChannel = {
      val encoder = new ZlibEncoder(compressionLevel)
      pipeline.addFirst("deflater", encoder)
      pipeline.addFirst("inflater", new ZlibDecoder())
      super.newChannel(pipeline)
    }
  }

}
