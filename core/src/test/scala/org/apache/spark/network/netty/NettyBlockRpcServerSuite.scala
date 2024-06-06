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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import org.mockito.Mockito.mock

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.client.TransportClient
import org.apache.spark.serializer.JavaSerializer

class NettyBlockRpcServerSuite extends SparkFunSuite {

  test("SPARK-38830: Rethrow IllegalArgumentException due to `Unknown message type`") {
    val serializer = new JavaSerializer(new SparkConf)
    val server = new NettyBlockRpcServer("enhanced-rpc-server", serializer, null)
    val bytes = Array[Byte](100.toByte)
    val message = ByteBuffer.wrap(bytes)
    val client = mock(classOf[TransportClient])
    val m = intercept[IllegalArgumentException] {
      server.receive(client, message)
    }.getMessage
    assert(m.startsWith("Unknown message type: 100"))
  }

  test("SPARK-38830: Warn and ignore NegativeArraySizeException due to the corruption") {
    val serializer = new JavaSerializer(new SparkConf)
    val server = new NettyBlockRpcServer("enhanced-rpc-server", serializer, null)
    val bytes = Array[Byte](0.toByte, 0xFF.toByte, 0xFF.toByte, 0xFF.toByte, 0xFF.toByte)
    val message = ByteBuffer.wrap(bytes)
    val client = mock(classOf[TransportClient])
    server.receive(client, message)
  }

  test("SPARK-38830: Warn and ignore IndexOutOfBoundsException due to the corruption") {
    val serializer = new JavaSerializer(new SparkConf)
    val server = new NettyBlockRpcServer("enhanced-rpc-server", serializer, null)
    val bytes = Array[Byte](1.toByte)
    val message = ByteBuffer.wrap(bytes)
    val client = mock(classOf[TransportClient])
    server.receive(client, message)
  }
}
