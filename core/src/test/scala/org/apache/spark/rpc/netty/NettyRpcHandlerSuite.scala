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

package org.apache.spark.rpc.netty

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import io.netty.channel.Channel
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.client.{TransportClient, TransportResponseHandler}
import org.apache.spark.network.server.StreamManager
import org.apache.spark.rpc._

class NettyRpcHandlerSuite extends SparkFunSuite {

  val env = mock(classOf[NettyRpcEnv])
  val sm = mock(classOf[StreamManager])
  when(env.deserialize(any(classOf[TransportClient]), any(classOf[ByteBuffer]))(any()))
    .thenReturn(new RequestMessage(RpcAddress("localhost", 12345), null, null))

  test("receive") {
    val dispatcher = mock(classOf[Dispatcher])
    val nettyRpcHandler = new NettyRpcHandler(dispatcher, env, sm)

    val channel = mock(classOf[Channel])
    val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
    when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 40000))
    nettyRpcHandler.channelActive(client)

    verify(dispatcher, times(1)).postToAll(RemoteProcessConnected(RpcAddress("localhost", 40000)))
  }

  test("connectionTerminated") {
    val dispatcher = mock(classOf[Dispatcher])
    val nettyRpcHandler = new NettyRpcHandler(dispatcher, env, sm)

    val channel = mock(classOf[Channel])
    val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
    when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 40000))
    nettyRpcHandler.channelActive(client)

    when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 40000))
    nettyRpcHandler.channelInactive(client)

    verify(dispatcher, times(1)).postToAll(RemoteProcessConnected(RpcAddress("localhost", 40000)))
    verify(dispatcher, times(1)).postToAll(
      RemoteProcessDisconnected(RpcAddress("localhost", 40000)))
  }

}
