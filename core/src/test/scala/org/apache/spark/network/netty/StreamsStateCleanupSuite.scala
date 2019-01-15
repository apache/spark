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

import io.netty.channel.Channel
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportResponseHandler}
import org.apache.spark.network.server.OneForOneStreamManager
import org.apache.spark.network.shuffle.protocol.OpenBlocks
import org.apache.spark.serializer.Serializer

class StreamsStateCleanupSuite extends SparkFunSuite with MockitoSugar {

  test("test streams are removed correctly") {
    val streamManager = new OneForOneStreamManager()
    val reverseClient = new TransportClient(mock[Channel], mock[TransportResponseHandler])
    val rpcHandler
      = new NettyBlockRpcServer("app0", mock[Serializer], mock[BlockDataManager])

    val openBlocks = new OpenBlocks("app0", "exec1",
      Array[String]("shuffle_0_0_0", "shuffle_0_0_1"))
      .toByteBuffer
    val callback = mock[RpcResponseCallback]

    // Open blocks
    rpcHandler.receive(reverseClient, openBlocks, callback)
    assert(streamManager.getStreamCount === 1)

    // Connection closed before any FetchChunk request received
    streamManager.connectionTerminated(reverseClient.getChannel)
    assert(streamManager.getStreamCount === 0)
  }
}
