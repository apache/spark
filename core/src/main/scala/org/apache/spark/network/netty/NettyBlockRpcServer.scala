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

import org.apache.spark.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.serializer.Serializer
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.network.server.{DefaultStreamManager, RpcHandler}
import org.apache.spark.storage.BlockId

import scala.collection.JavaConversions._

/** Request to read a set of blocks. Returns [[ShuffleStreamHandle]] to identify the stream. */
case class OpenBlocks(blockIds: Seq[BlockId])

/** Identifier for a fixed number of chunks to read from a stream created by [[OpenBlocks]]. */
case class ShuffleStreamHandle(streamId: Long, numChunks: Int)

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 */
class NettyBlockRpcServer(
    serializer: Serializer,
    streamManager: DefaultStreamManager,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  override def receive(messageBytes: Array[Byte], responseContext: RpcResponseCallback): Unit = {
    val ser = serializer.newInstance()
    val message = ser.deserialize[AnyRef](ByteBuffer.wrap(messageBytes))
    logTrace(s"Received request: $message")
    message match {
      case OpenBlocks(blockIds) =>
        val blocks: Seq[ManagedBuffer] = blockIds.map(blockManager.getBlockData)
        val streamId = streamManager.registerStream(blocks.iterator)
        responseContext.onSuccess(
          ser.serialize(new ShuffleStreamHandle(streamId, blocks.size)).array())
    }
  }
}
