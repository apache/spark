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

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.ShuffleStreamHandle
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}

object NettyMessages {
  /** Request to read a set of blocks. Returns [[ShuffleStreamHandle]] to identify the stream. */
  case class OpenBlocks(blockIds: Seq[BlockId])

  /** Request to upload a block with a certain StorageLevel. Returns nothing (empty byte array). */
  case class UploadBlock(blockId: BlockId, blockData: Array[Byte], level: StorageLevel)
}

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  import NettyMessages._

  private val streamManager = new OneForOneStreamManager()

  override def receive(
      client: TransportClient,
      messageBytes: Array[Byte],
      responseContext: RpcResponseCallback): Unit = {
    val ser = serializer.newInstance()
    val message = ser.deserialize[AnyRef](ByteBuffer.wrap(messageBytes))
    logTrace(s"Received request: $message")

    message match {
      case OpenBlocks(blockIds) =>
        val blocks: Seq[ManagedBuffer] = blockIds.map(blockManager.getBlockData)
        val streamId = streamManager.registerStream(blocks.iterator)
        logTrace(s"Registered streamId $streamId with ${blocks.size} buffers")
        responseContext.onSuccess(
          ser.serialize(new ShuffleStreamHandle(streamId, blocks.size)).array())

      case UploadBlock(blockId, blockData, level) =>
        blockManager.putBlockData(blockId, new NioManagedBuffer(ByteBuffer.wrap(blockData)), level)
        responseContext.onSuccess(new Array[Byte](0))
    }
  }

  override def getStreamManager(): StreamManager = streamManager
}
