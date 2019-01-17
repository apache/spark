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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  private def mergeShuffleBlockIds(iter: Iterator[String]) = {
    val arrayShuffleBlockIds = new ArrayBuffer[ArrayShuffleBlockId]
    var blockIds = new ArrayBuffer[ShuffleBlockId]
    while (iter.hasNext) {
      val curBlockId = BlockId(iter.next()).asInstanceOf[ShuffleBlockId]
      if (blockIds.isEmpty || curBlockId.mapId == blockIds.head.mapId) {
        blockIds += curBlockId
      } else {
        arrayShuffleBlockIds += ArrayShuffleBlockId(blockIds)
        blockIds = new ArrayBuffer[ShuffleBlockId]
        blockIds += curBlockId
      }
    }
    if (blockIds.nonEmpty) {
      arrayShuffleBlockIds += ArrayShuffleBlockId(blockIds)
    }
    arrayShuffleBlockIds.toArray
  }

  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>
        val blockIds =
          if (openBlocks.fetchContinuousShuffleBlocksInBatch) {
            mergeShuffleBlockIds(openBlocks.blockIds.iterator)
          } else {
            openBlocks.blockIds.map(BlockId.apply)
          }
        val blocksNum = blockIds.length
        val blocks = for (i <- (0 until blocksNum).view)
          yield blockManager.getBlockData(blockIds(i))
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)

      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        val (level: StorageLevel, classTag: ClassTag[_]) = {
          serializer
            .newInstance()
            .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
            .asInstanceOf[(StorageLevel, ClassTag[_])]
        }
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)
        logDebug(s"Receiving replicated block $blockId with level ${level} " +
          s"from ${client.getSocketAddress}")
        blockManager.putBlockData(blockId, data, level, classTag)
        responseContext.onSuccess(ByteBuffer.allocate(0))
    }
  }

  override def receiveStream(
      client: TransportClient,
      messageHeader: ByteBuffer,
      responseContext: RpcResponseCallback): StreamCallbackWithID = {
    val message =
      BlockTransferMessage.Decoder.fromByteBuffer(messageHeader).asInstanceOf[UploadBlockStream]
    val (level: StorageLevel, classTag: ClassTag[_]) = {
      serializer
        .newInstance()
        .deserialize(ByteBuffer.wrap(message.metadata))
        .asInstanceOf[(StorageLevel, ClassTag[_])]
    }
    val blockId = BlockId(message.blockId)
    logDebug(s"Receiving replicated block $blockId with level ${level} as stream " +
      s"from ${client.getSocketAddress}")
    // This will return immediately, but will setup a callback on streamData which will still
    // do all the processing in the netty thread.
    blockManager.putBlockDataAsStream(blockId, level, classTag)
  }

  override def getStreamManager(): StreamManager = streamManager
}
