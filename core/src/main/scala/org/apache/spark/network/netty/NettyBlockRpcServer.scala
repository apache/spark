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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{WrappedLargeByteBuffer, LargeByteBufferHelper, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}

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

  private val streamManager = new OneForOneStreamManager()

  private val openRequests = new ConcurrentHashMap[String,PartialBlockUploadHandler]()

  override def receive(
      client: TransportClient,
      messageBytes: Array[Byte],
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteArray(messageBytes)
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>
        val blocks: Seq[ManagedBuffer] =
          openBlocks.blockIds.map(BlockId.apply).map(blockManager.getBlockData)
        val streamId = streamManager.registerStream(blocks.iterator.asJava)
        logTrace(s"Registered streamId $streamId with ${blocks.size} buffers")
        responseContext.onSuccess(new StreamHandle(streamId, blocks.size).toByteArray)

      case uploadBlock: UploadBlock =>
        // StorageLevel is serialized as bytes using our JavaSerializer.
        val level: StorageLevel =
          serializer.newInstance().deserialize(ByteBuffer.wrap(uploadBlock.metadata))
        val data = new NioManagedBuffer(LargeByteBufferHelper.asLargeByteBuffer(uploadBlock.blockData))
        logTrace("putting block into our block manager: " + blockManager)
        blockManager.putBlockData(BlockId(uploadBlock.blockId), data, level)
        responseContext.onSuccess(new Array[Byte](0))

      case uploadPartialBock: UploadPartialBlock =>
        logTrace("received upload partial block: " + uploadPartialBock)
        val storageLevel: StorageLevel =
          serializer.newInstance().deserialize(ByteBuffer.wrap(uploadPartialBock.metadata))
        logTrace("open requests = " + openRequests)
        openRequests.putIfAbsent(uploadPartialBock.blockId,
          new PartialBlockUploadHandler(uploadPartialBock.blockId, storageLevel,
            uploadPartialBock.nTotalBlockChunks))
        val handler = openRequests.get(uploadPartialBock.blockId)
        handler.addPartialBlock(uploadPartialBock, storageLevel)
        responseContext.onSuccess(new Array[Byte](0))
    }
  }


  private class PartialBlockUploadHandler(
    val blockId: String,
    val storageLevel: StorageLevel,
    val nTotalBlockChunks: Int
  ) {
    val chunks = new Array[Array[Byte]](nTotalBlockChunks)
    var nMissing = nTotalBlockChunks

    def addPartialBlock(partial: UploadPartialBlock, storageLevel: StorageLevel): Unit = {
      if (partial.nTotalBlockChunks != nTotalBlockChunks) {
        throw new IllegalArgumentException(s"received incompatible UploadPartialBlock: expecting " +
          s"$nTotalBlockChunks total chunks, but new msg has ${partial.nTotalBlockChunks}")
      }
      if (storageLevel != this.storageLevel) {
        throw new IllegalArgumentException(s"received incompatible UploadPartialBlock: expecting " +
          s"${this.storageLevel}, but new message has $storageLevel")
      }
      logTrace("received partial msg")
      chunks(partial.blockChunkIndex) = partial.blockData
      nMissing -= 1
      logTrace("nmissing = " + nMissing)
      if (nMissing == 0) {
        //we've got all the blocks -- now we can insert into the block manager
        logTrace("received all partial blocks for " + blockId)
        val data = new NioManagedBuffer(new WrappedLargeByteBuffer(chunks.map{ByteBuffer.wrap}))
        blockManager.putBlockData(BlockId(blockId), data, storageLevel)
        openRequests.remove(blockId)
      }
    }
  }

  override def getStreamManager(): StreamManager = streamManager
}
