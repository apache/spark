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
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockBatchId, ShuffleBlockId, StorageLevel}

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

  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = try {
      BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    } catch {
      case e: IllegalArgumentException if e.getMessage.startsWith("Unknown message type") =>
        logWarning(s"This could be a corrupted RPC message (capacity: ${rpcMessage.capacity()}) " +
          s"from ${client.getSocketAddress}. Please use `spark.authenticate.*` configurations " +
          "in case of security incidents.")
        throw e

      case _: IndexOutOfBoundsException | _: NegativeArraySizeException =>
        // Netty may throw non-'IOException's for corrupted buffers. In this case,
        // we ignore the entire message with warnings because we cannot trust any contents.
        logWarning(s"Ignored a corrupted RPC message (capacity: ${rpcMessage.capacity()}) " +
          s"from ${client.getSocketAddress}. Please use `spark.authenticate.*` configurations " +
          "in case of security incidents.")
        return
    }
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>
        val blocksNum = openBlocks.blockIds.length
        val blocks = (0 until blocksNum).map { i =>
          val blockId = BlockId.apply(openBlocks.blockIds(i))
          assert(!blockId.isInstanceOf[ShuffleBlockBatchId],
            "Continuous shuffle block fetching only works for new fetch protocol.")
          blockManager.getLocalBlockData(blockId)
        }
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)

      case fetchShuffleBlocks: FetchShuffleBlocks =>
        val blocks = fetchShuffleBlocks.mapIds.zipWithIndex.flatMap { case (mapId, index) =>
          if (!fetchShuffleBlocks.batchFetchEnabled) {
            fetchShuffleBlocks.reduceIds(index).map { reduceId =>
              blockManager.getLocalBlockData(
                ShuffleBlockId(fetchShuffleBlocks.shuffleId, mapId, reduceId))
            }
          } else {
            val startAndEndId = fetchShuffleBlocks.reduceIds(index)
            if (startAndEndId.length != 2) {
              throw new IllegalStateException(s"Invalid shuffle fetch request when batch mode " +
                s"is enabled: $fetchShuffleBlocks")
            }
            Array(blockManager.getLocalBlockData(
              ShuffleBlockBatchId(
                fetchShuffleBlocks.shuffleId, mapId, startAndEndId(0), startAndEndId(1))))
          }
        }

        val numBlockIds = if (fetchShuffleBlocks.batchFetchEnabled) {
          fetchShuffleBlocks.mapIds.length
        } else {
          fetchShuffleBlocks.reduceIds.map(_.length).sum
        }

        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $numBlockIds buffers")
        responseContext.onSuccess(
          new StreamHandle(streamId, numBlockIds).toByteBuffer)

      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        val (level, classTag) = deserializeMetadata(uploadBlock.metadata)
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)
        logDebug(s"Receiving replicated block $blockId with level ${level} " +
          s"from ${client.getSocketAddress}")
        val blockStored = blockManager.putBlockData(blockId, data, level, classTag)
        if (blockStored) {
          responseContext.onSuccess(ByteBuffer.allocate(0))
        } else {
          val exception = new Exception(s"Upload block for $blockId failed. This mostly happens " +
            s"when there is not sufficient space available to store the block.")
          responseContext.onFailure(exception)
        }

      case getLocalDirs: GetLocalDirsForExecutors =>
        val isIncorrectAppId = getLocalDirs.appId != appId
        val execNum = getLocalDirs.execIds.length
        if (isIncorrectAppId || execNum != 1) {
          val errorMsg = "Invalid GetLocalDirsForExecutors request: " +
            s"${if (isIncorrectAppId) s"incorrect application id: ${getLocalDirs.appId};"}" +
            s"${if (execNum != 1) s"incorrect executor number: $execNum (expected 1);"}"
          responseContext.onFailure(new IllegalStateException(errorMsg))
        } else {
          val expectedExecId = blockManager.asInstanceOf[BlockManager].executorId
          val actualExecId = getLocalDirs.execIds.head
          if (actualExecId != expectedExecId) {
            responseContext.onFailure(new IllegalStateException(
              s"Invalid executor id: $actualExecId, expected $expectedExecId."))
          } else {
            responseContext.onSuccess(new LocalDirsForExecutors(
              Map(actualExecId -> blockManager.getLocalDiskDirs).asJava).toByteBuffer)
          }
        }

      case diagnose: DiagnoseCorruption =>
        val cause = blockManager.diagnoseShuffleBlockCorruption(
          ShuffleBlockId(diagnose.shuffleId, diagnose.mapId, diagnose.reduceId ),
          diagnose.checksum,
          diagnose.algorithm)
        responseContext.onSuccess(new CorruptionCause(cause).toByteBuffer)
    }
  }

  override def receiveStream(
      client: TransportClient,
      messageHeader: ByteBuffer,
      responseContext: RpcResponseCallback): StreamCallbackWithID = {
    val message =
      BlockTransferMessage.Decoder.fromByteBuffer(messageHeader).asInstanceOf[UploadBlockStream]
    val (level, classTag) = deserializeMetadata(message.metadata)
    val blockId = BlockId(message.blockId)
    logDebug(s"Receiving replicated block $blockId with level ${level} as stream " +
      s"from ${client.getSocketAddress}")
    // This will return immediately, but will setup a callback on streamData which will still
    // do all the processing in the netty thread.
    blockManager.putBlockDataAsStream(blockId, level, classTag)
  }

  private def deserializeMetadata[T](metadata: Array[Byte]): (StorageLevel, ClassTag[T]) = {
    serializer
      .newInstance()
      .deserialize(ByteBuffer.wrap(metadata))
      .asInstanceOf[(StorageLevel, ClassTag[T])]
  }

  override def getStreamManager(): StreamManager = streamManager
}
