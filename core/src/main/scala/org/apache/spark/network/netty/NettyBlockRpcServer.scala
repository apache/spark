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

import java.io.InputStream
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor}

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ChunkedByteBufferUtil, ManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks, StreamHandle, UploadBlock}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.ThreadUtils

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

  import NettyBlockRpcServer._
  private val streamManager = new OneForOneStreamManager()

  override def receive(
      client: TransportClient,
      rpcMessage: InputStream,
      responseContext: RpcResponseCallback): Unit = {
    val toDo: () => Unit = () => {
      val message = BlockTransferMessage.Decoder.fromDataInputStream(rpcMessage)
      logTrace(s"Received request: $message")
      message match {
        case openBlocks: OpenBlocks =>
          val blocks: Seq[ManagedBuffer] =
            openBlocks.blockIds.map(BlockId.apply).map(blockManager.getBlockData)
          val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
          logTrace(s"Registered streamId $streamId with ${blocks.size} buffers")
          val streamHandle = new StreamHandle(streamId, blocks.size)
          responseContext.onSuccess(streamHandle.toByteBuffer)

        case uploadBlock: UploadBlock =>
          // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
          val (level: StorageLevel, classTag: ClassTag[_]) = {
            serializer
              .newInstance()
              .deserialize(ChunkedByteBufferUtil.wrap(uploadBlock.metadata))
              .asInstanceOf[(StorageLevel, ClassTag[_])]
          }
          val data = uploadBlock.blockData
          val blockId = BlockId(uploadBlock.blockId)
          blockManager.putBlockData(blockId, data, level, classTag)
          responseContext.onSuccess(ChunkedByteBufferUtil.wrap())
      }
      Unit
    }
    receivedMessages.offer(ReceiveMessage(client, responseContext, toDo))
  }

  override def channelInactive(client: TransportClient): Unit = {
    val list = scala.collection.mutable.ListBuffer[ReceiveMessage]()
    receivedMessages.toArray(Array.empty[ReceiveMessage]).filter(_.client == client)
    var ms = receivedMessages.poll()
    while (ms != null) {
      if (ms.client != client) {
        list += ms
      }
      ms = receivedMessages.poll()
    }
    list.foreach(m => receivedMessages.offer(m))
  }

  override def getStreamManager(): StreamManager = streamManager

}

object NettyBlockRpcServer extends Logging {

  private val receivedMessages = new LinkedBlockingQueue[ReceiveMessage]

  private val threadpool: ThreadPoolExecutor = {
    val numThreads = 2
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "block-rpcServer-dispatcher")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  case class ReceiveMessage(client: TransportClient, responseContext: RpcResponseCallback,
    toDo: () => Unit)

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          val data = receivedMessages.take()
          try {
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              receivedMessages.offer(PoisonPill)
              return
            }
            data.toDo()
          } catch {
            case NonFatal(e) =>
              data.responseContext.onFailure(e)
              logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new ReceiveMessage(null, null, null)
}
