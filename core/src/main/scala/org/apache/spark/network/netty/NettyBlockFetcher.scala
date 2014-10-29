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
import java.util

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.network.BlockFetchingListener
import org.apache.spark.network.netty.NettyMessages._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, ChunkReceivedCallback, TransportClient}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

/**
 * Responsible for holding the state for a request for a single set of blocks. This assumes that
 * the chunks will be returned in the same order as requested, and that there will be exactly
 * one chunk per block.
 *
 * Upon receipt of any block, the listener will be called back. Upon failure part way through,
 * the listener will receive a failure callback for each outstanding block.
 */
class NettyBlockFetcher(
    serializer: Serializer,
    client: TransportClient,
    blockIds: Seq[String],
    listener: BlockFetchingListener)
  extends Logging {

  require(blockIds.nonEmpty)

  private val ser = serializer.newInstance()

  private var streamHandle: ShuffleStreamHandle = _

  private val chunkCallback = new ChunkReceivedCallback {
    // On receipt of a chunk, pass it upwards as a block.
    def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = Utils.logUncaughtExceptions {
      listener.onBlockFetchSuccess(blockIds(chunkIndex), buffer)
    }

    // On receipt of a failure, fail every block from chunkIndex onwards.
    def onFailure(chunkIndex: Int, e: Throwable): Unit = {
      blockIds.drop(chunkIndex).foreach { blockId =>
        listener.onBlockFetchFailure(blockId, e);
      }
    }
  }

  /** Begins the fetching process, calling the listener with every block fetched. */
  def start(): Unit = {
    // Send the RPC to open the given set of blocks. This will return a ShuffleStreamHandle.
    client.sendRpc(ser.serialize(OpenBlocks(blockIds.map(BlockId.apply))).array(),
      new RpcResponseCallback {
        override def onSuccess(response: Array[Byte]): Unit = {
          try {
            streamHandle = ser.deserialize[ShuffleStreamHandle](ByteBuffer.wrap(response))
            logTrace(s"Successfully opened block set: $streamHandle! Preparing to fetch chunks.")

            // Immediately request all chunks -- we expect that the total size of the request is
            // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
            for (i <- 0 until streamHandle.numChunks) {
              client.fetchChunk(streamHandle.streamId, i, chunkCallback)
            }
          } catch {
            case e: Exception =>
              logError("Failed while starting block fetches", e)
              blockIds.foreach(blockId => Utils.tryLog(listener.onBlockFetchFailure(blockId, e)))
          }
        }

        override def onFailure(e: Throwable): Unit = {
          logError("Failed while starting block fetches", e)
          blockIds.foreach(blockId => Utils.tryLog(listener.onBlockFetchFailure(blockId, e)))
        }
      })
  }
}
