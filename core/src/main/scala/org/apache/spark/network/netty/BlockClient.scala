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

import java.io.Closeable
import java.util.concurrent.TimeoutException

import io.netty.channel.{ChannelFuture, ChannelFutureListener}

import org.apache.spark.Logging
import org.apache.spark.network.BlockFetchingListener


/**
 * Client for [[NettyBlockTransferService]]. The connection to server must have been established
 * using [[BlockClientFactory]] before instantiating this.
 *
 * This class is used to make requests to the server , while [[BlockClientHandler]] is responsible
 * for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 *
 * @param cf the ChannelFuture for the connection.
 * @param handler [[BlockClientHandler]] for handling outstanding requests.
 */
@throws[TimeoutException]
private[netty]
class BlockClient(cf: ChannelFuture, handler: BlockClientHandler) extends Closeable with Logging {

  private[this] val serverAddr = cf.channel().remoteAddress().toString

  def isActive: Boolean = cf.channel().isActive

  /**
   * Ask the remote server for a sequence of blocks, and execute the callback.
   *
   * Note that this is asynchronous and returns immediately. Upstream caller should throttle the
   * rate of fetching; otherwise we could run out of memory due to large outstanding fetches.
   *
   * @param blockIds sequence of block ids to fetch.
   * @param listener callback to fire on fetch success / failure.
   */
  def fetchBlocks(blockIds: Seq[String], listener: BlockFetchingListener): Unit = {
    var startTime: Long = 0
    logTrace {
      startTime = System.nanoTime()
      s"Sending request $blockIds to $serverAddr"
    }

    blockIds.foreach { blockId =>
      handler.addRequest(blockId, listener)
    }

    cf.channel().writeAndFlush(BlockFetchRequest(blockIds)).addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) {
          logTrace {
            val timeTaken = (System.nanoTime() - startTime).toDouble / 1000000
            s"Sending request $blockIds to $serverAddr took $timeTaken ms"
          }
        } else {
          // Fail all blocks.
          val errorMsg =
            s"Failed to send request $blockIds to $serverAddr: ${future.cause.getMessage}"
          logError(errorMsg, future.cause)
          blockIds.foreach { blockId =>
            handler.removeRequest(blockId)
            listener.onBlockFetchFailure(blockId, new RuntimeException(errorMsg))
          }
        }
      }
    })
  }

  /** Close the connection. This does NOT block till the connection is closed. */
  def close(): Unit = cf.channel().close()
}
