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

import java.util.concurrent.TimeoutException

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelFuture, ChannelFutureListener, ChannelInitializer, ChannelOption}

import org.apache.spark.Logging
import org.apache.spark.network.BlockFetchingListener


/**
 * Client for [[NettyBlockTransferService]]. Use [[BlockClientFactory]] to
 * instantiate this client.
 *
 * The constructor blocks until a connection is successfully established.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
@throws[TimeoutException]
private[netty]
class BlockClient(factory: BlockClientFactory, hostname: String, port: Int)
  extends Logging {

  private val handler = new BlockClientHandler
  private val encoder = new ClientRequestEncoder
  private val decoder = new ServerResponseDecoder

  /** Netty Bootstrap for creating the TCP connection. */
  private val bootstrap: Bootstrap = {
    val b = new Bootstrap
    b.group(factory.workerGroup)
      .channel(factory.socketChannelClass)
      // Use pooled buffers to reduce temporary buffer allocation
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      .option[Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, factory.conf.connectTimeoutMs)

    b.handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline
          .addLast("clientRequestEncoder", encoder)
          .addLast("frameDecoder", ProtocolUtils.createFrameDecoder())
          .addLast("serverResponseDecoder", decoder)
          .addLast("handler", handler)
      }
    })
    b
  }

  /** Netty ChannelFuture for the connection. */
  private val cf: ChannelFuture = bootstrap.connect(hostname, port)
  if (!cf.awaitUninterruptibly(factory.conf.connectTimeoutMs)) {
    throw new TimeoutException(
      s"Connecting to $hostname:$port timed out (${factory.conf.connectTimeoutMs} ms)")
  }

  /**
   * Ask the remote server for a sequence of blocks, and execute the callback.
   *
   * Note that this is asynchronous and returns immediately. Upstream caller should throttle the
   * rate of fetching; otherwise we could run out of memory.
   *
   * @param blockIds sequence of block ids to fetch.
   * @param listener callback to fire on fetch success / failure.
   */
  def fetchBlocks(blockIds: Seq[String], listener: BlockFetchingListener): Unit = {
    var startTime: Long = 0
    logTrace {
      startTime = System.nanoTime
      s"Sending request $blockIds to $hostname:$port"
    }

    blockIds.foreach { blockId =>
      handler.addRequest(blockId, listener)
    }

    cf.channel().writeAndFlush(BlockFetchRequest(blockIds)).addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) {
          logTrace {
            val timeTaken = (System.nanoTime - startTime).toDouble / 1000000
            s"Sending request $blockIds to $hostname:$port took $timeTaken ms"
          }
        } else {
          // Fail all blocks.
          val errorMsg =
            s"Failed to send request $blockIds to $hostname:$port: ${future.cause.getMessage}"
          logError(errorMsg, future.cause)
          blockIds.foreach { blockId =>
            handler.removeRequest(blockId)
          }
          listener.onBlockFetchFailure(new RuntimeException(errorMsg))
        }
      }
    })
  }

  def waitForClose(): Unit = {
    cf.channel().closeFuture().sync()
  }

  def close(): Unit = cf.channel().close()
}
