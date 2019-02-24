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

package org.apache.spark.network.server;

import java.net.SocketAddress;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.Encodable;

import static org.apache.spark.network.util.NettyUtils.*;

/**
 * A dedicated ChannelHandler for processing ChunkFetchRequest messages. When sending response
 * of ChunkFetchRequest messages to the clients, the thread performing the I/O on the underlying
 * channel could potentially be blocked due to disk contentions. If several hundreds of clients
 * send ChunkFetchRequest to the server at the same time, it could potentially occupying all
 * threads from TransportServer's default EventLoopGroup for waiting for disk reads before it
 * can send the block data back to the client as part of the ChunkFetchSuccess messages. As a
 * result, it would leave no threads left to process other RPC messages, which takes much less
 * time to process, and could lead to client timing out on either performing SASL authentication,
 * registering executors, or waiting for response for an OpenBlocks messages.
 */
public class ChunkFetchRequestHandler extends SimpleChannelInboundHandler<ChunkFetchRequest> {
  private static final Logger logger = LoggerFactory.getLogger(ChunkFetchRequestHandler.class);

  private final TransportClient client;
  private final StreamManager streamManager;
  /** The max number of chunks being transferred and not finished yet. */
  private final long maxChunksBeingTransferred;

  public ChunkFetchRequestHandler(
      TransportClient client,
      StreamManager streamManager,
      Long maxChunksBeingTransferred) {
    this.client = client;
    this.streamManager = streamManager;
    this.maxChunksBeingTransferred = maxChunksBeingTransferred;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Exception in connection from " + getRemoteAddress(ctx.channel()), cause);
    ctx.close();
  }

  @Override
  protected void channelRead0(
      ChannelHandlerContext ctx,
      final ChunkFetchRequest msg) throws Exception {
    Channel channel = ctx.channel();
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
        msg.streamChunkId);
    }
    long chunksBeingTransferred = streamManager.chunksBeingTransferred();
    if (chunksBeingTransferred >= maxChunksBeingTransferred) {
      logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
        chunksBeingTransferred, maxChunksBeingTransferred);
      channel.close();
      return;
    }
    ManagedBuffer buf;
    try {
      streamManager.checkAuthorization(client, msg.streamChunkId.streamId);
      buf = streamManager.getChunk(msg.streamChunkId.streamId, msg.streamChunkId.chunkIndex);
    } catch (Exception e) {
      logger.error(String.format("Error opening block %s for request from %s",
        msg.streamChunkId, getRemoteAddress(channel)), e);
      respond(channel, new ChunkFetchFailure(msg.streamChunkId,
        Throwables.getStackTraceAsString(e)));
      return;
    }

    streamManager.chunkBeingSent(msg.streamChunkId.streamId);
    respond(channel, new ChunkFetchSuccess(msg.streamChunkId, buf)).addListener(
      (ChannelFutureListener) future -> streamManager.chunkSent(msg.streamChunkId.streamId));
  }

  /**
   * The invocation to channel.writeAndFlush is async, and the actual I/O on the
   * channel will be handled by the EventLoop the channel is registered to. So even
   * though we are processing the ChunkFetchRequest in a separate thread pool, the actual I/O,
   * which is the potentially blocking call that could deplete server handler threads, is still
   * being processed by TransportServer's default EventLoopGroup. In order to throttle the max
   * number of threads that channel I/O for sending response to ChunkFetchRequest, the thread
   * calling channel.writeAndFlush will wait for the completion of sending response back to
   * client by invoking await(). This will throttle the rate at which threads from
   * ChunkFetchRequest dedicated EventLoopGroup submit channel I/O requests to TransportServer's
   * default EventLoopGroup, thus making sure that we can reserve some threads in
   * TransportServer's default EventLoopGroup for handling other RPC messages.
   */
  private ChannelFuture respond(
      final Channel channel,
      final Encodable result) throws InterruptedException {
    final SocketAddress remoteAddress = channel.remoteAddress();
    return channel.writeAndFlush(result).await().addListener((ChannelFutureListener) future -> {
      if (future.isSuccess()) {
        logger.trace("Sent result {} to client {}", result, remoteAddress);
      } else {
        logger.error(String.format("Error sending result %s to %s; closing connection",
          result, remoteAddress), future.cause());
        channel.close();
      }
    });
  }
}
