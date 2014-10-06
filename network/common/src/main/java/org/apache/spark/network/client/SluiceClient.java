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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.util.UUID;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.request.ChunkFetchRequest;
import org.apache.spark.network.protocol.request.RpcRequest;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of Sluice. The convenience method
 * "sendRPC" is provided to enable control plane communication between the client and server to
 * perform this setup.
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --> returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of SluiceClient using {@link SluiceClientFactory}. A single SluiceClient
 * may be used for multiple streams, but any given stream must be restricted to a single client,
 * in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link SluiceClientHandler} is
 * responsible for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class SluiceClient implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(SluiceClient.class);

  private final ChannelFuture cf;
  private final SluiceClientHandler handler;

  private final String serverAddr;

  SluiceClient(ChannelFuture cf, SluiceClientHandler handler) {
    this.cf = cf;
    this.handler = handler;

    if (cf != null && cf.channel() != null && cf.channel().remoteAddress() != null) {
      serverAddr = cf.channel().remoteAddress().toString();
    } else {
      serverAddr = "<unknown address>";
    }
  }

  public boolean isActive() {
    return cf.channel().isActive();
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   *
   * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single SluiceClient
   * is used to fetch the chunks.
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchChunk(
      long streamId,
      final int chunkIndex,
      final ChunkReceivedCallback callback) {
    final long startTime = System.currentTimeMillis();
    logger.debug("Sending fetch chunk request {} to {}", chunkIndex, serverAddr);

    final StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    handler.addFetchRequest(streamChunkId, callback);

    cf.channel().writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            long timeTaken = System.currentTimeMillis() - startTime;
            logger.debug("Sending request {} to {} took {} ms", streamChunkId, serverAddr,
                timeTaken);
          } else {
            // Fail all blocks.
            String errorMsg = String.format("Failed to send request %s to %s: %s", streamChunkId,
              serverAddr, future.cause().getMessage());
            logger.error(errorMsg, future.cause());
            future.cause().printStackTrace();
            handler.removeFetchRequest(streamChunkId);
            callback.onFailure(chunkIndex, new RuntimeException(errorMsg));
          }
        }
      });
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   */
  public void sendRpc(byte[] message, final RpcResponseCallback callback) {
    final long startTime = System.currentTimeMillis();
    logger.debug("Sending RPC to {}", serverAddr);

    final long tag = UUID.randomUUID().getLeastSignificantBits();
    handler.addRpcRequest(tag, callback);

    cf.channel().writeAndFlush(new RpcRequest(tag, message)).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            long timeTaken = System.currentTimeMillis() - startTime;
            logger.debug("Sending request {} to {} took {} ms", tag, serverAddr, timeTaken);
          } else {
            // Fail all blocks.
            String errorMsg = String.format("Failed to send request %s to %s: %s", tag,
                serverAddr, future.cause().getMessage());
            logger.error(errorMsg, future.cause());
            handler.removeRpcRequest(tag);
            callback.onFailure(new RuntimeException(errorMsg));
          }
        }
      });
  }

  @Override
  public void close() {
    cf.channel().close();
  }
}
