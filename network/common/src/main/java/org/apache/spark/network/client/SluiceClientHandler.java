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

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.response.ChunkFetchFailure;
import org.apache.spark.network.protocol.response.ChunkFetchSuccess;
import org.apache.spark.network.protocol.response.RpcFailure;
import org.apache.spark.network.protocol.response.RpcResponse;
import org.apache.spark.network.protocol.response.ServerResponse;

/**
 * Handler that processes server responses, in response to requests issued from [[SluiceClient]].
 * It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class SluiceClientHandler extends SimpleChannelInboundHandler<ServerResponse> {
  private final Logger logger = LoggerFactory.getLogger(SluiceClientHandler.class);

  private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches =
      new ConcurrentHashMap<StreamChunkId, ChunkReceivedCallback>();

  private final Map<Long, RpcResponseCallback> outstandingRpcs =
      new ConcurrentHashMap<Long, RpcResponseCallback>();

  public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
    outstandingFetches.put(streamChunkId, callback);
  }

  public void removeFetchRequest(StreamChunkId streamChunkId) {
    outstandingFetches.remove(streamChunkId);
  }

  public void addRpcRequest(long tag, RpcResponseCallback callback) {
    outstandingRpcs.put(tag, callback);
  }

  public void removeRpcRequest(long tag) {
    outstandingRpcs.remove(tag);
  }

  /**
   * Fire the failure callback for all outstanding requests. This is called when we have an
   * uncaught exception or pre-mature connection termination.
   */
  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
      entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
    }
    // TODO(rxin): Maybe we need to synchronize the access? Otherwise we could clear new requests
    // as well. But I guess that is ok given the caller will fail as soon as any requests fail.
    outstandingFetches.clear();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    if (outstandingFetches.size() > 0) {
      SocketAddress remoteAddress = ctx.channel().remoteAddress();
      logger.error("Still have {} requests outstanding when contention from {} is closed",
        outstandingFetches.size(), remoteAddress);
      failOutstandingRequests(new RuntimeException("Connection from " + remoteAddress + " closed"));
    }
    super.channelUnregistered(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (outstandingFetches.size() > 0) {
      logger.error(String.format("Exception in connection from %s: %s",
        ctx.channel().remoteAddress(), cause.getMessage()), cause);
      failOutstandingRequests(cause);
    }
    ctx.close();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, ServerResponse message) {
    String server = ctx.channel().remoteAddress().toString();
    if (message instanceof ChunkFetchSuccess) {
      ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Got a response for block {} from {} but it is not outstanding",
          resp.streamChunkId, server);
        resp.buffer.release();
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        listener.onSuccess(resp.streamChunkId.chunkIndex, resp.buffer);
        resp.buffer.release();
      }
    } else if (message instanceof ChunkFetchFailure) {
      ChunkFetchFailure resp = (ChunkFetchFailure) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Got a response for block {} from {} ({}) but it is not outstanding",
          resp.streamChunkId, server, resp.errorString);
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        listener.onFailure(resp.streamChunkId.chunkIndex,
          new ChunkFetchFailureException(resp.streamChunkId.chunkIndex, resp.errorString));
      }
    } else if (message instanceof RpcResponse) {
      RpcResponse resp = (RpcResponse) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.tag);
      if (listener == null) {
        logger.warn("Got a response for RPC {} from {} ({} bytes) but it is not outstanding",
          resp.tag, server, resp.response.length);
      } else {
        outstandingRpcs.remove(resp.tag);
        listener.onSuccess(resp.response);
      }
    } else if (message instanceof RpcFailure) {
      RpcFailure resp = (RpcFailure) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.tag);
      if (listener == null) {
        logger.warn("Got a response for RPC {} from {} ({}) but it is not outstanding",
          resp.tag, server, resp.errorString);
      } else {
        outstandingRpcs.remove(resp.tag);
        listener.onFailure(new RuntimeException(resp.errorString));
      }
    }
  }

  @VisibleForTesting
  public int numOutstandingRequests() {
    return outstandingFetches.size();
  }
}
