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

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.*;
import org.apache.spark.network.server.MessageHandler;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
  private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private final Channel channel;

  private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

  private final Map<StreamChunkId, StreamCallback> outstandingFetchAsStreams;

  private final Map<Long, RpcResponseCallback> outstandingRpcs;

  private final Queue<Pair<String, StreamCallback>> streamCallbacks;
  private volatile boolean streamActive;

  /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
  private final AtomicLong timeOfLastRequestNs;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingFetches = new ConcurrentHashMap<>();
    this.outstandingFetchAsStreams = new ConcurrentHashMap<>();
    this.outstandingRpcs = new ConcurrentHashMap<>();
    this.streamCallbacks = new ConcurrentLinkedQueue<>();
    this.timeOfLastRequestNs = new AtomicLong(0);
  }

  public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
    updateTimeOfLastRequest();
    outstandingFetches.put(streamChunkId, callback);
  }

  public void removeFetchRequest(StreamChunkId streamChunkId) {
    outstandingFetches.remove(streamChunkId);
  }

  public void addFetchAsStreamRequest(
      StreamChunkId streamChunkId,
      StreamCallback callback) {
      updateTimeOfLastRequest();
    outstandingFetchAsStreams.put(streamChunkId, callback);
  }

  public void removeFetchAsStreamRequest(StreamChunkId streamChunkId) {
    outstandingFetchAsStreams.remove(streamChunkId);
  }

  public void addRpcRequest(long requestId, RpcResponseCallback callback) {
    updateTimeOfLastRequest();
    outstandingRpcs.put(requestId, callback);
  }

  public void removeRpcRequest(long requestId) {
    outstandingRpcs.remove(requestId);
  }

  public void addStreamCallback(String streamId, StreamCallback callback) {
    timeOfLastRequestNs.set(System.nanoTime());
    streamCallbacks.offer(ImmutablePair.of(streamId, callback));
  }

  @VisibleForTesting
  public void deactivateStream() {
    streamActive = false;
  }

  /**
   * Fire the failure callback for all outstanding requests. This is called when we have an
   * uncaught exception or pre-mature connection termination.
   */
  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
      try {
        entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
      } catch (Exception e) {
        logger.warn("ChunkReceivedCallback.onFailure throws exception", e);
      }
    }
    for (Map.Entry<StreamChunkId, StreamCallback> entry : outstandingFetchAsStreams.entrySet()) {
      try {
        entry.getValue().onFailure(entry.getKey().toString(), cause);
      } catch (Exception e) {
        logger.warn("ChunkFetchRequest's StreamCallback.onFailure throws exception", e);
      }
    }
    for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
      try {
        entry.getValue().onFailure(cause);
      } catch (Exception e) {
        logger.warn("RpcResponseCallback.onFailure throws exception", e);
      }
    }
    for (Pair<String, StreamCallback> entry : streamCallbacks) {
      try {
        entry.getValue().onFailure(entry.getKey(), cause);
      } catch (Exception e) {
        logger.warn("StreamCallback.onFailure throws exception", e);
      }
    }

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingFetches.clear();
    outstandingFetchAsStreams.clear();
    outstandingRpcs.clear();
    streamCallbacks.clear();
  }

  @Override
  public void channelActive() {
  }

  @Override
  public void channelInactive() {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  @Override
  public void handle(ResponseMessage message) throws Exception {
    if (message instanceof ChunkFetchSuccess) {
      ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel));
        resp.body().release();
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
        resp.body().release();
      }
      // The response is `ChunkFetchSuccess`. It's either because the request was a normal chunk
      // fetch request, or the server side is an old version that doesn't support fetch chunk as
      // stream. So the next line is either a no-op, or remove the callback that will never be
      // called later.
      outstandingFetchAsStreams.remove(resp.streamChunkId);
    } else if (message instanceof ChunkFetchStreamResponse) {
      ChunkFetchStreamResponse resp = (ChunkFetchStreamResponse) message;
      StreamCallback callback = outstandingFetchAsStreams.get(resp.streamChunkId);
      if (callback == null) {
        logger.warn("Ignoring response for block {} from {} since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel));
        resp.body().release();
      } else {
        readStream(resp.streamChunkId.toString(), resp.byteCount, callback);
      }
    } else if (message instanceof ChunkFetchFailure) {
      ChunkFetchFailure resp = (ChunkFetchFailure) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel), resp.errorString);
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
          "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
      }
    } else if (message instanceof RpcResponse) {
      RpcResponse resp = (RpcResponse) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
          resp.requestId, getRemoteAddress(channel), resp.body().size());
      } else {
        outstandingRpcs.remove(resp.requestId);
        try {
          listener.onSuccess(resp.body().nioByteBuffer());
        } finally {
          resp.body().release();
        }
      }
    } else if (message instanceof RpcFailure) {
      RpcFailure resp = (RpcFailure) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
          resp.requestId, getRemoteAddress(channel), resp.errorString);
      } else {
        outstandingRpcs.remove(resp.requestId);
        listener.onFailure(new RuntimeException(resp.errorString));
      }
    } else if (message instanceof StreamResponse) {
      StreamResponse resp = (StreamResponse) message;
      Pair<String, StreamCallback> entry = streamCallbacks.poll();
      if (entry != null) {
        StreamCallback callback = entry.getValue();
        readStream(resp.streamId, resp.byteCount, callback);
      } else {
        logger.error("Could not find callback for StreamResponse.");
      }
    } else if (message instanceof StreamFailure) {
      StreamFailure resp = (StreamFailure) message;
      Pair<String, StreamCallback> entry = streamCallbacks.poll();
      if (entry != null) {
        StreamCallback callback = entry.getValue();
        try {
          callback.onFailure(resp.streamId, new RuntimeException(resp.error));
        } catch (IOException ioe) {
          logger.warn("Error in stream failure handler.", ioe);
        }
      } else {
        logger.warn("Stream failure with unknown callback: {}", resp.error);
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  private void readStream(String streamId, long byteCount, StreamCallback callback) {
    if (byteCount > 0) {
      StreamInterceptor<ResponseMessage> interceptor = new StreamInterceptor<>(
        this, streamId, byteCount, callback);
      try {
        TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
          channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
        frameDecoder.setInterceptor(interceptor);
        streamActive = true;
      } catch (Exception e) {
        logger.error("Error installing stream handler.", e);
        deactivateStream();
      }
    } else {
      try {
        callback.onComplete(streamId);
      } catch (Exception e) {
        logger.warn("Error in stream handler onComplete().", e);
      }
    }
  }

  /** Returns total number of outstanding requests (fetch requests + rpcs) */
  public int numOutstandingRequests() {
    return outstandingFetches.size() + outstandingFetchAsStreams.size() + outstandingRpcs.size() +
      streamCallbacks.size() + (streamActive ? 1 : 0);
  }

  /** Returns the time in nanoseconds of when the last request was sent out. */
  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }

  /** Updates the time of the last request to the current system time. */
  public void updateTimeOfLastRequest() {
    timeOfLastRequestNs.set(System.nanoTime());
  }

}
