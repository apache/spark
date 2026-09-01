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

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.MergedBlockMetaSuccess;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamFailure;
import org.apache.spark.network.protocol.StreamResponse;
import org.apache.spark.network.server.MessageHandler;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.apache.spark.network.util.TransportFrameDecoder;
import org.apache.spark.util.Pair;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(TransportResponseHandler.class);

  private final Channel channel;

  private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

  private final Map<Long, BaseResponseCallback> outstandingRpcs;

  private final Queue<Pair<String, StreamCallback>> streamCallbacks;
  private volatile boolean streamActive;

  /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
  private final AtomicLong timeOfLastRequestNs;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingFetches = new ConcurrentHashMap<>();
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

  public void addRpcRequest(long requestId, BaseResponseCallback callback) {
    updateTimeOfLastRequest();
    outstandingRpcs.put(requestId, callback);
  }

  public void removeRpcRequest(long requestId) {
    outstandingRpcs.remove(requestId);
  }

  public void addStreamCallback(String streamId, StreamCallback callback) {
    updateTimeOfLastRequest();
    streamCallbacks.offer(Pair.of(streamId, callback));
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
        entry.getValue().onFailure(entry.getKey().chunkIndex(), cause);
      } catch (Exception e) {
        logger.warn("ChunkReceivedCallback.onFailure throws exception", e);
      }
    }
    for (BaseResponseCallback callback : outstandingRpcs.values()) {
      try {
        callback.onFailure(cause);
      } catch (Exception e) {
        logger.warn("RpcResponseCallback.onFailure throws exception", e);
      }
    }
    for (Pair<String, StreamCallback> entry : streamCallbacks) {
      try {
        entry.getRight().onFailure(entry.getLeft(), cause);
      } catch (Exception e) {
        logger.warn("StreamCallback.onFailure throws exception", e);
      }
    }

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingFetches.clear();
    outstandingRpcs.clear();
    streamCallbacks.clear();
  }

  @Override
  public void channelActive() {
  }

  @Override
  public void channelInactive() {
    if (hasOutstandingRequests()) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        MDC.of(LogKeys.COUNT, numOutstandingRequests()),
        MDC.of(LogKeys.HOST_PORT, remoteAddress));
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    if (hasOutstandingRequests()) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        MDC.of(LogKeys.COUNT, numOutstandingRequests()),
        MDC.of(LogKeys.HOST_PORT, remoteAddress));
      failOutstandingRequests(cause);
    }
  }

  @Override
  public void handle(ResponseMessage message) throws Exception {
    if (message instanceof ChunkFetchSuccess resp) {
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} since it is not outstanding",
          MDC.of(LogKeys.STREAM_CHUNK_ID, resp.streamChunkId),
          MDC.of(LogKeys.HOST_PORT, getRemoteAddress(channel)));
        resp.body().release();
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        listener.onSuccess(resp.streamChunkId.chunkIndex(), resp.body());
        resp.body().release();
      }
    } else if (message instanceof ChunkFetchFailure resp) {
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
          MDC.of(LogKeys.STREAM_CHUNK_ID, resp.streamChunkId),
          MDC.of(LogKeys.HOST_PORT, getRemoteAddress(channel)),
          MDC.of(LogKeys.ERROR, resp.errorString));
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        listener.onFailure(resp.streamChunkId.chunkIndex(), new ChunkFetchFailureException(
          "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
      }
    } else if (message instanceof RpcResponse resp) {
      RpcResponseCallback listener = (RpcResponseCallback) outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
          MDC.of(LogKeys.REQUEST_ID, resp.requestId),
          MDC.of(LogKeys.HOST_PORT, getRemoteAddress(channel)),
          MDC.of(LogKeys.RESPONSE_BODY_SIZE, resp.body().size()));
        resp.body().release();
      } else {
        outstandingRpcs.remove(resp.requestId);
        try {
          listener.onSuccess(resp.body().nioByteBuffer());
        } finally {
          resp.body().release();
        }
      }
    } else if (message instanceof RpcFailure resp) {
      BaseResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
          MDC.of(LogKeys.REQUEST_ID, resp.requestId),
          MDC.of(LogKeys.HOST_PORT, getRemoteAddress(channel)),
          MDC.of(LogKeys.ERROR, resp.errorString));
      } else {
        outstandingRpcs.remove(resp.requestId);
        listener.onFailure(new RuntimeException(resp.errorString));
      }
    } else if (message instanceof MergedBlockMetaSuccess resp) {
      try {
        MergedBlockMetaResponseCallback listener =
          (MergedBlockMetaResponseCallback) outstandingRpcs.get(resp.requestId);
        if (listener == null) {
          logger.warn("Ignoring response for MergedBlockMetaRequest {} from {} ({} bytes) since "
            + "it is not outstanding",
            MDC.of(LogKeys.REQUEST_ID, resp.requestId),
            MDC.of(LogKeys.HOST_PORT, getRemoteAddress(channel)),
            MDC.of(LogKeys.RESPONSE_BODY_SIZE, resp.body().size()));
        } else {
          outstandingRpcs.remove(resp.requestId);
          listener.onSuccess(resp.getNumChunks(), resp.body());
        }
      } finally {
        resp.body().release();
      }
    } else if (message instanceof StreamResponse resp) {
      Pair<String, StreamCallback> entry = streamCallbacks.poll();
      if (entry != null) {
        // Guard against a desynced callback queue before using the polled callback. Under correct
        // operation this is always a no-op; see verifyStreamCallbackMatches.
        verifyStreamCallbackMatches(entry, resp.streamId, "response");
        StreamCallback callback = entry.getRight();
        if (resp.byteCount > 0) {
          StreamInterceptor<ResponseMessage> interceptor = new StreamInterceptor<>(
            this, resp.streamId, resp.byteCount, callback);
          try {
            TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
              channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
            frameDecoder.setInterceptor(interceptor);
            streamActive = true;
          } catch (Exception e) {
            logger.error("Error installing stream handler.", e);
            deactivateStream();
            try {
              callback.onFailure(resp.streamId, e);
            } catch (IOException ioe) {
              logger.warn("Error in stream failure handler.", ioe);
            }
            // Installing the interceptor failed, so incoming data on this channel can no longer
            // be decoded. Close it so the broken connection is not reused from the pool.
            channel.close();
          }
        } else {
          try {
            callback.onComplete(resp.streamId);
          } catch (Exception e) {
            logger.warn("Error in stream handler onComplete().", e);
          }
        }
      } else {
        logger.error("Could not find callback for StreamResponse.");
      }
    } else if (message instanceof StreamFailure resp) {
      Pair<String, StreamCallback> entry = streamCallbacks.poll();
      if (entry != null) {
        // Same guard as the StreamResponse branch: verify the polled callback is the one this
        // failure is for before routing it. Under correct operation this is always a no-op.
        verifyStreamCallbackMatches(entry, resp.streamId, "failure");
        StreamCallback callback = entry.getRight();
        try {
          callback.onFailure(resp.streamId, new RuntimeException(resp.error));
        } catch (IOException ioe) {
          logger.warn("Error in stream failure handler.", ioe);
        }
      } else {
        logger.warn("Stream failure with unknown callback: {}",
          MDC.of(LogKeys.ERROR, resp.error));
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  /**
   * Verifies that the callback polled from the head of the FIFO {@link #streamCallbacks} queue is
   * the one this stream response/failure is for, by comparing the callback's registered streamId
   * with the streamId carried by the response.
   *
   * <p>Under correct operation this equality always holds and the method is a no-op: responses to
   * {@code StreamRequest}s arrive on a single connection in the order the client sent them (see
   * SPARK-11265), and the client registers each callback under the exact streamId it requested, so
   * the head of the queue always corresponds to the next response. A mismatch is therefore not
   * reachable by any normal client/server interaction; it could only be produced by memory or
   * hardware corruption (e.g. a bit flip in the streamId or a corrupted queue). This is a defensive
   * check that turns such corruption -- which would otherwise silently deliver the wrong block's
   * bytes to a reader -- into a loud, retriable failure.
   *
   * <p>On a mismatch it fails the polled callback under its own streamId (so its caller does not
   * hang waiting for a response it will never correctly receive; {@code poll()} has already removed
   * it from the queue) and throws {@link IllegalStateException}, which propagates to Netty's
   * {@code exceptionCaught} so the connection is torn down and its remaining outstanding requests
   * are re-fetched in order on a fresh channel.
   */
  private void verifyStreamCallbackMatches(
      Pair<String, StreamCallback> entry, String responseStreamId, String kind) {
    if (entry.getLeft().equals(responseStreamId)) {
      return;
    }
    String msg = String.format(
      "Stream callback queue desynced: %s streamId %s does not match the head of the callback "
        + "queue (streamId %s) from %s. This is unreachable under correct operation and may "
        + "indicate memory or hardware corruption; failing the connection to avoid delivering the "
        + "wrong block.", kind, responseStreamId, entry.getLeft(), getRemoteAddress(channel));
    // Log at the detection site so this otherwise-silent guard is directly greppable ("desynced"),
    // independent of the generic connection-exception log the thrown IllegalStateException produces
    // downstream.
    logger.error(msg);
    try {
      entry.getRight().onFailure(entry.getLeft(), new IOException(msg));
    } catch (IOException ioe) {
      logger.warn("Error in stream failure handler.", ioe);
    }
    throw new IllegalStateException(msg);
  }

  /** Returns total number of outstanding requests (fetch requests + rpcs) */
  public int numOutstandingRequests() {
    return outstandingFetches.size() + outstandingRpcs.size() + streamCallbacks.size() +
      (streamActive ? 1 : 0);
  }

  /** Check if there are any outstanding requests (fetch requests + rpcs) */
  public Boolean hasOutstandingRequests() {
    return streamActive || !outstandingFetches.isEmpty() || !outstandingRpcs.isEmpty() ||
        !streamCallbacks.isEmpty();
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
