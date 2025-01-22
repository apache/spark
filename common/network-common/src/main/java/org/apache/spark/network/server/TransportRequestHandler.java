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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.*;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.util.TransportFrameDecoder;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler is
 * attached to a single Netty channel, and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link TransportServer}.
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {

  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(TransportRequestHandler.class);

  /** The Netty channel that this handler is associated with. */
  private final Channel channel;

  /** Client on the same channel allowing us to talk back to the requester. */
  private final TransportClient reverseClient;

  /** Handles all RPC messages. */
  private final RpcHandler rpcHandler;

  /** Returns each chunk part of a stream. */
  private final StreamManager streamManager;

  /** The max number of chunks being transferred and not finished yet. */
  private final long maxChunksBeingTransferred;

  /** The dedicated ChannelHandler for ChunkFetchRequest messages. */
  private final ChunkFetchRequestHandler chunkFetchRequestHandler;

  public TransportRequestHandler(
      Channel channel,
      TransportClient reverseClient,
      RpcHandler rpcHandler,
      Long maxChunksBeingTransferred,
      ChunkFetchRequestHandler chunkFetchRequestHandler) {
    this.channel = channel;
    this.reverseClient = reverseClient;
    this.rpcHandler = rpcHandler;
    this.streamManager = rpcHandler.getStreamManager();
    this.maxChunksBeingTransferred = maxChunksBeingTransferred;
    this.chunkFetchRequestHandler = chunkFetchRequestHandler;
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    rpcHandler.exceptionCaught(cause, reverseClient);
  }

  @Override
  public void channelActive() {
    rpcHandler.channelActive(reverseClient);
  }

  @Override
  public void channelInactive() {
    if (streamManager != null) {
      try {
        streamManager.connectionTerminated(channel);
      } catch (RuntimeException e) {
        logger.error("StreamManager connectionTerminated() callback failed.", e);
      }
    }
    rpcHandler.channelInactive(reverseClient);
  }

  @Override
  public void handle(RequestMessage request) throws Exception {
    if (request instanceof ChunkFetchRequest chunkFetchRequest) {
      chunkFetchRequestHandler.processFetchRequest(channel, chunkFetchRequest);
    } else if (request instanceof RpcRequest rpcRequest) {
      processRpcRequest(rpcRequest);
    } else if (request instanceof OneWayMessage oneWayMessage) {
      processOneWayMessage(oneWayMessage);
    } else if (request instanceof StreamRequest streamRequest) {
      processStreamRequest(streamRequest);
    } else if (request instanceof UploadStream uploadStream) {
      processStreamUpload(uploadStream);
    } else if (request instanceof MergedBlockMetaRequest mergedBlockMetaRequest) {
      processMergedBlockMetaRequest(mergedBlockMetaRequest);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  private void processStreamRequest(final StreamRequest req) {
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch stream {}", getRemoteAddress(channel),
        req.streamId);
    }

    if (maxChunksBeingTransferred < Long.MAX_VALUE) {
      long chunksBeingTransferred = streamManager.chunksBeingTransferred();
      if (chunksBeingTransferred >= maxChunksBeingTransferred) {
        logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
          MDC.of(LogKeys.NUM_CHUNKS$.MODULE$, chunksBeingTransferred),
          MDC.of(LogKeys.MAX_NUM_CHUNKS$.MODULE$, maxChunksBeingTransferred));
        channel.close();
        return;
      }
    }
    ManagedBuffer buf;
    try {
      buf = streamManager.openStream(req.streamId);
    } catch (Exception e) {
      logger.error("Error opening stream {} for request from {}", e,
        MDC.of(LogKeys.STREAM_ID$.MODULE$, req.streamId),
        MDC.of(LogKeys.HOST_PORT$.MODULE$, getRemoteAddress(channel)));
      respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
      return;
    }

    if (buf != null) {
      streamManager.streamBeingSent(req.streamId);
      respond(new StreamResponse(req.streamId, buf.size(), buf)).addListener(future -> {
        streamManager.streamSent(req.streamId);
      });
    } else {
      // org.apache.spark.executor.ExecutorClassLoader.STREAM_NOT_FOUND_REGEX should also be updated
      // when the following error message is changed.
      respond(new StreamFailure(req.streamId, String.format(
        "Stream '%s' was not found.", req.streamId)));
    }
  }

  private void processRpcRequest(final RpcRequest req) {
    try {
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on RPC id {}", e,
        MDC.of(LogKeys.REQUEST_ID$.MODULE$, req.requestId));
      respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    } finally {
      req.body().release();
    }
  }

  /**
   * Handle a request from the client to upload a stream of data.
   */
  private void processStreamUpload(final UploadStream req) {
    assert (req.body() == null);
    try {
      RpcResponseCallback callback = new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        }
      };
      TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
          channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
      ByteBuffer meta = req.meta.nioByteBuffer();
      StreamCallbackWithID streamHandler = rpcHandler.receiveStream(reverseClient, meta, callback);
      if (streamHandler == null) {
        throw new NullPointerException("rpcHandler returned a null streamHandler");
      }
      StreamCallbackWithID wrappedCallback = new StreamCallbackWithID() {
        @Override
        public void onData(String streamId, ByteBuffer buf) throws IOException {
          streamHandler.onData(streamId, buf);
        }

        @Override
        public void onComplete(String streamId) throws IOException {
           try {
             streamHandler.onComplete(streamId);
             callback.onSuccess(streamHandler.getCompletionResponse());
           } catch (BlockPushNonFatalFailure ex) {
             // Respond an RPC message with the error code to client instead of using exceptions
             // encoded in the RPCFailure. This type of exceptions gets thrown more frequently
             // than a regular exception on the shuffle server side due to the best-effort nature
             // of push-based shuffle and requires special handling on the client side. Using a
             // proper RPCResponse is more efficient.
             callback.onSuccess(ex.getResponse());
             streamHandler.onFailure(streamId, ex);
           } catch (Exception ex) {
             IOException ioExc = new IOException("Failure post-processing complete stream;" +
               " failing this rpc and leaving channel active", ex);
             callback.onFailure(ioExc);
             streamHandler.onFailure(streamId, ioExc);
           }
        }

        @Override
        public void onFailure(String streamId, Throwable cause) throws IOException {
          callback.onFailure(new IOException("Destination failed while reading stream", cause));
          streamHandler.onFailure(streamId, cause);
        }

        @Override
        public String getID() {
          return streamHandler.getID();
        }
      };
      if (req.bodyByteCount > 0) {
        StreamInterceptor<RequestMessage> interceptor = new StreamInterceptor<>(
          this, wrappedCallback.getID(), req.bodyByteCount, wrappedCallback);
        frameDecoder.setInterceptor(interceptor);
      } else {
        wrappedCallback.onComplete(wrappedCallback.getID());
      }
    } catch (Exception e) {
      if (e instanceof BlockPushNonFatalFailure blockPushNonFatalFailure) {
        // Thrown by rpcHandler.receiveStream(reverseClient, meta, callback), the same as
        // onComplete method. Respond an RPC message with the error code to client instead of
        // using exceptions encoded in the RPCFailure. Using a proper RPCResponse is more
        // efficient, and now only include the too old attempt case here.
        respond(new RpcResponse(req.requestId,
          new NioManagedBuffer(blockPushNonFatalFailure.getResponse())));
      } else {
        logger.error("Error while invoking RpcHandler#receive() on RPC id {}", e,
          MDC.of(LogKeys.REQUEST_ID$.MODULE$, req.requestId));
        respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
      }
      // We choose to totally fail the channel, rather than trying to recover as we do in other
      // cases.  We don't know how many bytes of the stream the client has already sent for the
      // stream, it's not worth trying to recover.
      channel.pipeline().fireExceptionCaught(e);
    } finally {
      req.meta.release();
    }
  }

  private void processOneWayMessage(OneWayMessage req) {
    try {
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
    } finally {
      req.body().release();
    }
  }

  private void processMergedBlockMetaRequest(final MergedBlockMetaRequest req) {
    try {
      rpcHandler.getMergedBlockMetaReqHandler().receiveMergeBlockMetaReq(reverseClient, req,
        new MergedBlockMetaResponseCallback() {

          @Override
          public void onSuccess(int numChunks, ManagedBuffer buffer) {
            logger.trace("Sending meta for request {} numChunks {}", req, numChunks);
            respond(new MergedBlockMetaSuccess(req.requestId, numChunks, buffer));
          }

          @Override
          public void onFailure(Throwable e) {
            logger.trace("Failed to send meta for {}", req);
            respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
          }
      });
    } catch (Exception e) {
      logger.error("Error while invoking receiveMergeBlockMetaReq() for appId {} shuffleId {} "
        + "reduceId {}", e, MDC.of(LogKeys.APP_ID$.MODULE$, req.appId),
          MDC.of(LogKeys.SHUFFLE_ID$.MODULE$, req.shuffleId),
          MDC.of(LogKeys.REDUCE_ID$.MODULE$, req.reduceId));
      respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  private ChannelFuture respond(Encodable result) {
    SocketAddress remoteAddress = channel.remoteAddress();
    return channel.writeAndFlush(result).addListener(future -> {
      if (future.isSuccess()) {
        logger.trace("Sent result {} to client {}", result, remoteAddress);
      } else {
        logger.error("Error sending result {} to {}; closing connection", future.cause(),
          MDC.of(LogKeys.RESULT$.MODULE$, result),
          MDC.of(LogKeys.HOST_PORT$.MODULE$, remoteAddress));
        channel.close();
      }
    });
  }
}
