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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.*;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.util.JavaUtils;
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

  private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

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
    if (request instanceof ChunkFetchRequest) {
      chunkFetchRequestHandler.processFetchRequest(channel, (ChunkFetchRequest) request);
    } else if (request instanceof RpcRequest) {
      processRpcRequest((RpcRequest) request);
    } else if (request instanceof OneWayMessage) {
      processOneWayMessage((OneWayMessage) request);
    } else if (request instanceof StreamRequest) {
      processStreamRequest((StreamRequest) request);
    } else if (request instanceof UploadStream) {
      processStreamUpload((UploadStream) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  private void processStreamRequest(final StreamRequest req) {
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch stream {}", getRemoteAddress(channel),
        req.streamId);
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
      buf = streamManager.openStream(req.streamId);
    } catch (Exception e) {
      logger.error(String.format(
        "Error opening stream %s for request from %s", req.streamId, getRemoteAddress(channel)), e);
      respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
      return;
    }

    if (buf != null) {
      streamManager.streamBeingSent(req.streamId);
      respond(new StreamResponse(req.streamId, buf.size(), buf)).addListener(future -> {
        streamManager.streamSent(req.streamId);
      });
    } else {
      // org.apache.spark.repl.ExecutorClassLoader.STREAM_NOT_FOUND_REGEX should also be updated
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
      logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
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
      // Retain the original metadata buffer, since it will be used during the invocation of
      // this method. Will be released later.
      req.meta.retain();
      // Make a copy of the original metadata buffer. In benchmark, we noticed that
      // we cannot respond the original metadata buffer back to the client, otherwise
      // in cases where multiple concurrent shuffles are present, a wrong metadata might
      // be sent back to client. This is related to the eager release of the metadata buffer,
      // i.e., we always release the original buffer by the time the invocation of this
      // method ends, instead of by the time we respond it to the client. This is necessary,
      // otherwise we start seeing memory issues very quickly in benchmarks.
      ByteBuffer meta = cloneBuffer(req.meta.nioByteBuffer());
      RpcResponseCallback callback = new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          // Piggyback request metadata as part of the exception error String, so we can
          // respond the metadata upon a failure without changing the existing protocol.
          respond(new RpcFailure(req.requestId,
              JavaUtils.encodeHeaderIntoErrorString(meta.duplicate(), e)));
          req.meta.release();
        }
      };
      TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
          channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
      StreamCallbackWithID streamHandler =
          rpcHandler.receiveStream(reverseClient, meta.duplicate(), callback);
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
             callback.onSuccess(meta.duplicate());
           } catch (Exception ex) {
             IOException ioExc = new IOException("Failure post-processing complete stream;" +
               " failing this rpc and leaving channel active", ex);
             // req.meta will be released once inside callback.onFailure. Retain it one more
             // time to be released in the finally block.
             req.meta.retain();
             callback.onFailure(ioExc);
             streamHandler.onFailure(streamId, ioExc);
           } finally {
             req.meta.release();
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
      logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
      try {
        // It's OK to respond the original metadata buffer here, because this is still inside
        // the invocation of this method.
        respond(new RpcFailure(req.requestId,
            JavaUtils.encodeHeaderIntoErrorString(req.meta.nioByteBuffer(), e)));
      } catch (IOException ioe) {
        // No exception will be thrown here. req.meta.nioByteBuffer will not throw IOException
        // because it's a NettyManagedBuffer. This try-catch block is to make compiler happy.
        logger.error("Error in handling failure while invoking RpcHandler#receive() on RPC id {}",
            req.requestId, e);
      } finally {
        req.meta.release();
      }
      // We choose to totally fail the channel, rather than trying to recover as we do in other
      // cases.  We don't know how many bytes of the stream the client has already sent for the
      // stream, it's not worth trying to recover.
      channel.pipeline().fireExceptionCaught(e);
    } finally {
      // Make sure we always release the original metadata buffer by the time we exit the
      // invocation of this method. Otherwise, we see memory issues fairly quickly in benchmarks.
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

  /**
   * Make a full copy of a nio ByteBuffer.
   */
  private static ByteBuffer cloneBuffer(ByteBuffer buf) {
    ByteBuffer clone = ByteBuffer.allocate(buf.capacity());
    clone.put(buf.duplicate());
    clone.flip();
    return clone;
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
        logger.error(String.format("Error sending result %s to %s; closing connection",
          result, remoteAddress), future.cause());
        channel.close();
      }
    });
  }
}
