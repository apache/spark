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

package org.apache.spark.network.shuffle;

import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * Extends {@link TransportContext} to support customized shuffle service. Specifically, we
 * modified the Netty Channel Pipeline so that IO expensive messages such as FINALIZE_SHUFFLE_MERGE
 * are processed in the separate handlers.
 * */
public class ShuffleTransportContext extends TransportContext {
  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(ShuffleTransportContext.class);
  private static final ShuffleMessageDecoder SHUFFLE_DECODER =
      new ShuffleMessageDecoder(MessageDecoder.INSTANCE);
  private final EventLoopGroup finalizeWorkers;

  public ShuffleTransportContext(
    TransportConf conf,
    ExternalBlockHandler rpcHandler,
    boolean closeIdleConnections) {
    this(conf, rpcHandler, closeIdleConnections, false);
  }

  public ShuffleTransportContext(TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections,
      boolean isClientOnly) {
    super(conf, rpcHandler, closeIdleConnections, isClientOnly);

    if ("shuffle".equalsIgnoreCase(conf.getModuleName()) && conf.separateFinalizeShuffleMerge()) {
      finalizeWorkers = NettyUtils.createEventLoop(
          IOMode.valueOf(conf.ioMode()),
          conf.finalizeShuffleMergeHandlerThreads(),
          "shuffle-finalize-merge-handler");
      logger.info("finalize shuffle merged workers created");
    } else {
      finalizeWorkers = null;
    }
  }

  @Override
  public TransportChannelHandler initializePipeline(SocketChannel channel, boolean isClient) {
    TransportChannelHandler ch = super.initializePipeline(channel, isClient);
    addHandlerToPipeline(channel, ch);
    return ch;
  }

  @Override
  public TransportChannelHandler initializePipeline(SocketChannel channel,
      RpcHandler channelRpcHandler, boolean isClient) {
    TransportChannelHandler ch = super.initializePipeline(channel, channelRpcHandler, isClient);
    addHandlerToPipeline(channel, ch);
    return ch;
  }

  /**
   * Add finalize handler to pipeline if needed. This is needed only when
   * separateFinalizeShuffleMerge is enabled.
   */
  private void addHandlerToPipeline(SocketChannel channel,
      TransportChannelHandler transportChannelHandler) {
    if (finalizeWorkers != null) {
      channel.pipeline().addLast(finalizeWorkers, FinalizedHandler.HANDLER_NAME,
        new FinalizedHandler(transportChannelHandler.getRequestHandler()));
    }
  }

  @Override
  protected MessageToMessageDecoder<ByteBuf> getDecoder() {
    return finalizeWorkers == null ? super.getDecoder() : SHUFFLE_DECODER;
  }

  @ChannelHandler.Sharable
  static class ShuffleMessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    private final MessageDecoder delegate;
    ShuffleMessageDecoder(MessageDecoder delegate) {
      super();
      this.delegate = delegate;
    }

    /**
     * Decode the message and check if it is a finalize merge request. If yes, then create an
     * internal rpc request message and add it to the list of messages to be handled by
     * {@link TransportChannelHandler}
    */
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext,
        ByteBuf byteBuf,
        List<Object> list) throws Exception {
      delegate.decode(channelHandlerContext, byteBuf, list);
      Object msg = list.get(list.size() - 1);
      if (msg instanceof RpcRequest req) {
        ByteBuffer buffer = req.body().nioByteBuffer();
        byte type = Unpooled.wrappedBuffer(buffer).readByte();
        if (type == BlockTransferMessage.Type.FINALIZE_SHUFFLE_MERGE.id()) {
          list.remove(list.size() - 1);
          RpcRequestInternal rpcRequestInternal =
            new RpcRequestInternal(BlockTransferMessage.Type.FINALIZE_SHUFFLE_MERGE, req);
          logger.trace("Created internal rpc request msg with rpcId {} for finalize merge req",
            req.requestId);
          list.add(rpcRequestInternal);
        }
      }
    }
  }

  /**
   * Internal message to handle rpc requests that is not accepted by
   * {@link TransportChannelHandler} as this message doesn't extend {@link Message}. It will be
   * accepted by {@link FinalizedHandler} instead, which is configured to execute in a separate
   * EventLoopGroup.
   */
  record RpcRequestInternal(BlockTransferMessage.Type messageType, RpcRequest rpcRequest) {
  }

  static class FinalizedHandler extends SimpleChannelInboundHandler<RpcRequestInternal> {
    private static final SparkLogger logger = SparkLoggerFactory.getLogger(FinalizedHandler.class);
    public static final String HANDLER_NAME = "finalizeHandler";
    private final TransportRequestHandler transportRequestHandler;

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
      if (msg instanceof RpcRequestInternal rpcRequestInternal) {
        return rpcRequestInternal.messageType == BlockTransferMessage.Type.FINALIZE_SHUFFLE_MERGE;
      }
      return false;
    }

    FinalizedHandler(TransportRequestHandler transportRequestHandler) {
      this.transportRequestHandler = transportRequestHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext,
        RpcRequestInternal req) throws Exception {
      if (logger.isTraceEnabled()) {
        logger.trace("Finalize shuffle req from {} for rpc request {}",
                getRemoteAddress(channelHandlerContext.channel()), req.rpcRequest.requestId);
      }
      this.transportRequestHandler.handle(req.rpcRequest);
    }
  }
}
