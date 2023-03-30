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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class ShuffleTransportContext extends TransportContext {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleTransportContext.class);
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

    if (conf.getModuleName() != null &&
      conf.getModuleName().equalsIgnoreCase("shuffle") && conf.separateFinalizeShuffleMerge()) {
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
  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    TransportChannelHandler ch = super.initializePipeline(channel);
    addFinalizeHandlerToPipelineIfNeeded(channel, ch);
    return ch;
  }

  @Override
  public TransportChannelHandler initializePipeline(SocketChannel channel,
      RpcHandler channelRpcHandler) {
    TransportChannelHandler ch = super.initializePipeline(channel, channelRpcHandler);
    addFinalizeHandlerToPipelineIfNeeded(channel, ch);
    return ch;
  }

  private void addFinalizeHandlerToPipelineIfNeeded(SocketChannel channel,
      TransportChannelHandler ch) {
    if (finalizeWorkers != null) {
      channel.pipeline().addLast(finalizeWorkers, FinalizedHandler.HANDLER_NAME,
        new FinalizedHandler(ch.getRequestHandler()));
    }
  }

  @Override
  protected MessageToMessageDecoder<ByteBuf> getDecoder() {
    if (finalizeWorkers != null) {
      return new ShuffleMessageDecoder(MessageDecoder.INSTANCE);
    }
    return super.getDecoder();
  }

  static class ShuffleMessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    private final MessageDecoder delegate;
    public ShuffleMessageDecoder(MessageDecoder delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext,
        ByteBuf byteBuf,
        List<Object> list) throws Exception {
      delegate.decode(channelHandlerContext, byteBuf, list);
      Object msg = list.get(list.size() - 1);
      if (msg instanceof RpcRequest) {
        RpcRequest req = (RpcRequest) msg;
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
   * Internal message to handle rpc requests that should not be accepted by
   * {@link TransportChannelHandler}. Since, this message doesn't extend {@link Message}, it will
   * not be accepted by {@link TransportChannelHandler}.
   */
  static class RpcRequestInternal {
    public final BlockTransferMessage.Type messageType;
    public final RpcRequest rpcRequest;

    public RpcRequestInternal(BlockTransferMessage.Type messageType,
        RpcRequest rpcRequest) {
      this.messageType = messageType;
      this.rpcRequest = rpcRequest;
    }
  }

  static class FinalizedHandler extends SimpleChannelInboundHandler<RpcRequestInternal> {
    private static final Logger logger = LoggerFactory.getLogger(FinalizedHandler.class);
    public static final String HANDLER_NAME = "finalizeHandler";
    private final TransportRequestHandler transportRequestHandler;

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
      if (msg instanceof RpcRequestInternal) {
        RpcRequestInternal rpcRequestInternal = (RpcRequestInternal) msg;
        return rpcRequestInternal.messageType == BlockTransferMessage.Type.FINALIZE_SHUFFLE_MERGE;
      }
      return false;
    }

    public FinalizedHandler(TransportRequestHandler transportRequestHandler) {
      this.transportRequestHandler = transportRequestHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext,
        RpcRequestInternal req) throws Exception {
      logger.debug("finalize handler invoked for rpc request {}", req.rpcRequest.requestId);
      this.transportRequestHandler.handle(req.rpcRequest);
    }
  }
}
