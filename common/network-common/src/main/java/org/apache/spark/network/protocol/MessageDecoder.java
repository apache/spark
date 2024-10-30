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

package org.apache.spark.network.protocol;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

/**
 * Decoder used by the client side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public final class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

  private static final SparkLogger logger = SparkLoggerFactory.getLogger(MessageDecoder.class);

  public static final MessageDecoder INSTANCE = new MessageDecoder();

  private MessageDecoder() {}

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    Message.Type msgType = Message.Type.decode(in);
    Message decoded = decode(msgType, in);
    assert decoded.type() == msgType;
    logger.trace("Received message {}: {}", msgType, decoded);
    out.add(decoded);
  }

  private Message decode(Message.Type msgType, ByteBuf in) {
    return switch (msgType) {
      case ChunkFetchRequest -> ChunkFetchRequest.decode(in);
      case ChunkFetchSuccess -> ChunkFetchSuccess.decode(in);
      case ChunkFetchFailure -> ChunkFetchFailure.decode(in);
      case RpcRequest -> RpcRequest.decode(in);
      case RpcResponse -> RpcResponse.decode(in);
      case RpcFailure -> RpcFailure.decode(in);
      case OneWayMessage -> OneWayMessage.decode(in);
      case StreamRequest -> StreamRequest.decode(in);
      case StreamResponse -> StreamResponse.decode(in);
      case StreamFailure -> StreamFailure.decode(in);
      case UploadStream -> UploadStream.decode(in);
      case MergedBlockMetaRequest -> MergedBlockMetaRequest.decode(in);
      case MergedBlockMetaSuccess -> MergedBlockMetaSuccess.decode(in);
      default -> throw new IllegalArgumentException("Unexpected message type: " + msgType);
    };
  }
}
