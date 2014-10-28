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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decoder used by the client side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public final class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

  private final Logger logger = LoggerFactory.getLogger(MessageDecoder.class);
  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    Message.Type msgType = Message.Type.decode(in);
    Message decoded = decode(msgType, in);
    assert decoded.type() == msgType;
    logger.trace("Received message " + msgType + ": " + decoded);
    out.add(decoded);
  }

  private Message decode(Message.Type msgType, ByteBuf in) {
    switch (msgType) {
      case ChunkFetchRequest:
        return ChunkFetchRequest.decode(in);

      case ChunkFetchSuccess:
        return ChunkFetchSuccess.decode(in);

      case ChunkFetchFailure:
        return ChunkFetchFailure.decode(in);

      case RpcRequest:
        return RpcRequest.decode(in);

      case RpcResponse:
        return RpcResponse.decode(in);

      case RpcFailure:
        return RpcFailure.decode(in);

      default:
        throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }
}
