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

package org.apache.spark.network.protocol.request;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * Decoder in the server side to decode client requests.
 * This decoder is stateless so it is safe to be shared by multiple threads.
 *
 * This assumes the inbound messages have been processed by a frame decoder created by
 * {@link org.apache.spark.network.util.NettyUtils#createFrameDecoder()}.
 */
@ChannelHandler.Sharable
public final class ClientRequestDecoder extends MessageToMessageDecoder<ByteBuf> {

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    ClientRequest.Type msgType = ClientRequest.Type.decode(in);
    ClientRequest decoded = decode(msgType, in);
    assert decoded.type() == msgType;
    assert in.readableBytes() == 0;
    out.add(decoded);
  }

  private ClientRequest decode(ClientRequest.Type msgType, ByteBuf in) {
    switch (msgType) {
      case ChunkFetchRequest:
        return ChunkFetchRequest.decode(in);

      case RpcRequest:
        return RpcRequest.decode(in);

      default: throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }
}
