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
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * Encoder for {@link ClientRequest} used in client side.
 *
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public final class ClientRequestEncoder extends MessageToMessageEncoder<ClientRequest> {
  @Override
  public void encode(ChannelHandlerContext ctx, ClientRequest in, List<Object> out) {
    ClientRequest.Type msgType = in.type();
    // Write 8 bytes for the frame's length, followed by the request type and request itself.
    int frameLength = 8 + msgType.encodedLength() + in.encodedLength();
    ByteBuf buf = ctx.alloc().buffer(frameLength);
    buf.writeLong(frameLength);
    msgType.encode(buf);
    in.encode(buf);
    assert buf.writableBytes() == 0;
    out.add(buf);
  }
}
