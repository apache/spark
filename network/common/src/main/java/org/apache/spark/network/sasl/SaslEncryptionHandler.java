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

package org.apache.spark.network.sasl;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

/**
 * A channel handler that performs encryption / decryption of data using SASL. It needs to be
 * paired with a frame decoder so that the receiving side can decrypt the blocks.
 */
class SaslEncryptionHandler extends MessageToMessageCodec<ByteBuf, ByteBuf>  {

  private final SaslEncryptionBackend backend;

  SaslEncryptionHandler(SaslEncryptionBackend backend) {
    this.backend = backend;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
    throws Exception {
    encryptOrDecrypt(msg, out, false);
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
    throws Exception {
    encryptOrDecrypt(msg, out, true);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    backend.dispose();
  }

  private void encryptOrDecrypt(ByteBuf msg, List<Object> out, boolean encrypt) throws Exception {
    byte[] data;
    int offset;
    int length = msg.readableBytes();
    if (msg.hasArray()) {
      data = msg.array();
      offset = msg.arrayOffset();
    } else {
      data = new byte[length];
      msg.readBytes(data);
      offset = 0;
    }

    byte[] dest;
    if (encrypt) {
      dest = backend.wrap(data, offset, length);
      out.add(Unpooled.copyLong(8 + dest.length));
    } else {
      dest = backend.unwrap(data, offset, length);
    }

    out.add(Unpooled.wrappedBuffer(dest));
    msg.readerIndex(msg.readerIndex() + msg.readableBytes());
  }

}
