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

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;

import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.NettyUtils;

/**
 * A channel handler that performs encryption / decryption of data using SASL.
 */
class SaslEncryptionHandler extends ChannelOutboundHandlerAdapter {

  private final Object lock;
  private final SaslEncryptionBackend backend;

  SaslEncryptionHandler(SaslEncryptionBackend backend) {
    this.lock = new Object();
    this.backend = backend;
  }

  /**
   * Finishes configuring the pipeline for SASL encryption. Add a frame decoder and a decryption
   * handler.
   *
   * The MessageDemux handler is removed since this handler already handles thread-safety on the
   * write path; that's needed to ensure that messages are encrypted and decrypted in the same
   * order.
   */
  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    ctx.channel().pipeline()
      .addFirst("saslDecryption", new DecryptionHandler())
      .addFirst("saslFrameDecoder", NettyUtils.createFrameDecoder());
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
    throws Exception {
    synchronized (lock) {
      ByteBuf encrypted;
      try {
        encrypted = encryptOrDecrypt(msg, true);
      } finally {
        ReferenceCountUtil.release(msg);
      }
      ctx.write(Unpooled.copyLong(8 + encrypted.readableBytes()), ctx.voidPromise());
      ctx.write(encrypted, promise);
    }
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    backend.dispose();
    ctx.close(promise);
  }

  private ByteBuf encryptOrDecrypt(Object msg, boolean encrypt) throws Exception {
    byte[] data;
    int offset;
    int length;
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      length = buf.readableBytes();
      if (buf.hasArray()) {
        data = buf.array();
        offset = buf.arrayOffset();
      } else {
        data = new byte[length];
        buf.readBytes(data);
        offset = 0;
      }
    } else if (msg instanceof FileRegion) {
      FileRegion region = (FileRegion) msg;
      length = Ints.checkedCast(region.count());
      ByteArrayWritableChannel bc = new ByteArrayWritableChannel(length);
      while (region.count() > region.transfered()) {
        region.transferTo(bc, region.transfered());
      }
      data = bc.getData();
      offset = 0;
    } else {
      throw new IllegalArgumentException("Unexpected message: " + msg.getClass().getName());
    }

    byte[] dest;
    if (encrypt) {
      dest = backend.wrap(data, offset, length);
    } else {
      dest = backend.unwrap(data, offset, length);
    }
    return Unpooled.wrappedBuffer(dest);
  }

  private class DecryptionHandler extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
      throws Exception {
      out.add(encryptOrDecrypt(msg, false));
    }

  }

}
