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

package org.apache.spark.network.sasl.aes;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.AbstractReferenceCounted;
import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.commons.crypto.stream.CryptoOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.ByteArrayReadableChannel;
import org.apache.spark.network.util.ByteArrayWritableChannel;

public class AesEncryption {
  public static final String ENCRYPTION_HANDLER_NAME = "AesEncryption";
  public static final String DECRYPTION_HANDLER_NAME = "AesDecryption";

  public static void addToChannel(Channel ch, AesCipher cipher) {
    ch.pipeline().addFirst(ENCRYPTION_HANDLER_NAME, new AesEncryptHandler(cipher))
      .addFirst(DECRYPTION_HANDLER_NAME, new AesDecryptHandler(cipher));
  }

  private static class AesEncryptHandler extends ChannelOutboundHandlerAdapter {
    private final AesCipher cipher;
    private ByteArrayWritableChannel byteChannel;

    AesEncryptHandler(AesCipher cipher) {
      this.cipher = cipher;
      byteChannel = new ByteArrayWritableChannel(AesCipher.STREAM_BUFFER_SIZE);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
      ctx.write(new EncryptMessage(cipher, msg, byteChannel), promise);
    }
  }

  private static class AesDecryptHandler extends ChannelInboundHandlerAdapter {
    private final AesCipher cipher;
    private CryptoInputStream cis;
    private ByteArrayReadableChannel byteChannel;

    AesDecryptHandler(AesCipher cipher) {
      this.cipher = cipher;
      byteChannel = new ByteArrayReadableChannel();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
      ByteBuf in = (ByteBuf) data;

      try {
        while (in.isReadable()) {
          byteChannel.feedData(in);
          cis = cipher.CreateInputStream(byteChannel);

          int i;
          byte[] decryptedData = new byte[byteChannel.length()];
          int offset = 0;
          while ((i = cis.read(decryptedData, offset, decryptedData.length - offset)) > 0) {
            offset += i;
            if (offset >= decryptedData.length) break;
          }
          byteChannel.reset();

          ctx.fireChannelRead(Unpooled.wrappedBuffer(decryptedData, 0, offset));
        }
      } finally {
        in.release();
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      byteChannel.reset();
      super.channelInactive(ctx);
    }
  }

  private static class EncryptMessage extends AbstractReferenceCounted implements FileRegion {
    private final Logger logger = LoggerFactory.getLogger(EncryptMessage.class);

    private AesCipher cipher;
    private final boolean isByteBuf;
    private final ByteBuf buf;
    private final FileRegion region;
    private long transferred;
    private ByteArrayWritableChannel byteChannel;

    EncryptMessage(AesCipher cipher, Object msg, ByteArrayWritableChannel ch) {
      Preconditions.checkArgument(msg instanceof ByteBuf || msg instanceof FileRegion,
        "Unrecognized message type: %s", msg.getClass().getName());
      this.cipher = cipher;
      this.isByteBuf = msg instanceof ByteBuf;
      this.buf = isByteBuf ? (ByteBuf) msg : null;
      this.region = isByteBuf ? null : (FileRegion) msg;
      this.transferred = 0;
      byteChannel = ch;
    }

    @Override
    public long count() {
      return isByteBuf ? buf.readableBytes() : region.count();
    }

    @Override
    public long position() {
      return 0;
    }

    @Override
    public long transfered() {
      return transferred;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
      Preconditions.checkArgument(position == transfered(), "Invalid position.");
      CryptoOutputStream cos = cipher.CreateOutputStream(target);

      do {
        byteChannel.reset();
        if (isByteBuf) {
          int copied = byteChannel.write(buf.nioBuffer());
          buf.skipBytes(copied);
        } else {
          region.transferTo(byteChannel, region.transfered());
        }

        cos.write(byteChannel.getData(), 0, byteChannel.length());
        transferred += byteChannel.length();
      } while (transferred < count());

      cos.flush();
      logger.debug("enc total {} bytes", transferred);

      return transferred;
    }

    @Override
    protected void deallocate() {
      byteChannel.reset();
      if (region != null) {
        region.release();
      }

      if (buf != null) {
        buf.release();
      }
    }
  }

}
