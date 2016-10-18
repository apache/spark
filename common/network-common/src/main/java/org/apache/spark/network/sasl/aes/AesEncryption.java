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
import java.nio.ByteBuffer;

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

  public static void addToChannel(Channel ch, AesCipher cipher) throws IOException {
    ch.pipeline().addFirst(ENCRYPTION_HANDLER_NAME, new AesEncryptHandler(cipher))
      .addFirst(DECRYPTION_HANDLER_NAME, new AesDecryptHandler(cipher));
  }

  private static class AesEncryptHandler extends ChannelOutboundHandlerAdapter {
    private ByteArrayWritableChannel byteChannel;
    private CryptoOutputStream cos;

    AesEncryptHandler(AesCipher cipher) throws IOException {
      byteChannel = new ByteArrayWritableChannel(AesCipher.STREAM_BUFFER_SIZE);
      cos = cipher.CreateOutputStream(byteChannel);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
      ctx.write(new EncryptMessage(cos, msg, byteChannel), promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      cos.close();
      super.close(ctx, promise);
    }
  }

  private static class AesDecryptHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger = LoggerFactory.getLogger(AesDecryptHandler.class);
    private CryptoInputStream cis;
    private ByteArrayReadableChannel byteChannel;
    private long totalDecrypted;

    AesDecryptHandler(AesCipher cipher) throws IOException {
      byteChannel = new ByteArrayReadableChannel(AesCipher.STREAM_BUFFER_SIZE);
      cis = cipher.CreateInputStream(byteChannel);
      totalDecrypted = 0;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
      ByteBuf in = (ByteBuf) data;

      while (in.isReadable()) {
        byteChannel.feedData(in);
        int i;
        byte[] decryptedData = new byte[byteChannel.length()];
        int offset = 0;
        while ((i = cis.read(decryptedData, offset, decryptedData.length - offset)) > 0) {
          offset += i;
          if (offset >= decryptedData.length) {
            break;
          }
        }

        totalDecrypted += offset;
        byteChannel.reset();
        ctx.fireChannelRead(Unpooled.wrappedBuffer(decryptedData, 0, decryptedData.length));
      }

      in.release();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      cis.close();
      logger.debug("{} channel inactive decrypted {} bytes", ctx.channel(), totalDecrypted);
      super.channelInactive(ctx);
    }
  }

  private static class EncryptMessage extends AbstractReferenceCounted implements FileRegion {
    private final Logger logger = LoggerFactory.getLogger(EncryptMessage.class);

    private final boolean isByteBuf;
    private final ByteBuf buf;
    private final FileRegion region;
    private long transferred;
    private CryptoOutputStream cos;

    // Encrypt Data channel used to store encrypted data from crypto stream.
    private ByteArrayWritableChannel byteEncChannel;

    // Raw data channel used to store raw data from upper handler.
    private ByteArrayWritableChannel byteRawChannel;
    private ByteBuffer currentEncrypted;

    EncryptMessage(CryptoOutputStream cos, Object msg, ByteArrayWritableChannel ch) {
      Preconditions.checkArgument(msg instanceof ByteBuf || msg instanceof FileRegion,
        "Unrecognized message type: %s", msg.getClass().getName());
      this.isByteBuf = msg instanceof ByteBuf;
      this.buf = isByteBuf ? (ByteBuf) msg : null;
      this.region = isByteBuf ? null : (FileRegion) msg;
      this.transferred = 0;
      byteRawChannel = new ByteArrayWritableChannel(AesCipher.STREAM_BUFFER_SIZE);
      this.cos = cos;
      byteEncChannel = ch;
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

      do {
        if(currentEncrypted == null) {
          encryptMore();
        }
        int byteWritten = currentEncrypted.remaining();
        target.write(currentEncrypted);
        byteWritten -= currentEncrypted.remaining();
        transferred += byteWritten;
        if(!currentEncrypted.hasRemaining()) {
          currentEncrypted = null;
          byteEncChannel.reset();
        }
      } while (transferred < count());

      logger.debug("{} transferred total {} bytes", target, transferred);

      return transferred;
    }

    public void encryptMore() throws IOException {

      byteRawChannel.reset();
      if (isByteBuf) {
        int copied = byteRawChannel.write(buf.nioBuffer());
        buf.skipBytes(copied);
      } else {
        region.transferTo(byteRawChannel, region.transfered());
      }
      cos.write(byteRawChannel.getData(), 0, byteRawChannel.length());
      cos.flush();

      currentEncrypted = ByteBuffer.wrap(byteEncChannel.getData(),
        0, byteEncChannel.length());
    }

    @Override
    protected void deallocate() {
      byteRawChannel.reset();
      byteEncChannel.reset();
      if (region != null) {
        region.release();
      }
      if (buf != null) {
        buf.release();
      }
    }
  }

}
