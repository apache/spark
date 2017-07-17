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

package org.apache.spark.network.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.AbstractReferenceCounted;
import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.commons.crypto.stream.CryptoOutputStream;

import org.apache.spark.network.util.ByteArrayReadableChannel;
import org.apache.spark.network.util.ByteArrayWritableChannel;

/**
 * Cipher for encryption and decryption.
 */
public class TransportCipher {
  @VisibleForTesting
  static final String ENCRYPTION_HANDLER_NAME = "TransportEncryption";
  private static final String DECRYPTION_HANDLER_NAME = "TransportDecryption";
  private static final int STREAM_BUFFER_SIZE = 1024 * 32;

  private final Properties conf;
  private final String cipher;
  private final SecretKeySpec key;
  private final byte[] inIv;
  private final byte[] outIv;

  public TransportCipher(
      Properties conf,
      String cipher,
      SecretKeySpec key,
      byte[] inIv,
      byte[] outIv) {
    this.conf = conf;
    this.cipher = cipher;
    this.key = key;
    this.inIv = inIv;
    this.outIv = outIv;
  }

  public String getCipherTransformation() {
    return cipher;
  }

  @VisibleForTesting
  SecretKeySpec getKey() {
    return key;
  }

  /** The IV for the input channel (i.e. output channel of the remote side). */
  public byte[] getInputIv() {
    return inIv;
  }

  /** The IV for the output channel (i.e. input channel of the remote side). */
  public byte[] getOutputIv() {
    return outIv;
  }

  private CryptoOutputStream createOutputStream(WritableByteChannel ch) throws IOException {
    return new CryptoOutputStream(cipher, conf, ch, key, new IvParameterSpec(outIv));
  }

  private CryptoInputStream createInputStream(ReadableByteChannel ch) throws IOException {
    return new CryptoInputStream(cipher, conf, ch, key, new IvParameterSpec(inIv));
  }

  /**
   * Add handlers to channel.
   *
   * @param ch the channel for adding handlers
   * @throws IOException
   */
  public void addToChannel(Channel ch) throws IOException {
    ch.pipeline()
      .addFirst(ENCRYPTION_HANDLER_NAME, new EncryptionHandler(this))
      .addFirst(DECRYPTION_HANDLER_NAME, new DecryptionHandler(this));
  }

  private static class EncryptionHandler extends ChannelOutboundHandlerAdapter {
    private final ByteArrayWritableChannel byteChannel;
    private final CryptoOutputStream cos;

    EncryptionHandler(TransportCipher cipher) throws IOException {
      byteChannel = new ByteArrayWritableChannel(STREAM_BUFFER_SIZE);
      cos = cipher.createOutputStream(byteChannel);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
      ctx.write(new EncryptedMessage(cos, msg, byteChannel), promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      try {
        cos.close();
      } finally {
        super.close(ctx, promise);
      }
    }
  }

  private static class DecryptionHandler extends ChannelInboundHandlerAdapter {
    private final CryptoInputStream cis;
    private final ByteArrayReadableChannel byteChannel;

    DecryptionHandler(TransportCipher cipher) throws IOException {
      byteChannel = new ByteArrayReadableChannel();
      cis = cipher.createInputStream(byteChannel);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
      byteChannel.feedData((ByteBuf) data);

      byte[] decryptedData = new byte[byteChannel.readableBytes()];
      int offset = 0;
      while (offset < decryptedData.length) {
        offset += cis.read(decryptedData, offset, decryptedData.length - offset);
      }

      ctx.fireChannelRead(Unpooled.wrappedBuffer(decryptedData, 0, decryptedData.length));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      try {
        cis.close();
      } finally {
        super.channelInactive(ctx);
      }
    }
  }

  private static class EncryptedMessage extends AbstractReferenceCounted implements FileRegion {
    private final boolean isByteBuf;
    private final ByteBuf buf;
    private final FileRegion region;
    private long transferred;
    private CryptoOutputStream cos;

    // Due to streaming issue CRYPTO-125: https://issues.apache.org/jira/browse/CRYPTO-125, it has
    // to utilize two helper ByteArrayWritableChannel for streaming. One is used to receive raw data
    // from upper handler, another is used to store encrypted data.
    private ByteArrayWritableChannel byteEncChannel;
    private ByteArrayWritableChannel byteRawChannel;

    private ByteBuffer currentEncrypted;

    EncryptedMessage(CryptoOutputStream cos, Object msg, ByteArrayWritableChannel ch) {
      Preconditions.checkArgument(msg instanceof ByteBuf || msg instanceof FileRegion,
        "Unrecognized message type: %s", msg.getClass().getName());
      this.isByteBuf = msg instanceof ByteBuf;
      this.buf = isByteBuf ? (ByteBuf) msg : null;
      this.region = isByteBuf ? null : (FileRegion) msg;
      this.transferred = 0;
      this.byteRawChannel = new ByteArrayWritableChannel(STREAM_BUFFER_SIZE);
      this.cos = cos;
      this.byteEncChannel = ch;
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
        if (currentEncrypted == null) {
          encryptMore();
        }

        int bytesWritten = currentEncrypted.remaining();
        target.write(currentEncrypted);
        bytesWritten -= currentEncrypted.remaining();
        transferred += bytesWritten;
        if (!currentEncrypted.hasRemaining()) {
          currentEncrypted = null;
          byteEncChannel.reset();
        }
      } while (transferred < count());

      return transferred;
    }

    private void encryptMore() throws IOException {
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
