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
import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.commons.crypto.stream.CryptoOutputStream;

import org.apache.spark.network.util.AbstractFileRegion;
import org.apache.spark.network.util.ByteArrayReadableChannel;
import org.apache.spark.network.util.ByteArrayWritableChannel;

/**
 * Cipher for encryption and decryption.
 */
public class TransportCipher {
  @VisibleForTesting
  static final String ENCRYPTION_HANDLER_NAME = "TransportEncryption";
  private static final String DECRYPTION_HANDLER_NAME = "TransportDecryption";
  @VisibleForTesting
  static final int STREAM_BUFFER_SIZE = 1024 * 32;

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

  @VisibleForTesting
  CryptoOutputStream createOutputStream(WritableByteChannel ch) throws IOException {
    return new CryptoOutputStream(cipher, conf, ch, key, new IvParameterSpec(outIv));
  }

  @VisibleForTesting
  CryptoInputStream createInputStream(ReadableByteChannel ch) throws IOException {
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

  @VisibleForTesting
  static class EncryptionHandler extends ChannelOutboundHandlerAdapter {
    private final ByteArrayWritableChannel byteChannel;
    private final CryptoOutputStream cos;
    private boolean isCipherValid;

    EncryptionHandler(TransportCipher cipher) throws IOException {
      byteChannel = new ByteArrayWritableChannel(STREAM_BUFFER_SIZE);
      cos = cipher.createOutputStream(byteChannel);
      isCipherValid = true;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
      ctx.write(createEncryptedMessage(msg), promise);
    }

    @VisibleForTesting
    EncryptedMessage createEncryptedMessage(Object msg) {
      return new EncryptedMessage(this, cos, msg, byteChannel);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      try {
        if (isCipherValid) {
          cos.close();
        }
      } finally {
        super.close(ctx, promise);
      }
    }

    /**
     * SPARK-25535. Workaround for CRYPTO-141. Avoid further interaction with the underlying cipher
     * after an error occurs.
     */
    void reportError() {
      this.isCipherValid = false;
    }

    boolean isCipherValid() {
      return isCipherValid;
    }
  }

  private static class DecryptionHandler extends ChannelInboundHandlerAdapter {
    private final CryptoInputStream cis;
    private final ByteArrayReadableChannel byteChannel;
    private boolean isCipherValid;

    DecryptionHandler(TransportCipher cipher) throws IOException {
      byteChannel = new ByteArrayReadableChannel();
      cis = cipher.createInputStream(byteChannel);
      isCipherValid = true;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
      ByteBuf buffer = (ByteBuf) data;

      try {
        if (!isCipherValid) {
          throw new IOException("Cipher is in invalid state.");
        }
        byte[] decryptedData = new byte[buffer.readableBytes()];
        byteChannel.feedData(buffer);

        int offset = 0;
        while (offset < decryptedData.length) {
          // SPARK-25535: workaround for CRYPTO-141.
          try {
            offset += cis.read(decryptedData, offset, decryptedData.length - offset);
          } catch (InternalError ie) {
            isCipherValid = false;
            throw ie;
          }
        }

        ctx.fireChannelRead(Unpooled.wrappedBuffer(decryptedData, 0, decryptedData.length));
      } finally {
        buffer.release();
      }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      // We do the closing of the stream / channel in handlerRemoved(...) as
      // this method will be called in all cases:
      //
      //     - when the Channel becomes inactive
      //     - when the handler is removed from the ChannelPipeline
      try {
        if (isCipherValid) {
          cis.close();
        }
      } finally {
        super.handlerRemoved(ctx);
      }
    }
  }

  @VisibleForTesting
  static class EncryptedMessage extends AbstractFileRegion {
    private final boolean isByteBuf;
    private final ByteBuf buf;
    private final FileRegion region;
    private final CryptoOutputStream cos;
    private final EncryptionHandler handler;
    private final long count;
    private long transferred;

    // Due to streaming issue CRYPTO-125: https://issues.apache.org/jira/browse/CRYPTO-125, it has
    // to utilize two helper ByteArrayWritableChannel for streaming. One is used to receive raw data
    // from upper handler, another is used to store encrypted data.
    private ByteArrayWritableChannel byteEncChannel;
    private ByteArrayWritableChannel byteRawChannel;

    private ByteBuffer currentEncrypted;

    EncryptedMessage(
        EncryptionHandler handler,
        CryptoOutputStream cos,
        Object msg,
        ByteArrayWritableChannel ch) {
      Preconditions.checkArgument(msg instanceof ByteBuf || msg instanceof FileRegion,
        "Unrecognized message type: %s", msg.getClass().getName());
      this.handler = handler;
      this.isByteBuf = msg instanceof ByteBuf;
      this.buf = isByteBuf ? (ByteBuf) msg : null;
      this.region = isByteBuf ? null : (FileRegion) msg;
      this.transferred = 0;
      this.byteRawChannel = new ByteArrayWritableChannel(STREAM_BUFFER_SIZE);
      this.cos = cos;
      this.byteEncChannel = ch;
      this.count = isByteBuf ? buf.readableBytes() : region.count();
    }

    @Override
    public long count() {
      return count;
    }

    @Override
    public long position() {
      return 0;
    }

    @Override
    public long transferred() {
      return transferred;
    }

    @Override
    public EncryptedMessage touch(Object o) {
      super.touch(o);
      if (region != null) {
        region.touch(o);
      }
      if (buf != null) {
        buf.touch(o);
      }
      return this;
    }

    @Override
    public EncryptedMessage retain(int increment) {
      super.retain(increment);
      if (region != null) {
        region.retain(increment);
      }
      if (buf != null) {
        buf.retain(increment);
      }
      return this;
    }

    @Override
    public boolean release(int decrement) {
      if (region != null) {
        region.release(decrement);
      }
      if (buf != null) {
        buf.release(decrement);
      }
      return super.release(decrement);
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
      Preconditions.checkArgument(position == transferred(), "Invalid position.");

      if (transferred == count) {
        return 0;
      }

      long totalBytesWritten = 0L;
      do {
        if (currentEncrypted == null) {
          encryptMore();
        }

        long remaining = currentEncrypted.remaining();
        if (remaining == 0)  {
          // Just for safety to avoid endless loop. It usually won't happen, but since the
          // underlying `region.transferTo` is allowed to transfer 0 bytes, we should handle it for
          // safety.
          currentEncrypted = null;
          byteEncChannel.reset();
          return totalBytesWritten;
        }

        long bytesWritten = target.write(currentEncrypted);
        totalBytesWritten += bytesWritten;
        transferred += bytesWritten;
        if (bytesWritten < remaining) {
          // break as the underlying buffer in "target" is full
          break;
        }
        currentEncrypted = null;
        byteEncChannel.reset();
      } while (transferred < count);

      return totalBytesWritten;
    }

    private void encryptMore() throws IOException {
      if (!handler.isCipherValid()) {
        throw new IOException("Cipher is in invalid state.");
      }
      byteRawChannel.reset();

      if (isByteBuf) {
        int copied = byteRawChannel.write(buf.nioBuffer());
        buf.skipBytes(copied);
      } else {
        region.transferTo(byteRawChannel, region.transferred());
      }

      try {
        cos.write(byteRawChannel.getData(), 0, byteRawChannel.length());
        cos.flush();
      } catch (InternalError ie) {
        handler.reportError();
        throw ie;
      }

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
