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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.AbstractReferenceCounted;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.commons.crypto.stream.CryptoOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.ByteArrayReadableChannel;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.TransportConf;

/**
 * AES cipher for encryption and decryption.
 */
public class AesCipher {
  private static final Logger logger = LoggerFactory.getLogger(AesCipher.class);
  public static final String ENCRYPTION_HANDLER_NAME = "AesEncryption";
  public static final String DECRYPTION_HANDLER_NAME = "AesDecryption";
  public static final int STREAM_BUFFER_SIZE = 1024 * 32;
  public static final String TRANSFORM = "AES/CTR/NoPadding";

  private final SecretKeySpec inKeySpec;
  private final IvParameterSpec inIvSpec;
  private final SecretKeySpec outKeySpec;
  private final IvParameterSpec outIvSpec;
  private final Properties properties;

  public AesCipher(AesConfigMessage configMessage, TransportConf conf) throws IOException  {
    this.properties = CryptoStreamUtils.toCryptoConf(conf);
    this.inKeySpec = new SecretKeySpec(configMessage.inKey, "AES");
    this.inIvSpec = new IvParameterSpec(configMessage.inIv);
    this.outKeySpec = new SecretKeySpec(configMessage.outKey, "AES");
    this.outIvSpec = new IvParameterSpec(configMessage.outIv);
  }

  /**
   * Create AES crypto output stream
   * @param ch The underlying channel to write out.
   * @return Return output crypto stream for encryption.
   * @throws IOException
   */
  private CryptoOutputStream createOutputStream(WritableByteChannel ch) throws IOException {
    return new CryptoOutputStream(TRANSFORM, properties, ch, outKeySpec, outIvSpec);
  }

  /**
   * Create AES crypto input stream
   * @param ch The underlying channel used to read data.
   * @return Return input crypto stream for decryption.
   * @throws IOException
   */
  private CryptoInputStream createInputStream(ReadableByteChannel ch) throws IOException {
    return new CryptoInputStream(TRANSFORM, properties, ch, inKeySpec, inIvSpec);
  }

  /**
   * Add handlers to channel
   * @param ch the channel for adding handlers
   * @throws IOException
   */
  public void addToChannel(Channel ch) throws IOException {
    ch.pipeline()
      .addFirst(ENCRYPTION_HANDLER_NAME, new AesEncryptHandler(this))
      .addFirst(DECRYPTION_HANDLER_NAME, new AesDecryptHandler(this));
  }

  /**
   * Create the configuration message
   * @param conf is the local transport configuration.
   * @return Config message for sending.
   */
  public static AesConfigMessage createConfigMessage(TransportConf conf) {
    int keySize = conf.aesCipherKeySize();
    Properties properties = CryptoStreamUtils.toCryptoConf(conf);

    try {
      int paramLen = CryptoCipherFactory.getCryptoCipher(AesCipher.TRANSFORM, properties)
        .getBlockSize();
      byte[] inKey = new byte[keySize];
      byte[] outKey = new byte[keySize];
      byte[] inIv = new byte[paramLen];
      byte[] outIv = new byte[paramLen];

      CryptoRandom random = CryptoRandomFactory.getCryptoRandom(properties);
      random.nextBytes(inKey);
      random.nextBytes(outKey);
      random.nextBytes(inIv);
      random.nextBytes(outIv);

      return new AesConfigMessage(inKey, inIv, outKey, outIv);
    } catch (Exception e) {
      logger.error("AES config error", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * CryptoStreamUtils is used to convert config from TransportConf to AES Crypto config.
   */
  private static class CryptoStreamUtils {
    public static Properties toCryptoConf(TransportConf conf) {
      Properties props = new Properties();
      if (conf.aesCipherClass() != null) {
        props.setProperty(CryptoCipherFactory.CLASSES_KEY, conf.aesCipherClass());
      }
      return props;
    }
  }

  private static class AesEncryptHandler extends ChannelOutboundHandlerAdapter {
    private final ByteArrayWritableChannel byteChannel;
    private final CryptoOutputStream cos;

    AesEncryptHandler(AesCipher cipher) throws IOException {
      byteChannel = new ByteArrayWritableChannel(AesCipher.STREAM_BUFFER_SIZE);
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

  private static class AesDecryptHandler extends ChannelInboundHandlerAdapter {
    private final CryptoInputStream cis;
    private final ByteArrayReadableChannel byteChannel;

    AesDecryptHandler(AesCipher cipher) throws IOException {
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
      this.byteRawChannel = new ByteArrayWritableChannel(AesCipher.STREAM_BUFFER_SIZE);
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
