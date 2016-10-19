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
import java.util.HashMap;
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

  private final SecretKeySpec inKeySpec;
  private final IvParameterSpec inIvSpec;
  private final SecretKeySpec outKeySpec;
  private final IvParameterSpec outIvSpec;
  private Properties properties;

  private HashMap<ReadableByteChannel, CryptoInputStream> inputStreamMap;
  private HashMap<WritableByteChannel, CryptoOutputStream> outputStreamMap;

  public static final int STREAM_BUFFER_SIZE = 1024 * 32;
  public static final String TRANSFORM = "AES/CTR/NoPadding";

  public AesCipher(
      Properties properties,
      byte[] inKey,
      byte[] outKey,
      byte[] inIv,
      byte[] outIv) throws IOException {
    properties.setProperty(CryptoInputStream.STREAM_BUFFER_SIZE_KEY,
      String.valueOf(STREAM_BUFFER_SIZE));
    this.properties = properties;

    inputStreamMap = new HashMap<>();
    outputStreamMap= new HashMap<>();

    inKeySpec = new SecretKeySpec(inKey, "AES");
    inIvSpec = new IvParameterSpec(inIv);
    outKeySpec = new SecretKeySpec(outKey, "AES");
    outIvSpec = new IvParameterSpec(outIv);
  }

  public AesCipher(AesConfigMessage configMessage) throws IOException  {
    this(new Properties(), configMessage.inKey, configMessage.outKey,
      configMessage.inIv, configMessage.outIv);
  }

  /**
   * Create AES crypto output stream
   * @param ch The underlying channel to write out.
   * @return Return output crypto stream for encryption.
   * @throws IOException
   */
  public CryptoOutputStream CreateOutputStream(WritableByteChannel ch) throws IOException {
    if (!outputStreamMap.containsKey(ch)) {
      outputStreamMap.put(ch, new CryptoOutputStream(TRANSFORM, properties, ch, outKeySpec, outIvSpec));
    }

    return outputStreamMap.get(ch);
  }

  /**
   * Create AES crypto input stream
   * @param ch The underlying channel used to read data.
   * @return Return input crypto stream for decryption.
   * @throws IOException
   */
  public CryptoInputStream CreateInputStream(ReadableByteChannel ch) throws IOException {
    if (!inputStreamMap.containsKey(ch)) {
      inputStreamMap.put(ch, new CryptoInputStream(TRANSFORM, properties, ch, inKeySpec, inIvSpec));
    }

    return inputStreamMap.get(ch);
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
   * Generate a request config message which send to remote peer.
   * @param conf is the local transport configuration.
   * @return Config message for sending.
   */
  public static AesConfigMessage requestConfigMessage(TransportConf conf) {
    int keySize = conf.saslEncryptionAesCipherKeySizeBits();
    if (keySize % 8 != 0) {
      throw new IllegalArgumentException("The AES cipher key size in bits should be a multiple " +
        "of byte");
    }
    return new AesConfigMessage(keySize/8, null, null, null, null);
  }

  /**
   * Generate the configuration message according to request config message.
   * @param configMessage The request config message comes from remote.
   * @return Configuration message for sending.
   */
  public static AesConfigMessage responseConfigMessage(AesConfigMessage configMessage){

    Properties properties = new Properties();
    int keyLen = configMessage.keySize;

    try {
      int paramLen = CryptoCipherFactory.getCryptoCipher(AesCipher.TRANSFORM, properties).getBlockSize();
      byte[] inKey = new byte[keyLen];
      byte[] outKey = new byte[keyLen];
      byte[] inIv = new byte[paramLen];
      byte[] outIv = new byte[paramLen];

      CryptoRandom random = CryptoRandomFactory.getCryptoRandom(properties);
      random.nextBytes(inKey);
      random.nextBytes(outKey);
      random.nextBytes(inIv);
      random.nextBytes(outIv);

      configMessage.setParameters(keyLen, inKey, inIv, outKey, outIv);
    } catch (Exception e) {
      logger.error("AES negotiation exception ", e);
      throw Throwables.propagate(e);
    }

    return configMessage;
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
      logger.debug("{} channel decrypted {} bytes", ctx.channel(), totalDecrypted);
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

    // Due to streaming issue CRYPTO-125: https://issues.apache.org/jira/browse/CRYPTO-125, it has
    // to utilize two helper ByteArrayWritableChannel for streaming. One is used to receive raw data
    // from upper handler, another is used to store encrypted data.
    private ByteArrayWritableChannel byteEncChannel;
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
