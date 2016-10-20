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

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/**
 * The AES cipher options for SASL encryption negotiation.
 */
public class AesConfigMessage implements Encodable {
  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xEB;

  public int keySize;
  public byte[] inKey;
  public byte[] outKey;
  public byte[] inIv;
  public byte[] outIv;

  public AesConfigMessage(int keySize, byte[] inKey, byte[] inIv, byte[] outKey, byte[] outIv) {
    this.keySize = keySize;
    this.inKey = inKey;
    this.inIv = inIv;
    this.outKey = outKey;
    this.outIv = outIv;
  }

  /**
   * Set key and input vector for cipher option
   * @param keySize the size of key in byte.
   * @param inKey The decrypt key of one side.
   * @param inIv The input vector of one side.
   * @param outKey The decrypt key of another side.
   * @param outIv The input vector of another side.
   */
  public void setParameters(int keySize, byte[] inKey, byte[] inIv, byte[] outKey, byte[] outIv) {
    this.keySize = keySize;
    this.inKey = inKey;
    this.inIv = inIv;
    this.outKey = outKey;
    this.outIv = outIv;
  }

  @Override
  public int encodedLength() {
    return 1 + 4 + ((inKey != null && inIv != null && outKey != null && outIv != null) ?
      Encoders.ByteArrays.encodedLength(inKey) + Encoders.ByteArrays.encodedLength(inKey) +
      Encoders.ByteArrays.encodedLength(inIv) + Encoders.ByteArrays.encodedLength(outIv) : 0);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    buf.writeInt(keySize);
    if (inKey != null && inIv != null && outKey != null && outIv != null) {
      Encoders.ByteArrays.encode(buf, inKey);
      Encoders.ByteArrays.encode(buf, inIv);
      Encoders.ByteArrays.encode(buf, outKey);
      Encoders.ByteArrays.encode(buf, outIv);
    }
  }

  /**
   * Encode the config message.
   * @return ByteBuffer which contains encoded config message.
   */
  public ByteBuffer encodeMessage(){
    ByteBuffer buf = ByteBuffer.allocate(encodedLength());

    ByteBuf wrappedBuf = Unpooled.wrappedBuffer(buf);
    wrappedBuf.clear();
    encode(wrappedBuf);

    return buf;
  }

  /**
   * Decode the config message from buffer
   * @param buffer the buffer contain encoded config message
   * @return config message
   */
  public static AesConfigMessage decodeMessage(ByteBuffer buffer) {
    ByteBuf buf = Unpooled.wrappedBuffer(buffer);

    if (buf.readByte() != TAG_BYTE) {
      throw new IllegalStateException("Expected SaslMessage, received something else"
        + " (maybe your client does not have SASL enabled?)");
    }

    int keySize = buf.readInt();

    if (buf.readableBytes() > 0) {
      byte[] inKey = Encoders.ByteArrays.decode(buf);
      byte[] inIv = Encoders.ByteArrays.decode(buf);
      byte[] outKey = Encoders.ByteArrays.decode(buf);
      byte[] outIv = Encoders.ByteArrays.decode(buf);
      return new AesConfigMessage(keySize, inKey, inIv, outKey, outIv);
    } else {
      return new AesConfigMessage(keySize, null, null, null, null);
    }
  }

}
