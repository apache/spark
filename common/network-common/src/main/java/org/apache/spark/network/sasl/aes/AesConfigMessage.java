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
 * The AES cipher options for encryption negotiation.
 */
public class AesConfigMessage implements Encodable {
  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xEB;

  public byte[] inKey;
  public byte[] outKey;
  public byte[] inIv;
  public byte[] outIv;

  public AesConfigMessage(byte[] inKey, byte[] inIv, byte[] outKey, byte[] outIv) {
    if (inKey == null || inIv == null || outKey == null || outIv == null) {
      throw new IllegalArgumentException("Cipher Key or IV must not be null!");
    }

    this.inKey = inKey;
    this.inIv = inIv;
    this.outKey = outKey;
    this.outIv = outIv;
  }

  @Override
  public int encodedLength() {
    return 1 +
      Encoders.ByteArrays.encodedLength(inKey) + Encoders.ByteArrays.encodedLength(outKey) +
      Encoders.ByteArrays.encodedLength(inIv) + Encoders.ByteArrays.encodedLength(outIv);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    Encoders.ByteArrays.encode(buf, inKey);
    Encoders.ByteArrays.encode(buf, inIv);
    Encoders.ByteArrays.encode(buf, outKey);
    Encoders.ByteArrays.encode(buf, outIv);
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
      throw new IllegalStateException("Expected AesConfigMessage, received something else"
        + " (maybe your client does not have AES enabled?)");
    }

    byte[] outKey = Encoders.ByteArrays.decode(buf);
    byte[] outIv = Encoders.ByteArrays.decode(buf);
    byte[] inKey = Encoders.ByteArrays.decode(buf);
    byte[] inIv = Encoders.ByteArrays.decode(buf);
    return new AesConfigMessage(inKey, inIv, outKey, outIv);
  }

}
