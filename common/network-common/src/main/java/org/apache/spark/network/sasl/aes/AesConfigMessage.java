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

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/**
 * The AES cipher options for SASL encryption negotiation.
 */
public class AesConfigMessage implements Encodable {
  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xEB;

  public byte[] inKey;
  public byte[] outKey;
  public byte[] inIv;
  public byte[] outIv;

  public AesConfigMessage(byte[] inKey, byte[] inIv, byte[] outKey, byte[] outIv) {
    this.inKey = inKey;
    this.inIv = inIv;
    this.outKey = outKey;
    this.outIv = outIv;
  }

  /**
   * Set key and input vector for cipher option
   * @param inKey The decrypt key of one side
   * @param inIv The input vector of one side
   * @param outKey The decrypt key of another side
   * @param outIv The input vector of another side
   */
  public void setParameters(byte[] inKey, byte[] inIv, byte[] outKey, byte[] outIv) {
    this.inKey = inKey;
    this.inIv = inIv;
    this.outKey = outKey;
    this.outIv = outIv;
  }

  @Override
  public int encodedLength() {
    return 1 + ((inKey != null && inIv != null && outKey != null && outIv != null) ?
      Encoders.ByteArrays.encodedLength(inKey) + Encoders.ByteArrays.encodedLength(inKey) +
      Encoders.ByteArrays.encodedLength(inIv) + Encoders.ByteArrays.encodedLength(outIv) : 0);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    if (inKey != null && inIv != null && outKey != null && outIv != null) {
      Encoders.ByteArrays.encode(buf, inKey);
      Encoders.ByteArrays.encode(buf, inIv);
      Encoders.ByteArrays.encode(buf, outKey);
      Encoders.ByteArrays.encode(buf, outIv);
    }
  }

  public static AesConfigMessage decode(ByteBuf buf) {
    if (buf.readByte() != TAG_BYTE) {
      throw new IllegalStateException("Expected SaslMessage, received something else"
        + " (maybe your client does not have SASL enabled?)");
    }

    if (buf.readableBytes() > 0) {
      byte[] inKey = Encoders.ByteArrays.decode(buf);
      byte[] inIv = Encoders.ByteArrays.decode(buf);
      byte[] outKey = Encoders.ByteArrays.decode(buf);
      byte[] outIv = Encoders.ByteArrays.decode(buf);
      return new AesConfigMessage(inKey, inIv, outKey, outIv);
    } else {
      return new AesConfigMessage(null, null, null, null);
    }
  }

}
