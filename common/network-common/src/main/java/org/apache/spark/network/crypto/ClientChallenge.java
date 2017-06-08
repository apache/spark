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

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/**
 * The client challenge message, used to initiate authentication.
 *
 * Please see crypto/README.md for more details of implementation.
 */
public class ClientChallenge implements Encodable {
  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xFA;

  public final String appId;
  public final String kdf;
  public final int iterations;
  public final String cipher;
  public final int keyLength;
  public final byte[] nonce;
  public final byte[] challenge;

  public ClientChallenge(
      String appId,
      String kdf,
      int iterations,
      String cipher,
      int keyLength,
      byte[] nonce,
      byte[] challenge) {
    this.appId = appId;
    this.kdf = kdf;
    this.iterations = iterations;
    this.cipher = cipher;
    this.keyLength = keyLength;
    this.nonce = nonce;
    this.challenge = challenge;
  }

  @Override
  public int encodedLength() {
    return 1 + 4 + 4 +
      Encoders.Strings.encodedLength(appId) +
      Encoders.Strings.encodedLength(kdf) +
      Encoders.Strings.encodedLength(cipher) +
      Encoders.ByteArrays.encodedLength(nonce) +
      Encoders.ByteArrays.encodedLength(challenge);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, kdf);
    buf.writeInt(iterations);
    Encoders.Strings.encode(buf, cipher);
    buf.writeInt(keyLength);
    Encoders.ByteArrays.encode(buf, nonce);
    Encoders.ByteArrays.encode(buf, challenge);
  }

  public static ClientChallenge decodeMessage(ByteBuffer buffer) {
    ByteBuf buf = Unpooled.wrappedBuffer(buffer);

    if (buf.readByte() != TAG_BYTE) {
      throw new IllegalArgumentException("Expected ClientChallenge, received something else.");
    }

    return new ClientChallenge(
      Encoders.Strings.decode(buf),
      Encoders.Strings.decode(buf),
      buf.readInt(),
      Encoders.Strings.decode(buf),
      buf.readInt(),
      Encoders.ByteArrays.decode(buf),
      Encoders.ByteArrays.decode(buf));
  }

}
