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
 * A message sent in the forward secure authentication protocol, containing an app ID, a salt for
 * key derivation, and an encrypted payload.
 *
 * Please see crypto/README.md for more details of implementation.
 */
record AuthMessage(String appId, byte[] salt, byte[] ciphertext) implements Encodable {
  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xFB;

  @Override
  public int encodedLength() {
    return 1 +
      Encoders.Strings.encodedLength(appId) +
      Encoders.ByteArrays.encodedLength(salt) +
      Encoders.ByteArrays.encodedLength(ciphertext);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    Encoders.Strings.encode(buf, appId);
    Encoders.ByteArrays.encode(buf, salt);
    Encoders.ByteArrays.encode(buf, ciphertext);
  }

  public static AuthMessage decodeMessage(ByteBuffer buffer) {
    ByteBuf buf = Unpooled.wrappedBuffer(buffer);

    if (buf.readByte() != TAG_BYTE) {
      throw new IllegalArgumentException("Expected ClientChallenge, received something else.");
    }

    return new AuthMessage(
      Encoders.Strings.decode(buf),  // AppID
      Encoders.ByteArrays.decode(buf),  // Salt
      Encoders.ByteArrays.decode(buf));  // Ciphertext
  }
}
