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

package org.apache.spark.network.sasl;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/**
 * Encodes a Sasl-related message which is attempting to authenticate using some credentials tagged
 * with the given appId. This appId allows a single SaslRpcHandler to multiplex different
 * applications which may be using different sets of credentials.
 */
class SaslMessage implements Encodable {

  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xEA;

  public final String appId;
  public final byte[] payload;

  public SaslMessage(String appId, byte[] payload) {
    this.appId = appId;
    this.payload = payload;
  }

  @Override
  public int encodedLength() {
    return 1 + Encoders.Strings.encodedLength(appId) + Encoders.ByteArrays.encodedLength(payload);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    Encoders.Strings.encode(buf, appId);
    Encoders.ByteArrays.encode(buf, payload);
  }

  public static SaslMessage decode(ByteBuf buf) {
    if (buf.readByte() != TAG_BYTE) {
      throw new IllegalStateException("Expected SaslMessage, received something else"
        + " (maybe your client does not have SASL enabled?)");
    }

    String appId = Encoders.Strings.decode(buf);
    byte[] payload = Encoders.ByteArrays.decode(buf);
    return new SaslMessage(appId, payload);
  }
}
