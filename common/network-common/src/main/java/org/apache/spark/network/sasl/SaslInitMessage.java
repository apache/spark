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

import org.apache.spark.network.protocol.Encoders;

/**
 * Encodes the first message in Sasl exchange. When it is retried, the
 * SaslServer needs to be reset. {@link SaslRpcHandler} uses the type of this message
 * to reset the SaslServer.
 */
public final class SaslInitMessage extends SaslMessage {

  /** Serialization tag used to catch incorrect payloads. */
  static final byte TAG_BYTE = (byte) 0xEB;

  SaslInitMessage(String appId, byte[] message) {
    super(appId, message);
  }

  SaslInitMessage(String appId, ByteBuf message) {
    super(appId, message);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    Encoders.Strings.encode(buf, appId);
    // See comment in encodedLength().
    buf.writeInt((int) body().size());
  }
}
