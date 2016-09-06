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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.spark.network.buffer.ChunkedByteBufferUtil;
import org.apache.spark.network.buffer.InputStreamManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.protocol.AbstractMessage;

/**
 * Encodes a Sasl-related message which is attempting to authenticate using some credentials tagged
 * with the given appId. This appId allows a single SaslRpcHandler to multiplex different
 * applications which may be using different sets of credentials.
 */
class SaslMessage extends AbstractMessage {

  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xEA;

  public final String appId;

  SaslMessage(String appId, byte[] message) {
    this(appId, new NioManagedBuffer(ChunkedByteBufferUtil.wrap(message)));
  }

  SaslMessage(String appId, ManagedBuffer message) {
    super(message, true);
    this.appId = appId;
  }

  @Override
  public Type type() { return Type.User; }

  @Override
  public long encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    return 1 + Encoders.Strings.encodedLength(appId) + 8;
  }

  @Override
  public void encode(OutputStream out) throws IOException {
    Encoders.Bytes.encode(out, TAG_BYTE);
    Encoders.Strings.encode(out, appId);
    Encoders.Longs.encode(out, body().size());
  }

  public static SaslMessage decode(InputStream in) throws IOException {
    if (Encoders.Bytes.decode(in) != TAG_BYTE) {
      throw new IllegalStateException("Expected SaslMessage, received something else"
          + " (maybe your client does not have SASL enabled?)");
    }
    String appId = Encoders.Strings.decode(in);
    long limit = Encoders.Longs.decode(in);
    ManagedBuffer managedBuf = new InputStreamManagedBuffer(in, limit);
    return new SaslMessage(appId, managedBuf);
  }
}
