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
 * Server's response to client's challenge.
 *
 * Please see crypto/README.md for more details.
 */
public class ServerResponse implements Encodable {
  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xFB;

  public final byte[] response;
  public final byte[] nonce;
  public final byte[] inputIv;
  public final byte[] outputIv;

  public ServerResponse(
      byte[] response,
      byte[] nonce,
      byte[] inputIv,
      byte[] outputIv) {
    this.response = response;
    this.nonce = nonce;
    this.inputIv = inputIv;
    this.outputIv = outputIv;
  }

  @Override
  public int encodedLength() {
    return 1 +
      Encoders.ByteArrays.encodedLength(response) +
      Encoders.ByteArrays.encodedLength(nonce) +
      Encoders.ByteArrays.encodedLength(inputIv) +
      Encoders.ByteArrays.encodedLength(outputIv);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    Encoders.ByteArrays.encode(buf, response);
    Encoders.ByteArrays.encode(buf, nonce);
    Encoders.ByteArrays.encode(buf, inputIv);
    Encoders.ByteArrays.encode(buf, outputIv);
  }

  public static ServerResponse decodeMessage(ByteBuffer buffer) {
    ByteBuf buf = Unpooled.wrappedBuffer(buffer);

    if (buf.readByte() != TAG_BYTE) {
      throw new IllegalArgumentException("Expected ServerResponse, received something else.");
    }

    return new ServerResponse(
      Encoders.ByteArrays.decode(buf),
      Encoders.ByteArrays.decode(buf),
      Encoders.ByteArrays.decode(buf),
      Encoders.ByteArrays.decode(buf));
  }

}
