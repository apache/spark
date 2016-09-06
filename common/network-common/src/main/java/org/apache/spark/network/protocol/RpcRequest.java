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

package org.apache.spark.network.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Objects;

import org.apache.spark.network.buffer.InputStreamManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * A generic RPC which is handled by a remote {@link org.apache.spark.network.server.RpcHandler}.
 * This will correspond to a single
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 */
public final class RpcRequest extends AbstractMessage implements RequestMessage {
  /** Used to link an RPC request with its response. */
  public final long requestId;
  public final long bodySize;

  public RpcRequest(long requestId, ManagedBuffer message) {
    this(requestId, message.size(), message);
  }

  public RpcRequest(long requestId, long bodySize, ManagedBuffer buffer) {
    this(requestId, bodySize, true, buffer);
  }

  public RpcRequest(long requestId, long bodySize, boolean isBodyInFrame, ManagedBuffer buffer) {
    super(buffer, isBodyInFrame);
    this.requestId = requestId;
    this.bodySize = bodySize;
  }

  @Override
  public Type type() { return Type.RpcRequest; }

  @Override
  public long encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    return 8 + 8 + 1;
  }

  @Override
  public void encode(OutputStream out) throws IOException {
    Encoders.Longs.encode(out, requestId);
    Encoders.Longs.encode(out, bodySize);
    int ibif = isBodyInFrame() ? 1 : 0;
    Encoders.Bytes.encode(out, (byte) ibif);
  }

  public static RpcRequest decode(InputStream in) throws IOException {
    long requestId = Encoders.Longs.decode(in);
    long byteCount = Encoders.Longs.decode(in);
    boolean isBodyInFrame = Encoders.Bytes.decode(in) == 1;
    ManagedBuffer managedBuf = null;
    if (isBodyInFrame) {
      managedBuf = new InputStreamManagedBuffer(in, byteCount);
    }
    return new RpcRequest(requestId, byteCount, isBodyInFrame, managedBuf);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcRequest) {
      RpcRequest o = (RpcRequest) other;
      return requestId == o.requestId && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("body", body())
      .toString();
  }
}
