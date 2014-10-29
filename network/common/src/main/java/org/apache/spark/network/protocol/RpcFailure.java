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

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/** Response to {@link RpcRequest} for a failed RPC. */
public final class RpcFailure implements ResponseMessage {
  public final long requestId;
  public final String errorString;

  public RpcFailure(long requestId, String errorString) {
    this.requestId = requestId;
    this.errorString = errorString;
  }

  @Override
  public Type type() { return Type.RpcFailure; }

  @Override
  public int encodedLength() {
    return 8 + 4 + errorString.getBytes(Charsets.UTF_8).length;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    byte[] errorBytes = errorString.getBytes(Charsets.UTF_8);
    buf.writeInt(errorBytes.length);
    buf.writeBytes(errorBytes);
  }

  public static RpcFailure decode(ByteBuf buf) {
    long requestId = buf.readLong();
    int numErrorStringBytes = buf.readInt();
    byte[] errorBytes = new byte[numErrorStringBytes];
    buf.readBytes(errorBytes);
    return new RpcFailure(requestId, new String(errorBytes, Charsets.UTF_8));
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcFailure) {
      RpcFailure o = (RpcFailure) other;
      return requestId == o.requestId && errorString.equals(o.errorString);
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("errorString", errorString)
      .toString();
  }
}
