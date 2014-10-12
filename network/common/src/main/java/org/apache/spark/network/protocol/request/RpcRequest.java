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

package org.apache.spark.network.protocol.request;

import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * A generic RPC which is handled by a remote {@link org.apache.spark.network.server.RpcHandler}.
 * This will correspond to a single
 * {@link org.apache.spark.network.protocol.response.ResponseMessage} (either success or failure).
 */
public final class RpcRequest implements RequestMessage {
  /** Tag is used to link an RPC request with its response. */
  public final long tag;

  /** Serialized message to send to remote RpcHandler. */
  public final byte[] message;

  public RpcRequest(long tag, byte[] message) {
    this.tag = tag;
    this.message = message;
  }

  @Override
  public Type type() { return Type.RpcRequest; }

  @Override
  public int encodedLength() {
    return 8 + 4 + message.length;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(tag);
    buf.writeInt(message.length);
    buf.writeBytes(message);
  }

  public static RpcRequest decode(ByteBuf buf) {
    long tag = buf.readLong();
    int messageLen = buf.readInt();
    byte[] message = new byte[messageLen];
    buf.readBytes(message);
    return new RpcRequest(tag, message);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcRequest) {
      RpcRequest o = (RpcRequest) other;
      return tag == o.tag && Arrays.equals(message, o.message);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("tag", tag)
      .add("message", message)
      .toString();
  }
}
