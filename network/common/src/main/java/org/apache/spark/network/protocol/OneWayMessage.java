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

import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * A RPC that does not expect a reply, which is handled by a remote
 * {@link org.apache.spark.network.server.RpcHandler}.
 */
public final class OneWayMessage implements RequestMessage {
  /** Serialized message to send to remote RpcHandler. */
  public final byte[] message;

  public OneWayMessage(byte[] message) {
    this.message = message;
  }

  @Override
  public Type type() { return Type.OneWayMessage; }

  @Override
  public int encodedLength() {
    return Encoders.ByteArrays.encodedLength(message);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.ByteArrays.encode(buf, message);
  }

  public static OneWayMessage decode(ByteBuf buf) {
    byte[] message = Encoders.ByteArrays.decode(buf);
    return new OneWayMessage(message);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(message);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OneWayMessage) {
      OneWayMessage o = (OneWayMessage) other;
      return Arrays.equals(message, o.message);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("message", message)
      .toString();
  }
}
