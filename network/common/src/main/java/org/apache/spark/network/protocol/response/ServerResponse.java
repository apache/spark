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

package org.apache.spark.network.protocol.response;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encodable;

/**
 * Messages from server to client (usually in response to some
 * {@link org.apache.spark.network.protocol.request.ClientRequest}.
 */
public interface ServerResponse extends Encodable {
  /** Used to identify this response type. */
  Type type();

  /**
   * Preceding every serialized ServerResponse is the type, which allows us to deserialize
   * the response.
   */
  public static enum Type implements Encodable {
    ChunkFetchSuccess(0), ChunkFetchFailure(1), RpcResponse(2), RpcFailure(3);

    private final byte id;

    private Type(int id) {
      assert id < 128 : "Cannot have more than 128 response types";
      this.id = (byte) id;
    }

    public byte id() { return id; }

    @Override public int encodedLength() { return 1; }

    @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch(id) {
        case 0: return ChunkFetchSuccess;
        case 1: return ChunkFetchFailure;
        case 2: return RpcResponse;
        case 3: return RpcFailure;
        default: throw new IllegalArgumentException("Unknown response type: " + id);
      }
    }
  }
}
