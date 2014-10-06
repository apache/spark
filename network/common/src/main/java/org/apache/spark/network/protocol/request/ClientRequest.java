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

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encodable;

/** Messages from the client to the server. */
public interface ClientRequest extends Encodable {
  /** Used to identify this request type. */
  Type type();

  /**
   * Preceding every serialized ClientRequest is the type, which allows us to deserialize
   * the request.
   */
  public static enum Type implements Encodable {
    ChunkFetchRequest(0), RpcRequest(1);

    private final byte id;

    private Type(int id) {
      assert id < 128 : "Cannot have more than 128 request types";
      this.id = (byte) id;
    }

    public byte id() { return id; }

    @Override public int encodedLength() { return 1; }

    @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch(id) {
        case 0: return ChunkFetchRequest;
        case 1: return RpcRequest;
        default: throw new IllegalArgumentException("Unknown request type: " + id);
      }
    }
  }
}
