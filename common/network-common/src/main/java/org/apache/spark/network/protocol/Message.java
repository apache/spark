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

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;

/** An on-the-wire transmittable message. */
public interface Message extends Encodable {
  /** Used to identify this request type. */
  Type type();

  /** An optional body for the message. */
  ManagedBuffer body();

  /** Whether to include the body of the message in the same frame as the message. */
  boolean isBodyInFrame();

  /** Preceding every serialized Message is its type, which allows us to deserialize it. */
  enum Type implements Encodable {
    ChunkFetchRequest(0), ChunkFetchSuccess(1), ChunkFetchFailure(2),
    RpcRequest(3), RpcResponse(4), RpcFailure(5),
    StreamRequest(6), StreamResponse(7), StreamFailure(8),
    OneWayMessage(9), UploadStream(10), MergedBlockMetaRequest(11), MergedBlockMetaSuccess(12),
    User(-1);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() { return id; }

    @Override public int encodedLength() { return 1; }

    @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      return switch (id) {
        case 0 -> ChunkFetchRequest;
        case 1 -> ChunkFetchSuccess;
        case 2 -> ChunkFetchFailure;
        case 3 -> RpcRequest;
        case 4 -> RpcResponse;
        case 5 -> RpcFailure;
        case 6 -> StreamRequest;
        case 7 -> StreamResponse;
        case 8 -> StreamFailure;
        case 9 -> OneWayMessage;
        case 10 -> UploadStream;
        case 11 -> MergedBlockMetaRequest;
        case 12 -> MergedBlockMetaSuccess;
        case -1 -> throw new IllegalArgumentException("User type messages cannot be decoded.");
        default -> throw new IllegalArgumentException("Unknown message type: " + id);
      };
    }
  }
}
