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

package org.apache.spark.network.shard.protocol;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.protocol.Encodable;

/**
 * Base class for shard lookup RPC messages used in distributed map join.
 * Provides encoding/decoding and type-based dispatch similar to
 * {@code BlockTransferMessage}.
 */
public abstract class ShardLookupMessage implements Encodable {

  protected abstract Type type();

  public enum Type {
    BATCH_LOOKUP_REQ(0), BATCH_LOOKUP_RESP(1);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() {
      return id;
    }
  }

  public static class Decoder {
    public static ShardLookupMessage fromByteBuffer(ByteBuffer msg) {
      ByteBuf buf = Unpooled.wrappedBuffer(msg);
      byte type = buf.readByte();
      switch (type) {
        case 0: // BATCH_LOOKUP_REQ
          return BatchLookupReq.decode(buf);
        case 1: // BATCH_LOOKUP_RESP
          return BatchLookupResp.decode(buf);
        default:
          throw new IllegalArgumentException("Unknown message type: " + type);
      }
    }
  }

  public ByteBuffer toByteBuffer() {
    ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
    buf.writeByte(type().id());
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.nioBuffer();
  }
}
