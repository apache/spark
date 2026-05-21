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

import java.util.Arrays;
import java.util.Objects;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

/**
 * Request message for a batched key lookup in distributed map join.
 * Contains the shard set ID, target shard, number of key fields,
 * and the serialized key data.
 */
public class BatchLookupReq extends ShardLookupMessage {

  public final long setId;
  public final int shardId;
  public final int numFields;
  public final byte[] keysData;

  public BatchLookupReq(long setId, int shardId, int numFields, byte[] keysData) {
    this.setId = setId;
    this.shardId = shardId;
    this.numFields = numFields;
    this.keysData = keysData;
  }

  @Override
  protected Type type() {
    return Type.BATCH_LOOKUP_REQ;
  }

  @Override
  public int hashCode() {
    return (Objects.hash(setId, shardId) * 31 + Objects.hash(numFields)) * 31 +
      Arrays.hashCode(keysData);
  }

  @Override
  public String toString() {
    return String.format("BatchLookupReq[setId=%d,shardId=%d,numFields=%d,keysSize=%d]",
      setId, shardId, numFields, keysData.length);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BatchLookupReq) {
      BatchLookupReq o = (BatchLookupReq) other;
      return setId == o.setId
        && shardId == o.shardId
        && numFields == o.numFields
        && Arrays.equals(keysData, o.keysData);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Long.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Encoders.ByteArrays.encodedLength(keysData);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(setId);
    buf.writeInt(shardId);
    buf.writeInt(numFields);
    Encoders.ByteArrays.encode(buf, keysData);
  }

  public static BatchLookupReq decode(ByteBuf buf) {
    long setId = buf.readLong();
    int shardId = buf.readInt();
    int numFields = buf.readInt();
    byte[] keysData = Encoders.ByteArrays.decode(buf);
    return new BatchLookupReq(setId, shardId, numFields, keysData);
  }
}
