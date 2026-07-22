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
 * Response message containing matching build-side rows for a batched
 * key lookup in distributed map join.
 */
public class BatchLookupResp extends ShardLookupMessage {

  public final long setId;
  public final int shardId;
  public final byte[] rowsData;

  public BatchLookupResp(long setId, int shardId, byte[] rowsData) {
    this.setId = setId;
    this.shardId = shardId;
    this.rowsData = rowsData;
  }

  @Override
  protected ShardLookupMessage.Type type() {
    return Type.BATCH_LOOKUP_RESP;
  }

  @Override
  public int hashCode() {
    return Objects.hash(setId, shardId) * 31 + Arrays.hashCode(rowsData);
  }

  @Override
  public String toString() {
    return String.format("BatchLookupResp[setId=%d,shardId=%d,rowsSize=%d]",
      setId, shardId, rowsData.length);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BatchLookupResp) {
      BatchLookupResp o = (BatchLookupResp) other;
      return setId == o.setId
        && shardId == o.shardId
        && Arrays.equals(rowsData, o.rowsData);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Long.BYTES
      + Integer.BYTES
      + Encoders.ByteArrays.encodedLength(rowsData);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(setId);
    buf.writeInt(shardId);
    Encoders.ByteArrays.encode(buf, rowsData);
  }

  public static BatchLookupResp decode(ByteBuf buf) {
    long setId = buf.readLong();
    int shardId = buf.readInt();
    byte[] rowsData = Encoders.ByteArrays.decode(buf);
    return new BatchLookupResp(setId, shardId, rowsData);
  }
}
