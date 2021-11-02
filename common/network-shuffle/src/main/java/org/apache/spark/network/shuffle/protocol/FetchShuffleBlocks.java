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

package org.apache.spark.network.shuffle.protocol;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/** Request to read a set of blocks. Returns {@link StreamHandle}. */
public class FetchShuffleBlocks extends AbstractFetchShuffleBlocks {
  // The length of mapIds must equal to reduceIds.size(), for the i-th mapId in mapIds,
  // it corresponds to the i-th int[] in reduceIds, which contains all reduce id for this map id.
  public final long[] mapIds;
  // When batchFetchEnabled=true, reduceIds[i] contains 2 elements: startReduceId (inclusive) and
  // endReduceId (exclusive) for the mapper mapIds[i].
  // When batchFetchEnabled=false, reduceIds[i] contains all the reduce IDs that mapper mapIds[i]
  // needs to fetch.
  public final int[][] reduceIds;
  public final boolean batchFetchEnabled;

  public FetchShuffleBlocks(
      String appId,
      String execId,
      int shuffleId,
      long[] mapIds,
      int[][] reduceIds,
      boolean batchFetchEnabled) {
    super(appId, execId, shuffleId);
    this.mapIds = mapIds;
    this.reduceIds = reduceIds;
    assert(mapIds.length == reduceIds.length);
    this.batchFetchEnabled = batchFetchEnabled;
    if (batchFetchEnabled) {
      for (int[] ids: reduceIds) {
        assert(ids.length == 2);
      }
    }
  }

  @Override
  protected Type type() { return Type.FETCH_SHUFFLE_BLOCKS; }

  @Override
  public String toString() {
    return toStringHelper()
      .append("mapIds", Arrays.toString(mapIds))
      .append("reduceIds", Arrays.deepToString(reduceIds))
      .append("batchFetchEnabled", batchFetchEnabled)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FetchShuffleBlocks that = (FetchShuffleBlocks) o;
    if (!super.equals(that)) return false;
    if (batchFetchEnabled != that.batchFetchEnabled) return false;
    if (!Arrays.equals(mapIds, that.mapIds)) return false;
    return Arrays.deepEquals(reduceIds, that.reduceIds);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(mapIds);
    result = 31 * result + Arrays.deepHashCode(reduceIds);
    result = 31 * result + (batchFetchEnabled ? 1 : 0);
    return result;
  }

  @Override
  public int getNumBlocks() {
    if (batchFetchEnabled) {
      return mapIds.length;
    }
    int numBlocks = 0;
    for (int[] ids : reduceIds) {
      numBlocks += ids.length;
    }
    return numBlocks;
  }

  @Override
  public int encodedLength() {
    int encodedLengthOfReduceIds = 0;
    for (int[] ids: reduceIds) {
      encodedLengthOfReduceIds += Encoders.IntArrays.encodedLength(ids);
    }
    return super.encodedLength()
      + Encoders.LongArrays.encodedLength(mapIds)
      + 4 /* encoded length of reduceIds.size() */
      + encodedLengthOfReduceIds
      + 1; /* encoded length of batchFetchEnabled */
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    Encoders.LongArrays.encode(buf, mapIds);
    buf.writeInt(reduceIds.length);
    for (int[] ids: reduceIds) {
      Encoders.IntArrays.encode(buf, ids);
    }
    buf.writeBoolean(batchFetchEnabled);
  }

  public static FetchShuffleBlocks decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    long[] mapIds = Encoders.LongArrays.decode(buf);
    int reduceIdsSize = buf.readInt();
    int[][] reduceIds = new int[reduceIdsSize][];
    for (int i = 0; i < reduceIdsSize; i++) {
      reduceIds[i] = Encoders.IntArrays.decode(buf);
    }
    boolean batchFetchEnabled = buf.readBoolean();
    return new FetchShuffleBlocks(appId, execId, shuffleId, mapIds, reduceIds, batchFetchEnabled);
  }
}
