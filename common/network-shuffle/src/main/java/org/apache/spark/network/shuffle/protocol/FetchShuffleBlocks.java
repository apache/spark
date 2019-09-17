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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/** Request to read a set of blocks. Returns {@link StreamHandle}. */
public class FetchShuffleBlocks extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final int shuffleId;
  // The length of mapIds must equal to reduceIds.size(), for the i-th mapId in mapIds,
  // it corresponds to the i-th int[] in reduceIds, which contains all reduce id for this map id.
  public final int[] mapIds;
  public final int[][] reduceIds;

  public FetchShuffleBlocks(
      String appId,
      String execId,
      int shuffleId,
      int[] mapIds,
      int[][] reduceIds) {
    this.appId = appId;
    this.execId = execId;
    this.shuffleId = shuffleId;
    this.mapIds = mapIds;
    this.reduceIds = reduceIds;
    assert(mapIds.length == reduceIds.length);
  }

  @Override
  protected Type type() { return Type.FETCH_SHUFFLE_BLOCKS; }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("execId", execId)
      .add("shuffleId", shuffleId)
      .add("mapIds", Arrays.toString(mapIds))
      .add("reduceIds", Arrays.deepToString(reduceIds))
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FetchShuffleBlocks that = (FetchShuffleBlocks) o;

    if (shuffleId != that.shuffleId) return false;
    if (!appId.equals(that.appId)) return false;
    if (!execId.equals(that.execId)) return false;
    if (!Arrays.equals(mapIds, that.mapIds)) return false;
    return Arrays.deepEquals(reduceIds, that.reduceIds);
  }

  @Override
  public int hashCode() {
    int result = appId.hashCode();
    result = 31 * result + execId.hashCode();
    result = 31 * result + shuffleId;
    result = 31 * result + Arrays.hashCode(mapIds);
    result = 31 * result + Arrays.deepHashCode(reduceIds);
    return result;
  }

  @Override
  public int encodedLength() {
    int encodedLengthOfReduceIds = 0;
    for (int[] ids: reduceIds) {
      encodedLengthOfReduceIds += Encoders.IntArrays.encodedLength(ids);
    }
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + 4 /* encoded length of shuffleId */
      + Encoders.IntArrays.encodedLength(mapIds)
      + 4 /* encoded length of reduceIds.size() */
      + encodedLengthOfReduceIds;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    buf.writeInt(shuffleId);
    Encoders.IntArrays.encode(buf, mapIds);
    buf.writeInt(reduceIds.length);
    for (int[] ids: reduceIds) {
      Encoders.IntArrays.encode(buf, ids);
    }
  }

  public static FetchShuffleBlocks decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    int[] mapIds = Encoders.IntArrays.decode(buf);
    int reduceIdsSize = buf.readInt();
    int[][] reduceIds = new int[reduceIdsSize][];
    for (int i = 0; i < reduceIdsSize; i++) {
      reduceIds[i] = Encoders.IntArrays.decode(buf);
    }
    return new FetchShuffleBlocks(appId, execId, shuffleId, mapIds, reduceIds);
  }
}
