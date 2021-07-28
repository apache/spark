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


/**
 * Request to read a set of block chunks. Returns {@link StreamHandle}.
 *
 * @since 3.2.0
 */
public class FetchShuffleBlockChunks extends AbstractFetchShuffleBlocks {
  // The length of reduceIds must equal to chunkIds.size().
  public final int[] reduceIds;
  // The i-th int[] in chunkIds contains all the chunks for the i-th reduceId in reduceIds.
  public final int[][] chunkIds;
  // shuffleMergeId is used to uniquely identify merging process of shuffle by
  // an indeterminate stage attempt.
  public final int shuffleMergeId;

  public FetchShuffleBlockChunks(
      String appId,
      String execId,
      int shuffleId,
      int shuffleMergeId,
      int[] reduceIds,
      int[][] chunkIds) {
    super(appId, execId, shuffleId);
    this.shuffleMergeId = shuffleMergeId;
    this.reduceIds = reduceIds;
    this.chunkIds = chunkIds;
    assert(reduceIds.length == chunkIds.length);
  }

  @Override
  protected Type type() { return Type.FETCH_SHUFFLE_BLOCK_CHUNKS; }

  @Override
  public String toString() {
    return toStringHelper()
      .append("shuffleMergeId", shuffleMergeId)
      .append("reduceIds", Arrays.toString(reduceIds))
      .append("chunkIds", Arrays.deepToString(chunkIds))
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FetchShuffleBlockChunks that = (FetchShuffleBlockChunks) o;
    if (!super.equals(that)) return false;
    if (shuffleMergeId != that.shuffleMergeId ||
      !Arrays.equals(reduceIds, that.reduceIds)) {
      return false;
    }
    return Arrays.deepEquals(chunkIds, that.chunkIds);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode() * 31 + shuffleMergeId;
    result = 31 * result + Arrays.hashCode(reduceIds);
    result = 31 * result + Arrays.deepHashCode(chunkIds);
    return result;
  }

  @Override
  public int encodedLength() {
    int encodedLengthOfChunkIds = 0;
    for (int[] ids: chunkIds) {
      encodedLengthOfChunkIds += Encoders.IntArrays.encodedLength(ids);
    }
    return super.encodedLength()
      + Encoders.IntArrays.encodedLength(reduceIds)
      + 4 /* encoded length of chunkIds.size() */
      + 4 /* encoded length of shuffleMergeId */
      + encodedLengthOfChunkIds;
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    buf.writeInt(shuffleMergeId);
    Encoders.IntArrays.encode(buf, reduceIds);
    // Even though reduceIds.length == chunkIds.length, we are explicitly setting the length in the
    // interest of forward compatibility.
    buf.writeInt(chunkIds.length);
    for (int[] ids: chunkIds) {
      Encoders.IntArrays.encode(buf, ids);
    }
  }

  @Override
  public int getNumBlocks() {
    int numBlocks = 0;
    for (int[] ids : chunkIds) {
      numBlocks += ids.length;
    }
    return numBlocks;
  }

  public static FetchShuffleBlockChunks decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    int shuffleMergeId = buf.readInt();
    int[] reduceIds = Encoders.IntArrays.decode(buf);
    int chunkIdsLen = buf.readInt();
    int[][] chunkIds = new int[chunkIdsLen][];
    for (int i = 0; i < chunkIdsLen; i++) {
      chunkIds[i] = Encoders.IntArrays.decode(buf);
    }
    return new FetchShuffleBlockChunks(appId, execId, shuffleId, shuffleMergeId, reduceIds,
      chunkIds);
  }
}
