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
public class OpenBlocks extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final String[] blockIds;
  // When fetchContinuousShuffleBlocksInBatch == true, OpenBlocks could contain ShuffleBlockId only
  public final boolean fetchContinuousShuffleBlocksInBatch;

  // This is only used in tests.
  public OpenBlocks(String appId, String execId, String[] blockIds) {
    this(appId, execId, blockIds, false);
  }

  public OpenBlocks(
      String appId,
      String execId,
      String[] blockIds,
      boolean fetchContinuousShuffleBlocksInBatch) {
    this.appId = appId;
    this.execId = execId;
    this.blockIds = blockIds;
    this.fetchContinuousShuffleBlocksInBatch = fetchContinuousShuffleBlocksInBatch;
  }

  @Override
  protected Type type() { return Type.OPEN_BLOCKS; }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId, execId, fetchContinuousShuffleBlocksInBatch) * 41
        + Arrays.hashCode(blockIds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("execId", execId)
      .add("blockIds", Arrays.toString(blockIds))
      .add("fetchContinuousShuffleBlocksInBatch", fetchContinuousShuffleBlocksInBatch)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof OpenBlocks) {
      OpenBlocks o = (OpenBlocks) other;
      return Objects.equal(appId, o.appId)
        && Objects.equal(execId, o.execId)
        && Arrays.equals(blockIds, o.blockIds)
        && Objects.equal(fetchContinuousShuffleBlocksInBatch,
          o.fetchContinuousShuffleBlocksInBatch);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + Encoders.StringArrays.encodedLength(blockIds)
      + 1;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    Encoders.StringArrays.encode(buf, blockIds);
    buf.writeBoolean(fetchContinuousShuffleBlocksInBatch);
  }

  public static OpenBlocks decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    String[] blockIds = Encoders.StringArrays.decode(buf);
    boolean fetchContinuousShuffleBlocksInBatch = false;
    if (buf.readableBytes() != 0) {
      fetchContinuousShuffleBlocksInBatch = buf.readBoolean();
    }
    return new OpenBlocks(appId, execId, blockIds, fetchContinuousShuffleBlocksInBatch);
  }
}
