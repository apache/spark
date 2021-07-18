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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726

/**
 * Request to push a block to a remote shuffle service to be merged in push based shuffle.
 * The remote shuffle service will also include this message when responding the push requests.
 *
 * @since 3.1.0
 */
public class PushBlockStream extends BlockTransferMessage {
  public final String appId;
  public final int appAttemptId;
  public final int shuffleId;
  public final int mapIndex;
  public final int reduceId;
  // Similar to the chunkIndex in StreamChunkId, indicating the index of a block in a batch of
  // blocks to be pushed.
  public final int index;

  public PushBlockStream(
      String appId,
      int appAttemptId,
      int shuffleId,
      int mapIndex,
      int reduceId,
      int index) {
    this.appId = appId;
    this.appAttemptId = appAttemptId;
    this.shuffleId = shuffleId;
    this.mapIndex = mapIndex;
    this.reduceId = reduceId;
    this.index = index;
  }

  @Override
  protected Type type() {
    return Type.PUSH_BLOCK_STREAM;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId, appAttemptId, shuffleId, mapIndex , reduceId, index);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("appId", appId)
      .append("attemptId", appAttemptId)
      .append("shuffleId", shuffleId)
      .append("mapIndex", mapIndex)
      .append("reduceId", reduceId)
      .append("index", index)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof PushBlockStream) {
      PushBlockStream o = (PushBlockStream) other;
      return Objects.equal(appId, o.appId)
        && appAttemptId == o.appAttemptId
        && shuffleId == o.shuffleId
        && mapIndex == o.mapIndex
        && reduceId == o.reduceId
        && index == o.index;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId) + 4 + 4 + 4 + 4 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    buf.writeInt(appAttemptId);
    buf.writeInt(shuffleId);
    buf.writeInt(mapIndex);
    buf.writeInt(reduceId);
    buf.writeInt(index);
  }

  public static PushBlockStream decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    int attemptId = buf.readInt();
    int shuffleId = buf.readInt();
    int mapIdx = buf.readInt();
    int reduceId = buf.readInt();
    int index = buf.readInt();
    return new PushBlockStream(appId, attemptId, shuffleId, mapIdx, reduceId, index);
  }
}
