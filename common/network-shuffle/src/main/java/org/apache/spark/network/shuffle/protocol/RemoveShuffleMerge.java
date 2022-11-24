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

import java.util.Objects;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.protocol.Encoders;

/**
 * Request to remove merged shuffle blocks.
 */
public class RemoveShuffleMerge extends BlockTransferMessage {
  public final String appId;
  public final String appAttemptId;
  public final Integer shuffleId;
  public final Integer shuffleMergeId;

  public RemoveShuffleMerge(
      String appId, String appAttemptId, Integer shuffleId, Integer shuffleMergeId) {
    this.appId = appId;
    this.appAttemptId = appAttemptId;
    this.shuffleId = shuffleId;
    this.shuffleMergeId = shuffleMergeId;
  }

  @Override
  protected Type type() { return Type.REMOVE_SHUFFLE_MERGE; }

  @Override
  public int hashCode() {
    return com.google.common.base.Objects
        .hashCode(appId, appAttemptId, shuffleId, shuffleMergeId);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("appId", appId)
        .append("appAttemptId", appAttemptId)
        .append("shuffleId", shuffleId)
        .append("shuffleMergeId", shuffleMergeId)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RemoveShuffleMerge) {
      RemoveShuffleMerge o = (RemoveShuffleMerge) other;
      return Objects.equals(appId, o.appId)
          && Objects.equals(appAttemptId, o.appAttemptId)
          && Objects.equals(shuffleId, o.shuffleId)
          && Objects.equals(shuffleMergeId, o.shuffleMergeId);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
        + 4 /* encoded length of appAttemptId */
        + 4 /* encoded length of shuffleId */
        + 4/* encoded length of shuffleMergeId */;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, appAttemptId);
    buf.writeInt(shuffleId);
    buf.writeInt(shuffleMergeId);
  }

  public static RemoveShuffleMerge decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String appAttemptId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    int shuffleMergeId = buf.readInt();
    return new RemoveShuffleMerge(appId, appAttemptId, shuffleId, shuffleMergeId);
  }
}
