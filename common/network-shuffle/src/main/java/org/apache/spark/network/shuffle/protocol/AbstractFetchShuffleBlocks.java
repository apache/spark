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

/**
 * Base class for fetch shuffle blocks and chunks.
 *
 * @since 3.2.0
 */
public abstract class AbstractFetchShuffleBlocks extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final int shuffleId;

  protected AbstractFetchShuffleBlocks(
      String appId,
      String execId,
      int shuffleId) {
    this.appId = appId;
    this.execId = execId;
    this.shuffleId = shuffleId;
  }

  public ToStringBuilder toStringHelper() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("appId", appId)
      .append("execId", execId)
      .append("shuffleId", shuffleId);
  }

  /**
   * Returns number of blocks in the request.
   */
  public abstract int getNumBlocks();

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AbstractFetchShuffleBlocks that = (AbstractFetchShuffleBlocks) o;
    return shuffleId == that.shuffleId
      && Objects.equal(appId, that.appId) && Objects.equal(execId, that.execId);
  }

  @Override
  public int hashCode() {
    int result = appId.hashCode();
    result = 31 * result + execId.hashCode();
    result = 31 * result + shuffleId;
    return result;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + 4; /* encoded length of shuffleId */
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    buf.writeInt(shuffleId);
  }
}
