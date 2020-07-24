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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * Request to find the meta information for the specified merged block. The meta information
 * contains the number of chunks in the merged blocks and the maps ids in each chunk.
 */
public class MergedBlockMetaRequest extends AbstractMessage implements RequestMessage {
  public final long requestId;
  public final String appId;
  public final String blockId;

  public MergedBlockMetaRequest(long requestId, String appId, String blockId) {
    super(null, false);
    this.requestId = requestId;
    this.appId = appId;
    this.blockId = blockId;
  }

  @Override
  public Type type() {
    return Type.MergedBlockMetaRequest;
  }

  @Override
  public int encodedLength() {
    return 8 + Encoders.Strings.encodedLength(appId) + Encoders.Strings.encodedLength(blockId);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, blockId);
  }

  public static MergedBlockMetaRequest decode(ByteBuf buf) {
    long requestId = buf.readLong();
    String appId = Encoders.Strings.decode(buf);
    String blockId = Encoders.Strings.decode(buf);
    return new MergedBlockMetaRequest(requestId, appId, blockId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, appId, blockId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof MergedBlockMetaRequest) {
      MergedBlockMetaRequest o = (MergedBlockMetaRequest) other;
      return requestId == o.requestId && Objects.equal(appId, o.appId) && Objects.equal(blockId,
          o.blockId);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("requestId", requestId)
        .add("appId", appId)
        .add("blockId", blockId)
        .toString();
  }
}
