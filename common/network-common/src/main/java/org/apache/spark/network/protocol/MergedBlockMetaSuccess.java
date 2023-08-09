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
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

/**
 * Response to {@link MergedBlockMetaRequest} request.
 * Note that the server-side encoding of this messages does NOT include the buffer itself.
 *
 * @since 3.2.0
 */
public class MergedBlockMetaSuccess extends AbstractResponseMessage {
  public final long requestId;
  public final int numChunks;

  public MergedBlockMetaSuccess(
      long requestId,
      int numChunks,
      ManagedBuffer chunkBitmapsBuffer) {
    super(chunkBitmapsBuffer, true);
    this.requestId = requestId;
    this.numChunks = numChunks;
  }

  @Override
  public Type type() {
    return Type.MergedBlockMetaSuccess;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, numChunks);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("requestId", requestId).append("numChunks", numChunks).toString();
  }

  @Override
  public int encodedLength() {
    return 8 + 4;
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    buf.writeInt(numChunks);
  }

  public int getNumChunks() {
    return numChunks;
  }

  /** Decoding uses the given ByteBuf as our data, and will retain() it. */
  public static MergedBlockMetaSuccess decode(ByteBuf buf) {
    long requestId = buf.readLong();
    int numChunks = buf.readInt();
    buf.retain();
    NettyManagedBuffer managedBuf = new NettyManagedBuffer(buf.duplicate());
    return new MergedBlockMetaSuccess(requestId, numChunks, managedBuf);
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new RpcFailure(requestId, error);
  }
}
