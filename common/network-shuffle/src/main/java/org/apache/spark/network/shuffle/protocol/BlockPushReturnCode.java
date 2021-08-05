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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.server.BlockPushNonFatalFailure;

/**
 * Error code indicating a non-fatal failure of a block push request.
 * Due to the best-effort nature of push-based shuffle, these failures
 * do not impact the completion of the block push process. The list of
 * such errors is in
 * {@link org.apache.spark.network.server.BlockPushNonFatalFailure.ReturnCode}.
 *
 * @since 3.2.0
 */
public class BlockPushReturnCode extends BlockTransferMessage {
  public final byte returnCode;
  // Block ID of the block that experiences a non-fatal block push failure.
  // Will be an empty string for any successfully pushed block.
  public final String failureBlockId;

  public BlockPushReturnCode(byte returnCode, String failureBlockId) {
    Preconditions.checkNotNull(BlockPushNonFatalFailure.getReturnCode(returnCode));
    this.returnCode = returnCode;
    this.failureBlockId = failureBlockId;
  }

  @Override
  protected Type type() {
    return Type.PUSH_BLOCK_RETURN_CODE;
  }

  @Override
  public int hashCode() {
    return Objects.hash(returnCode, failureBlockId);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("returnCode", returnCode)
      .append("failureBlockId", failureBlockId)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof BlockPushReturnCode) {
      BlockPushReturnCode o = (BlockPushReturnCode) other;
      return returnCode == o.returnCode && Objects.equals(failureBlockId, o.failureBlockId);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return 1 + Encoders.Strings.encodedLength(failureBlockId);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(returnCode);
    Encoders.Strings.encode(buf, failureBlockId);
  }

  public static BlockPushReturnCode decode(ByteBuf buf) {
    byte type = buf.readByte();
    String failureBlockId = Encoders.Strings.decode(buf);
    return new BlockPushReturnCode(type, failureBlockId);
  }
}
