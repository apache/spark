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

  public BlockPushReturnCode(byte returnCode) {
    Preconditions.checkNotNull(BlockPushNonFatalFailure.getReturnCode(returnCode));
    this.returnCode = returnCode;
  }

  @Override
  protected Type type() {
    return Type.PUSH_BLOCK_RETURN_CODE;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(returnCode);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("returnCode", returnCode)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof BlockPushReturnCode) {
      BlockPushReturnCode o = (BlockPushReturnCode) other;
      return returnCode == o.returnCode;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return 1;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(returnCode);
  }

  public static BlockPushReturnCode decode(ByteBuf buf) {
    byte type = buf.readByte();
    return new BlockPushReturnCode(type);
  }
}
