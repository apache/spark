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

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.shuffle.checksum.Cause;

/** Response to the {@link DiagnoseCorruption} */
public class CorruptionCause extends BlockTransferMessage {
  public Cause cause;

  public CorruptionCause(Cause cause) {
    this.cause = cause;
  }

  @Override
  protected Type type() {
    return Type.CORRUPTION_CAUSE;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("cause", cause)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CorruptionCause that = (CorruptionCause) o;
    return cause == that.cause;
  }

  @Override
  public int hashCode() {
    return cause.hashCode();
  }

  @Override
  public int encodedLength() {
    return 1; /* encoded length of cause */
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(cause.ordinal());
  }

  public static CorruptionCause decode(ByteBuf buf) {
    int ordinal = buf.readByte();
    return new CorruptionCause(Cause.values()[ordinal]);
  }
}
