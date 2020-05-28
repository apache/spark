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

package org.apache.spark.network.remoteshuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

// Needed by ScalaDoc. See SPARK-7726


/** Response message for {@link ConnectWriteRequest}. */
public class ConnectWriteResponse extends RemoteShuffleMessage {
  public final long sessionId;

  public ConnectWriteResponse(long sessionId) {
    this.sessionId = sessionId;
  }

  @Override
  protected Type type() { return Type.CONNECT_WRITE_RESPONSE; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConnectWriteResponse that = (ConnectWriteResponse) o;
    return sessionId == that.sessionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sessionId);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("sessionId", sessionId)
        .toString();
  }

  @Override
  public int encodedLength() {
    return Long.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(sessionId);
  }

  public static ConnectWriteResponse decode(ByteBuf buf) {
    long streamId = buf.readLong();
    return new ConnectWriteResponse(streamId);
  }
}
