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
import org.apache.spark.network.protocol.Encoders;

import java.util.Objects;

// Needed by ScalaDoc. See SPARK-7726


/** Response message for {@link ConnectWriteRequest}. */
public class ConnectWriteResponse extends RemoteShuffleMessage {
  public final String serverId;

  public ConnectWriteResponse(String serverId) {
    this.serverId = serverId;
  }

  @Override
  protected Type type() { return Type.CONNECT_WRITE_RESPONSE; }

  @Override
  public int hashCode() {
    return Objects.hash(serverId);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("serverId", serverId)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof ConnectWriteResponse) {
      ConnectWriteResponse o = (ConnectWriteResponse) other;
      return Objects.equals(serverId, o.serverId);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(serverId);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, serverId);;
  }

  public static ConnectWriteResponse decode(ByteBuf buf) {
    String serverId = Encoders.Strings.decode(buf);
    return new ConnectWriteResponse(serverId);
  }
}
