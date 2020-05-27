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


/** Request to connect shuffle write. Returns {@link ConnectWriteResponse}. */
public class ConnectWriteRequest extends RemoteShuffleMessage {
  public final String appId;
  public final String execId;
  public final int shuffleId;
  public final int stageAttempt;
  public final int numMaps;

  public ConnectWriteRequest(String appId, String execId, int shuffleId, int stageAttempt, int numMaps) {
    this.appId = appId;
    this.execId = execId;
    this.shuffleId = shuffleId;
    this.stageAttempt = stageAttempt;
    this.numMaps = numMaps;
  }

  @Override
  protected Type type() { return Type.CONNECT_WRITE_REQUEST; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConnectWriteRequest that = (ConnectWriteRequest) o;
    return shuffleId == that.shuffleId &&
        stageAttempt == that.stageAttempt &&
        numMaps == that.numMaps &&
        Objects.equals(appId, that.appId) &&
        Objects.equals(execId, that.execId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, execId, shuffleId, stageAttempt, numMaps);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("appId", appId)
        .append("execId", execId)
        .append("shuffleId", shuffleId)
        .append("stageAttempt", stageAttempt)
        .append("numMaps", numMaps)
        .toString();
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);;
    buf.writeInt(shuffleId);
    buf.writeInt(stageAttempt);
    buf.writeInt(numMaps);
  }

  public static ConnectWriteRequest decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    int stageAttempt = buf.readInt();
    int numMaps = buf.readInt();
    return new ConnectWriteRequest(appId, execId, shuffleId, stageAttempt, numMaps);
  }
}
