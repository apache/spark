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
import org.apache.spark.network.protocol.Encoders;

/** Request to get the cause of a corrupted block. Returns {@link CorruptionCause} */
public class DiagnoseCorruption extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final int shuffleId;
  public final long mapId;
  public final int reduceId;
  public final long checksum;
  public final String algorithm;

  public DiagnoseCorruption(
      String appId,
      String execId,
      int shuffleId,
      long mapId,
      int reduceId,
      long checksum,
      String algorithm) {
    this.appId = appId;
    this.execId = execId;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.reduceId = reduceId;
    this.checksum = checksum;
    this.algorithm = algorithm;
  }

  @Override
  protected Type type() {
    return Type.DIAGNOSE_CORRUPTION;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("appId", appId)
      .append("execId", execId)
      .append("shuffleId", shuffleId)
      .append("mapId", mapId)
      .append("reduceId", reduceId)
      .append("checksum", checksum)
      .append("algorithm", algorithm)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DiagnoseCorruption that = (DiagnoseCorruption) o;

    if (checksum != that.checksum) return false;
    if (shuffleId != that.shuffleId) return false;
    if (mapId != that.mapId) return false;
    if (reduceId != that.reduceId) return false;
    if (!algorithm.equals(that.algorithm)) return false;
    if (!appId.equals(that.appId)) return false;
    if (!execId.equals(that.execId)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = appId.hashCode();
    result = 31 * result + execId.hashCode();
    result = 31 * result + Integer.hashCode(shuffleId);
    result = 31 * result + Long.hashCode(mapId);
    result = 31 * result + Integer.hashCode(reduceId);
    result = 31 * result + Long.hashCode(checksum);
    result = 31 * result + algorithm.hashCode();
    return result;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + 4 /* encoded length of shuffleId */
      + 8 /* encoded length of mapId */
      + 4 /* encoded length of reduceId */
      + 8 /* encoded length of checksum */
      + Encoders.Strings.encodedLength(algorithm); /* encoded length of algorithm */
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    buf.writeInt(shuffleId);
    buf.writeLong(mapId);
    buf.writeInt(reduceId);
    buf.writeLong(checksum);
    Encoders.Strings.encode(buf, algorithm);
  }

  public static DiagnoseCorruption decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    long mapId = buf.readLong();
    int reduceId = buf.readInt();
    long checksum = buf.readLong();
    String algorithm = Encoders.Strings.decode(buf);
    return new DiagnoseCorruption(appId, execId, shuffleId, mapId, reduceId, checksum, algorithm);
  }
}
