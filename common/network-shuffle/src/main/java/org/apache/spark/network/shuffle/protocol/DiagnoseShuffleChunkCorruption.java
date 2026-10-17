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
import org.apache.spark.network.protocol.Encoders;

/**
 * Request to get the cause of a corrupted chunk of a merged shuffle partition. Unlike
 * {@link DiagnoseCorruption}, this is only served by the external shuffle service that merged
 * the chunk, so it identifies the chunk by shuffleMergeId and chunkId instead of by the executor
 * and map which produced it. Returns {@link CorruptionCause}
 */
public class DiagnoseShuffleChunkCorruption extends BlockTransferMessage {
  public final String appId;
  public final int shuffleId;
  public final int shuffleMergeId;
  public final int reduceId;
  public final int chunkId;
  public final long checksum;
  public final String algorithm;

  public DiagnoseShuffleChunkCorruption(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      int chunkId,
      long checksum,
      String algorithm) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.shuffleMergeId = shuffleMergeId;
    this.reduceId = reduceId;
    this.chunkId = chunkId;
    this.checksum = checksum;
    this.algorithm = algorithm;
  }

  @Override
  protected Type type() {
    return Type.DIAGNOSE_SHUFFLE_CHUNK_CORRUPTION;
  }

  @Override
  public String toString() {
    return "DiagnoseShuffleChunkCorruption[appId=" + appId + ",shuffleId=" + shuffleId +
        ",shuffleMergeId=" + shuffleMergeId + ",reduceId=" + reduceId + ",chunkId=" + chunkId +
        ",checksum=" + checksum + ",algorithm=" + algorithm + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DiagnoseShuffleChunkCorruption that = (DiagnoseShuffleChunkCorruption) o;

    if (checksum != that.checksum) return false;
    if (shuffleId != that.shuffleId) return false;
    if (shuffleMergeId != that.shuffleMergeId) return false;
    if (reduceId != that.reduceId) return false;
    if (chunkId != that.chunkId) return false;
    if (!algorithm.equals(that.algorithm)) return false;
    return appId.equals(that.appId);
  }

  @Override
  public int hashCode() {
    int result = appId.hashCode();
    result = 31 * result + Integer.hashCode(shuffleId);
    result = 31 * result + Integer.hashCode(shuffleMergeId);
    result = 31 * result + Integer.hashCode(reduceId);
    result = 31 * result + Integer.hashCode(chunkId);
    result = 31 * result + Long.hashCode(checksum);
    result = 31 * result + algorithm.hashCode();
    return result;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + 4 /* encoded length of shuffleId */
      + 4 /* encoded length of shuffleMergeId */
      + 4 /* encoded length of reduceId */
      + 4 /* encoded length of chunkId */
      + 8 /* encoded length of checksum */
      + Encoders.Strings.encodedLength(algorithm); /* encoded length of algorithm */
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    buf.writeInt(shuffleId);
    buf.writeInt(shuffleMergeId);
    buf.writeInt(reduceId);
    buf.writeInt(chunkId);
    buf.writeLong(checksum);
    Encoders.Strings.encode(buf, algorithm);
  }

  public static DiagnoseShuffleChunkCorruption decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    int shuffleMergeId = buf.readInt();
    int reduceId = buf.readInt();
    int chunkId = buf.readInt();
    long checksum = buf.readLong();
    String algorithm = Encoders.Strings.decode(buf);
    return new DiagnoseShuffleChunkCorruption(
      appId, shuffleId, shuffleMergeId, reduceId, chunkId, checksum, algorithm);
  }
}
