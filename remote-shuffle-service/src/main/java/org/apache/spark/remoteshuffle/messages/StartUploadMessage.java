/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.messages;

import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class StartUploadMessage extends BaseMessage {

  private int shuffleId;
  private int mapId;
  private long attemptId;
  private int numMaps;
  private int numPartitions;
  private String fileCompressionCodec;
  private short numSplits;

  public StartUploadMessage(int shuffleId, int mapId, long attemptId, int numMaps,
                            int numPartitions, String fileCompressionCodec, short numSplits) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.numMaps = numMaps;
    this.numPartitions = numPartitions;
    this.fileCompressionCodec = fileCompressionCodec;
    this.numSplits = numSplits;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_StartUploadMessage;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(shuffleId);
    buf.writeInt(mapId);
    buf.writeLong(attemptId);
    buf.writeInt(numMaps);
    buf.writeInt(numPartitions);
    ByteBufUtils.writeLengthAndString(buf, fileCompressionCodec);
    buf.writeShort(numSplits);
  }

  public static StartUploadMessage deserialize(ByteBuf buf) {
    int shuffleId = buf.readInt();
    int mapId = buf.readInt();
    long attemptId = buf.readLong();
    int numMaps = buf.readInt();
    int numPartitions = buf.readInt();
    String fileCompressionCodec = ByteBufUtils.readLengthAndString(buf);
    short numSplits = buf.readShort();
    return new StartUploadMessage(shuffleId, mapId, attemptId, numMaps, numPartitions,
        fileCompressionCodec, numSplits);
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getMapId() {
    return mapId;
  }

  public long getAttemptId() {
    return attemptId;
  }

  public int getNumMaps() {
    return numMaps;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public String getFileCompressionCodec() {
    return fileCompressionCodec;
  }

  public short getNumSplits() {
    return numSplits;
  }

  @Override
  public String toString() {
    return "StartUploadMessage{" +
        "shuffleId=" + shuffleId +
        ", mapId=" + mapId +
        ", attemptId=" + attemptId +
        ", numMaps=" + numMaps +
        ", numPartitions=" + numPartitions +
        ", fileCompressionCodec='" + fileCompressionCodec + '\'' +
        ", numSplits=" + numSplits +
        '}';
  }
}
