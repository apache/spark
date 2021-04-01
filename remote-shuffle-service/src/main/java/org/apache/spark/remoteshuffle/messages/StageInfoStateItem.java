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

import org.apache.spark.remoteshuffle.clients.ShuffleWriteConfig;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class StageInfoStateItem extends BaseMessage {
  private final AppShuffleId appShuffleId;
  private final int numPartitions;
  private final int fileStartIndex;
  private final ShuffleWriteConfig writeConfig;
  private final byte fileStatus;

  public StageInfoStateItem(AppShuffleId appShuffleId, int numPartitions, int fileStartIndex,
                            ShuffleWriteConfig writeConfig, byte fileStatus) {
    this.appShuffleId = appShuffleId;
    this.numPartitions = numPartitions;
    this.fileStartIndex = fileStartIndex;
    this.writeConfig = writeConfig;
    this.fileStatus = fileStatus;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_StageInfoStateItem;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppId());
    ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppAttempt());
    buf.writeInt(appShuffleId.getShuffleId());
    buf.writeInt(numPartitions);
    buf.writeInt(fileStartIndex);
    buf.writeShort(writeConfig.getNumSplits());
    buf.writeByte(fileStatus);
  }

  public static StageInfoStateItem deserialize(ByteBuf buf) {
    String appId = ByteBufUtils.readLengthAndString(buf);
    String appAttempt = ByteBufUtils.readLengthAndString(buf);
    int shuffleId = buf.readInt();
    int numPartitions = buf.readInt();
    int fileStartIndex = buf.readInt();
    short numSplits = buf.readShort();
    byte fileStatus = buf.readByte();
    return new StageInfoStateItem(new AppShuffleId(appId, appAttempt, shuffleId),
        numPartitions,
        fileStartIndex,
        new ShuffleWriteConfig(numSplits),
        fileStatus);
  }

  public AppShuffleId getAppShuffleId() {
    return appShuffleId;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getFileStartIndex() {
    return fileStartIndex;
  }

  public ShuffleWriteConfig getWriteConfig() {
    return writeConfig;
  }

  public byte getFileStatus() {
    return fileStatus;
  }

  @Override
  public String toString() {
    return "StageInfoStateItem{" +
        "appShuffleId=" + appShuffleId +
        ", numPartitions=" + numPartitions +
        ", fileStartIndex=" + fileStartIndex +
        ", writeConfig=" + writeConfig +
        ", fileStatus=" + fileStatus +
        '}';
  }
}
