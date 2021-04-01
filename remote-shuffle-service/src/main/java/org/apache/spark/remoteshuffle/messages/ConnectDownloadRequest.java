/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
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

import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/***
 * This request is for shuffle read to connect to shuffle server to download data.
 */
public class ConnectDownloadRequest extends BaseMessage {
  private final String user;
  private final String appId;
  private final String appAttempt;
  private final int shuffleId;
  private final int partitionId;
  private final List<Long> taskAttemptIds;

  public ConnectDownloadRequest(String user, AppShufflePartitionId appShufflePartitionId,
                                Collection<Long> taskAttemptIds) {
    this(user, appShufflePartitionId.getAppId(), appShufflePartitionId.getAppAttempt(),
        appShufflePartitionId.getShuffleId(), appShufflePartitionId.getPartitionId(),
        taskAttemptIds);
  }

  public ConnectDownloadRequest(String user, String appId, String appAttempt, int shuffleId,
                                int partitionId, Collection<Long> taskAttemptIds) {
    this.user = user;
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.taskAttemptIds = new ArrayList<>(taskAttemptIds);
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_ConnectDownloadRequest;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, user);
    ByteBufUtils.writeLengthAndString(buf, appId);
    ByteBufUtils.writeLengthAndString(buf, appAttempt);
    buf.writeInt(shuffleId);
    buf.writeInt(partitionId);
    buf.writeInt(taskAttemptIds.size());
    for (Long entry : taskAttemptIds) {
      buf.writeLong(entry);
    }
  }

  public static ConnectDownloadRequest deserialize(ByteBuf buf) {
    String user = ByteBufUtils.readLengthAndString(buf);
    String appId = ByteBufUtils.readLengthAndString(buf);
    String appAttempt = ByteBufUtils.readLengthAndString(buf);
    int shuffleId = buf.readInt();
    int partitionId = buf.readInt();
    int numTaskAttemptIds = buf.readInt();
    List<Long> taskAttemptIds = new ArrayList<>(numTaskAttemptIds);
    for (int i = 0; i < numTaskAttemptIds; i++) {
      long taskAttemptId = buf.readLong();
      taskAttemptIds.add(taskAttemptId);
    }
    return new ConnectDownloadRequest(user, appId, appAttempt, shuffleId, partitionId,
        taskAttemptIds);
  }

  public String getUser() {
    return user;
  }

  public String getAppId() {
    return appId;
  }

  public String getAppAttempt() {
    return appAttempt;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public List<Long> getTaskAttemptIds() {
    return taskAttemptIds;
  }

  @Override
  public String toString() {
    return "ConnectDownloadRequest{" +
        "user='" + user + '\'' +
        ", appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", shuffleId=" + shuffleId +
        ", partitionId=" + partitionId +
        ", taskAttemptIds=" + taskAttemptIds +
        '}';
  }
}
