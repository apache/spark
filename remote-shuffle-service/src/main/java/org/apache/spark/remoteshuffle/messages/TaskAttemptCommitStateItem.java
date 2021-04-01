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

import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.PartitionFilePathAndLength;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TaskAttemptCommitStateItem extends BaseMessage {
  private final AppShuffleId appShuffleId;
  private final Collection<Long> mapTaskAttemptIds;
  private final Collection<PartitionFilePathAndLength> partitionFilePathAndLengths;

  public TaskAttemptCommitStateItem(AppShuffleId appShuffleId, Collection<Long> mapTaskAttemptIds,
                                    Collection<PartitionFilePathAndLength> partitionFilePathAndLengths) {
    this.appShuffleId = appShuffleId;
    this.mapTaskAttemptIds = Collections.unmodifiableCollection(mapTaskAttemptIds);
    this.partitionFilePathAndLengths =
        Collections.unmodifiableCollection(partitionFilePathAndLengths);
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_TaskAttemptCommitStateItem;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppId());
    ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppAttempt());
    buf.writeInt(appShuffleId.getShuffleId());
    buf.writeInt(mapTaskAttemptIds.size());
    for (Long entry : mapTaskAttemptIds) {
      buf.writeLong(entry);
    }
    buf.writeInt(partitionFilePathAndLengths.size());
    for (PartitionFilePathAndLength entry : partitionFilePathAndLengths) {
      buf.writeInt(entry.getPartition());
      ByteBufUtils.writeLengthAndString(buf, entry.getPath());
      buf.writeLong(entry.getLength());
    }
  }

  public static TaskAttemptCommitStateItem deserialize(ByteBuf buf) {
    String appId = ByteBufUtils.readLengthAndString(buf);
    String appAttempt = ByteBufUtils.readLengthAndString(buf);
    int shuffleId = buf.readInt();
    int count = buf.readInt();
    List<Long> mapTaskAttemptIdList = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      long taskAttemptId = buf.readLong();
      mapTaskAttemptIdList.add(taskAttemptId);
    }
    count = buf.readInt();
    List<PartitionFilePathAndLength> partitionFilePathAndLengthList = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      int partition = buf.readInt();
      String path = ByteBufUtils.readLengthAndString(buf);
      long length = buf.readLong();
      partitionFilePathAndLengthList.add(new PartitionFilePathAndLength(partition, path, length));
    }
    return new TaskAttemptCommitStateItem(new AppShuffleId(appId, appAttempt, shuffleId),
        mapTaskAttemptIdList, partitionFilePathAndLengthList);
  }

  public AppShuffleId getAppShuffleId() {
    return appShuffleId;
  }

  public Collection<Long> getMapTaskAttemptIds() {
    return mapTaskAttemptIds;
  }

  public Collection<PartitionFilePathAndLength> getPartitionFilePathAndLengths() {
    return partitionFilePathAndLengths;
  }

  @Override
  public String toString() {
    return "TaskAttemptCommitStateItem{" +
        "appShuffleId=" + appShuffleId +
        ", mapTaskAttemptIds=" + StringUtils.join(mapTaskAttemptIds, ',') +
        ", partitionFilePathAndLengths=" + StringUtils.join(partitionFilePathAndLengths, ',') +
        '}';
  }
}
