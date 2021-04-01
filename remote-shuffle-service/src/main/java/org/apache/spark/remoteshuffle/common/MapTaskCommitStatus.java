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

package org.apache.spark.remoteshuffle.common;

import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;
import org.apache.spark.remoteshuffle.util.StringUtils;
import io.netty.buffer.ByteBuf;

import java.util.*;
import java.util.stream.Collectors;

public class MapTaskCommitStatus {
  public void serialize(ByteBuf buf) {
    buf.writeInt(getTaskAttemptIds().size());
    taskAttemptIds.forEach(taskId -> {
      buf.writeLong(taskId);
    });
  }

  public static MapTaskCommitStatus deserialize(ByteBuf buf) {
    int size = buf.readInt();

    Set<Long> ids = new HashSet<>();
    Map<Integer, Long> hashMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      long taskId = buf.readLong();
      ids.add(taskId);
    }

    return new MapTaskCommitStatus(ids);
  }

  // Last successful attempt ids
  private final Set<Long> taskAttemptIds;

  public MapTaskCommitStatus(Set<Long> taskAttemptIds) {
    this.taskAttemptIds = new HashSet<>(taskAttemptIds);
  }

  public Set<Long> getTaskAttemptIds() {
    return taskAttemptIds;
  }

  public boolean isPartitionDataAvailable(Collection<Long> fetchTaskAttemptIds) {
    // TODO need to verify fetchTaskAttemptIds non empty to make code safer
    if (fetchTaskAttemptIds.isEmpty()) {
      throw new RssInvalidDataException("fetchTaskAttemptIds cannot be empty");
    }

    return taskAttemptIds.containsAll(fetchTaskAttemptIds);
  }

  public String toShortString() {
    String str = String.format("(%s items)", taskAttemptIds.size());
    return "MapTaskCommitStatus{" +
        ", taskAttemptIds=" + str +
        '}';
  }

  @Override
  public String toString() {
    List<Long> sorted = taskAttemptIds.stream().sorted().collect(Collectors.toList());
    String str = StringUtils.toString4SortedNumberList(sorted);
    return "MapTaskCommitStatus{" +
        ", taskAttemptIds=" + str +
        '}';
  }
}
