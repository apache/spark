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

package org.apache.spark.remoteshuffle.clients;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/***
 * This class contains value for a shuffle task data block, and is used only on the client side.
 * value could be null.
 */
public class TaskDataBlock {
  private final byte[] payload;

  private long taskAttemptId;

  public TaskDataBlock(byte[] payload, long taskAttemptId) {
    this.payload = payload;
    this.taskAttemptId = taskAttemptId;
  }

  @Nullable
  public byte[] getPayload() {
    return payload;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public long totalBytes() {
    long bytes = 0L;
    if (payload != null) {
      bytes += payload.length;
    }
    return bytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskDataBlock that = (TaskDataBlock) o;
    return taskAttemptId == that.taskAttemptId &&
        Arrays.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(taskAttemptId);
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }

  @Override
  public String toString() {
    String payloadStr = payload == null ? "null" : payload.length + " bytes";
    return "TaskDataBlock{" +
        "taskAttemptId=" + taskAttemptId +
        ", payload=" + payloadStr +
        '}';
  }
}
