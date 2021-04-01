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

/***
 * This class wraps a chunk of data inside the shuffle file. The data (bytes) should
 * be written to shuffle file directly.
 */
public class ShuffleDataWrapper {
  private final int partitionId;
  private final long taskAttemptId;
  private final byte[] bytes;

  public ShuffleDataWrapper(int partitionId, long taskAttemptId, byte[] bytes) {
    if (bytes == null) {
      throw new NullPointerException("bytes");
    }

    this.partitionId = partitionId;
    this.taskAttemptId = taskAttemptId;
    this.bytes = bytes;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return "ShuffleDataWrapper{" +
        "partitionId=" + partitionId +
        ", taskAttemptId=" + taskAttemptId +
        ", bytes.length=" + bytes.length +
        '}';
  }
}
