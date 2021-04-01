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

import org.apache.spark.remoteshuffle.util.ByteBufUtils;

public class DataBlockHeader {
  public static int NUM_BYTES = Long.BYTES + Integer.BYTES;

  public static byte[] serializeToBytes(byte[] taskAttemptIdBytes, int length) {
    byte[] bytes = new byte[NUM_BYTES];
    System.arraycopy(taskAttemptIdBytes, 0, bytes, 0, Long.BYTES);
    ByteBufUtils.writeInt(bytes, Long.BYTES, length);
    return bytes;
  }

  public static DataBlockHeader deserializeFromBytes(byte[] bytes) {
    long taskAttemptId = ByteBufUtils.readLong(bytes, 0);
    int length = ByteBufUtils.readInt(bytes, Long.BYTES);
    return new DataBlockHeader(taskAttemptId, length);
  }

  private final long taskAttemptId;
  private final int length;

  public DataBlockHeader(long taskAttemptId, int length) {
    this.taskAttemptId = taskAttemptId;
    this.length = length;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public int getLength() {
    return length;
  }

  @Override
  public String toString() {
    return "DataBlockHeader{" +
        "taskAttemptId=" + taskAttemptId +
        ", length=" + length +
        '}';
  }
}
