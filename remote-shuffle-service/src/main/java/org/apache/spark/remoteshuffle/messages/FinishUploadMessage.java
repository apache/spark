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

import io.netty.buffer.ByteBuf;

/***
 * Message sent by upload client to indicate the uploading is finished.
 */
public class FinishUploadMessage extends BaseMessage {

  public static final byte ACK_FLAG_NO_ACK = 0;
  public static final byte ACK_FLAG_HAS_ACK = 1;

  private long taskAttemptId;
  private long timestamp;
  private byte ackFlag;

  public FinishUploadMessage(long taskAttemptId, long timestamp, byte ackFlag) {
    this.taskAttemptId = taskAttemptId;
    this.timestamp = timestamp;
    this.ackFlag = ackFlag;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_FinishUploadMessage;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeLong(taskAttemptId);
    buf.writeLong(timestamp);
    buf.writeByte(ackFlag);
  }

  public static FinishUploadMessage deserialize(ByteBuf buf) {
    long taskAttemptId = buf.readLong();
    long timestamp = buf.readLong();
    byte ackFlag = buf.readByte();
    return new FinishUploadMessage(taskAttemptId, timestamp, ackFlag);
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public byte getAckFlag() {
    return ackFlag;
  }

  @Override
  public String toString() {
    return "FinishUploadMessage{" +
        "taskAttemptId=" + taskAttemptId +
        ",timestamp=" + timestamp +
        ",ackFlag=" + ackFlag +
        '}';
  }
}
