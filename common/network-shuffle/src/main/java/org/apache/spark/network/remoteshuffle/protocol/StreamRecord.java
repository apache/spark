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

package org.apache.spark.network.remoteshuffle.protocol;

import io.netty.buffer.ByteBuf;

import java.util.Objects;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.remoteshuffle.protocol.RemoteShuffleMessage.Type;

/** A shuffle record streamed to server */
public class StreamRecord extends RemoteShuffleMessage {
  public final long streamId;
  public final int partition;
  public final TaskAttemptRecord taskAttemptRecord;

  public StreamRecord(long streamId, int partition, TaskAttemptRecord taskAttemptRecord) {
    this.streamId = streamId;
    this.partition = partition;
    this.taskAttemptRecord = taskAttemptRecord;
  }

  @Override
  protected Type type() { return Type.STREAM_RECORD; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamRecord that = (StreamRecord) o;
    return streamId == that.streamId &&
        partition == that.partition &&
        Objects.equals(taskAttemptRecord, that.taskAttemptRecord);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamId, partition, taskAttemptRecord);
  }

  @Override
  public String toString() {
    return "StreamRecord{" +
        "streamId=" + streamId +
        ", partition=" + partition +
        ", taskAttemptRecord=" + taskAttemptRecord +
        '}';
  }

  @Override
  public int encodedLength() {
    return Long.BYTES
      + Integer.BYTES
      + taskAttemptRecord.encodedLength();
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
    buf.writeInt(partition);
    taskAttemptRecord.encode(buf);
  }

  public static StreamRecord decode(ByteBuf buf) {
    long streamId = buf.readLong();
    int partition = buf.readInt();
    TaskAttemptRecord taskAttemptRecord = TaskAttemptRecord.decode(buf);
    return new StreamRecord(streamId, partition, taskAttemptRecord);
  }
}
