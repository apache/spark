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
import io.netty.buffer.Unpooled;
import org.apache.spark.network.protocol.Encodable;

import java.nio.ByteBuffer;
import java.util.Objects;

/** Task attempt id and record key/value data. The first 8 bytes contain task attempt id. */
public class TaskAttemptRecord implements Encodable {
  public final long taskAttempt;
  public final ByteBuffer key;
  public final ByteBuffer value;

  public TaskAttemptRecord(long taskAttempt, ByteBuffer key, ByteBuffer value) {
    this.taskAttempt = taskAttempt;
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskAttemptRecord that = (TaskAttemptRecord) o;
    return taskAttempt == that.taskAttempt &&
        Objects.equals(key, that.key) &&
        Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskAttempt, key, value);
  }

  @Override
  public String toString() {
    return "StreamRecord{" +
        "taskAttempt=" + taskAttempt +
        ", key=" + (key == null ? "null" : (key.remaining() + " bytes")) +
        ", value=" + (value == null ? "null" : (value.remaining() + " bytes")) +
        '}';
  }

  @Override
  public int encodedLength() {
    return Long.BYTES
      + Integer.BYTES
      + (key == null ? 0 : key.remaining())
      + Integer.BYTES
      + (value == null ? 0 : value.remaining());
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(taskAttempt);
    if (key == null) {
      buf.writeInt(-1);
    } else {
      buf.writeInt(key.remaining());
      buf.writeBytes(key);
    }
    if (value == null) {
      buf.writeInt(-1);
    } else {
      buf.writeInt(value.remaining());
      buf.writeBytes(value);
    }
  }

  public ByteBuffer toByteBuffer() {
    int len = encodedLength();
    ByteBuf buf = Unpooled.buffer(Integer.BYTES + len);
    buf.writeInt(len);
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.nioBuffer();
  }

  public static TaskAttemptRecord decode(ByteBuf buf) {
    long taskAttempt = buf.readLong();
    int keyLen = buf.readInt();
    ByteBuffer key = null;
    if (keyLen >= 0) {
      key = ByteBuffer.allocate(keyLen);
      buf.readBytes(key);
      key.flip();
    }
    int valueLen = buf.readInt();
    ByteBuffer value = null;
    if (valueLen >= 0) {
      value = ByteBuffer.allocate(valueLen);
      buf.readBytes(value);
      value.flip();
    }
    return new TaskAttemptRecord(taskAttempt, key, value);
  }

  public static long getTaskAttemptId(ByteBuffer data) {
    data.mark();
    long taskAttemptId = data.getLong();
    data.reset();
    return taskAttemptId;
  }
}
