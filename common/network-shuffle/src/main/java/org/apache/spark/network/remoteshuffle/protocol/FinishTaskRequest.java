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

import java.nio.ByteBuffer;
import java.util.Objects;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.remoteshuffle.protocol.RemoteShuffleMessage.Type;

/** Request to finish writing data from a map task. Returns {@link FinishTaskResponse}. */
public class FinishTaskRequest extends RemoteShuffleMessage {
  public final long sessionId;
  public final long taskAttempt;

  public FinishTaskRequest(long sessionId, long taskAttempt) {
    this.sessionId = sessionId;
    this.taskAttempt = taskAttempt;
  }

  @Override
  protected Type type() { return Type.FINISH_TASK_REQUEST; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FinishTaskRequest that = (FinishTaskRequest) o;
    return sessionId == that.sessionId &&
        taskAttempt == that.taskAttempt;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sessionId, taskAttempt);
  }

  @Override
  public String toString() {
    return "FinishTaskRequest{" +
        "sessionId=" + sessionId +
        ", taskAttempt=" + taskAttempt +
        '}';
  }

  @Override
  public int encodedLength() {
    return Long.BYTES
      + Long.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(sessionId);
    buf.writeLong(taskAttempt);
  }

  public static FinishTaskRequest decode(ByteBuf buf) {
    long sessionId = buf.readLong();
    long taskAttempt = buf.readLong();
    return new FinishTaskRequest(sessionId, taskAttempt);
  }
}
