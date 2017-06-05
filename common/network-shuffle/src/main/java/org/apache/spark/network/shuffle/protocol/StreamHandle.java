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

package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * Identifier for a stream created by an "open blocks" message.
 * This is used by {@link org.apache.spark.network.shuffle.OneForOneBlockFetcher}.
 */
public class StreamHandle extends BlockTransferMessage {
  public final long streamId;

  public StreamHandle(long streamId) {
    this.streamId = streamId;
  }

  @Override
  protected Type type() { return Type.STREAM_HANDLE; }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof StreamHandle) {
      StreamHandle o = (StreamHandle) other;
      return Objects.equal(streamId, o.streamId);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
  }

  public static StreamHandle decode(ByteBuf buf) {
    long streamId = buf.readLong();
    return new StreamHandle(streamId);
  }
}
