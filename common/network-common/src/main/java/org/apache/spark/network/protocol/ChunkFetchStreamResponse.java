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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Response to {@link ChunkFetchRequest} when its `fetchAsStream` flag is true and the stream has
 * been successfully opened.
 * <p>
 * Note the message itself does not contain the stream data. That is written separately by the
 * sender. The receiver is expected to set a temporary channel handler that will consume the
 * number of bytes this message says the stream has.
 */
public final class ChunkFetchStreamResponse extends AbstractResponseMessage {
  public final StreamChunkId streamChunkId;
  public final long byteCount;

  public ChunkFetchStreamResponse(
      StreamChunkId streamChunkId,
      long byteCount,
      ManagedBuffer buffer) {
    super(buffer, false);
    this.streamChunkId = streamChunkId;
    this.byteCount = byteCount;
  }

  @Override
  public Message.Type type() { return Type.ChunkFetchStreamResponse; }

  @Override
  public int encodedLength() {
    return streamChunkId.encodedLength() + 8;
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(ByteBuf buf) {
    streamChunkId.encode(buf);
    buf.writeLong(byteCount);
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new ChunkFetchFailure(streamChunkId, error);
  }

  public static ChunkFetchStreamResponse decode(ByteBuf buf) {
    StreamChunkId streamChunkId = StreamChunkId.decode(buf);
    long byteCount = buf.readLong();
    return new ChunkFetchStreamResponse(streamChunkId, byteCount, null);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamChunkId, byteCount);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchStreamResponse) {
      ChunkFetchStreamResponse o = (ChunkFetchStreamResponse) other;
      return streamChunkId.equals(o.streamChunkId) && byteCount == o.byteCount;
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .add("byteCount", byteCount)
      .add("body", body())
      .toString();
  }
}
