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

/**
 * Request to fetch a sequence of a single chunk of a stream. This will correspond to a single
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 */
public final class ChunkFetchRequest extends AbstractMessage implements RequestMessage {
  public final StreamChunkId streamChunkId;

  // Indicates if the client wants to fetch this chunk as a stream, to reduce memory consumption.
  // This field is newly added in Spark 3.0, and will be encoded in the message only when it's true.
  public final boolean fetchAsStream;

  public ChunkFetchRequest(StreamChunkId streamChunkId, boolean fetchAsStream) {
    this.streamChunkId = streamChunkId;
    this.fetchAsStream = fetchAsStream;
  }

  // This is only used in tests.
  public ChunkFetchRequest(StreamChunkId streamChunkId) {
    this.streamChunkId = streamChunkId;
    this.fetchAsStream = false;
  }

  @Override
  public Message.Type type() { return Type.ChunkFetchRequest; }

  @Override
  public int encodedLength() {
    return streamChunkId.encodedLength() + (fetchAsStream ? 1 : 0);
  }

  @Override
  public void encode(ByteBuf buf) {
    streamChunkId.encode(buf);
    if (fetchAsStream) {
      buf.writeBoolean(true);
    }
  }

  public static ChunkFetchRequest decode(ByteBuf buf) {
    StreamChunkId streamChunkId = StreamChunkId.decode(buf);
    boolean fetchAsStream;
    if (buf.readableBytes() >= 1) {
      // A sanity check. In `encode` we write true, so here we should read true.
      assert buf.readBoolean();
      fetchAsStream = true;
    } else {
      fetchAsStream = false;
    }
    return new ChunkFetchRequest(streamChunkId, fetchAsStream);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(streamChunkId, fetchAsStream);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchRequest) {
      ChunkFetchRequest o = (ChunkFetchRequest) other;
      return streamChunkId.equals(o.streamChunkId) && fetchAsStream == o.fetchAsStream;
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .add("fetchAsStream", fetchAsStream)
      .toString();
  }
}
