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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Objects;

import org.apache.spark.network.buffer.InputStreamManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;


/**
 * Response to {@link ChunkFetchRequest} when a chunk exists and has been successfully fetched.
 *
 * Note that the server-side encoding of this messages does NOT include the buffer itself, as this
 * may be written by Netty in a more efficient manner (i.e., zero-copy write).
 * Similarly, the client-side decoding will reuse the Netty ByteBuf as the buffer.
 */
public final class ChunkFetchSuccess extends AbstractResponseMessage {
  public final StreamChunkId streamChunkId;
  public final long byteCount;

  public ChunkFetchSuccess(StreamChunkId streamChunkId, ManagedBuffer buffer) {
    this(streamChunkId, buffer.size(), buffer);
  }

  public ChunkFetchSuccess(StreamChunkId streamChunkId, long byteCount, ManagedBuffer buffer) {
    super(buffer, true);
    this.streamChunkId = streamChunkId;
    this.byteCount = byteCount;
  }

  @Override
  public Type type() { return Type.ChunkFetchSuccess; }

  @Override
  public long encodedLength() {
    return streamChunkId.encodedLength() + 8;
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(OutputStream out) throws IOException {
    streamChunkId.encode(out);
    Encoders.Longs.encode(out, body().size());
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new ChunkFetchFailure(streamChunkId, error);
  }

  /** Decoding uses the given ByteBuf as our data, and will retain() it. */
  public static ChunkFetchSuccess decode(InputStream in) throws IOException {
    StreamChunkId streamChunkId = StreamChunkId.decode(in);
    long byteCount =  Encoders.Longs.decode(in);
    ManagedBuffer managedBuf = new InputStreamManagedBuffer(in, byteCount);
    return new ChunkFetchSuccess(streamChunkId, byteCount, managedBuf);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamChunkId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchSuccess) {
      ChunkFetchSuccess o = (ChunkFetchSuccess) other;
      return streamChunkId.equals(o.streamChunkId) && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .add("buffer", body())
      .toString();
  }
}
