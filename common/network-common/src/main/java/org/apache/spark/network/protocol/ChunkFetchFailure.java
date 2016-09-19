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

/**
 * Response to {@link ChunkFetchRequest} when there is an error fetching the chunk.
 */
public final class ChunkFetchFailure extends AbstractMessage implements ResponseMessage {
  public final StreamChunkId streamChunkId;
  public final String errorString;

  public ChunkFetchFailure(StreamChunkId streamChunkId, String errorString) {
    this.streamChunkId = streamChunkId;
    this.errorString = errorString;
  }

  @Override
  public Type type() { return Type.ChunkFetchFailure; }

  @Override
  public long encodedLength() {
    return streamChunkId.encodedLength() + Encoders.Strings.encodedLength(errorString);
  }

  @Override
  public void encode(OutputStream out) throws IOException {
    streamChunkId.encode(out);
    Encoders.Strings.encode(out, errorString);
  }

  public static ChunkFetchFailure decode(InputStream in) throws IOException {
    StreamChunkId streamChunkId = StreamChunkId.decode(in);
    String errorString = Encoders.Strings.decode(in);
    return new ChunkFetchFailure(streamChunkId, errorString);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamChunkId, errorString);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchFailure) {
      ChunkFetchFailure o = (ChunkFetchFailure) other;
      return streamChunkId.equals(o.streamChunkId) && errorString.equals(o.errorString);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .add("errorString", errorString)
      .toString();
  }
}
