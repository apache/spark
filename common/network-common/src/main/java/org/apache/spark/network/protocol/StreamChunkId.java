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
* Encapsulates a request for a particular chunk of a stream.
*/
public final class StreamChunkId implements Encodable {
  public final long streamId;
  public final int chunkIndex;

  public StreamChunkId(long streamId, int chunkIndex) {
    this.streamId = streamId;
    this.chunkIndex = chunkIndex;
  }

  @Override
  public long encodedLength() {
    return 8 + 4;
  }

  public void encode(OutputStream buffer) throws IOException {
    Encoders.Longs.encode(buffer, streamId);
    Encoders.Ints.encode(buffer, chunkIndex);
  }

  public static StreamChunkId decode(InputStream in) throws IOException {
    long streamId = Encoders.Longs.decode(in);
    int chunkIndex = Encoders.Ints.decode(in);
    return new StreamChunkId(streamId, chunkIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, chunkIndex);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamChunkId) {
      StreamChunkId o = (StreamChunkId) other;
      return streamId == o.streamId && chunkIndex == o.chunkIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("chunkIndex", chunkIndex)
      .toString();
  }
}
