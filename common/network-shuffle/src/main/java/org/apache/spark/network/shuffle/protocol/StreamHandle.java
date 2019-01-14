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
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * Identifier for a fixed number of chunks to read from a stream created by an "open blocks"
 * message. This is used by {@link org.apache.spark.network.shuffle.OneForOneBlockFetcher}.
 */
public class StreamHandle extends BlockTransferMessage {
  public final long streamId;
  public final int numChunks;
  public final int[] chunkSizes;

  private static final int[] EMPTY_CHUNK_SIZES_ARRAY = new int[0];

  // This is only used in tests.
  public StreamHandle(long streamId, int numChunks) {
    this(streamId, numChunks, EMPTY_CHUNK_SIZES_ARRAY);
  }

  public StreamHandle(long streamId, int numChunks, int[] chunkSizes) {
    this.streamId = streamId;
    this.numChunks = numChunks;
    this.chunkSizes = chunkSizes;
  }

  @Override
  protected Type type() { return Type.STREAM_HANDLE; }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, numChunks) * 41 + Arrays.hashCode(chunkSizes);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("numChunks", numChunks)
      .add("chunkSizes", Arrays.toString(chunkSizes))
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof StreamHandle) {
      StreamHandle o = (StreamHandle) other;
      return Objects.equal(streamId, o.streamId)
        && Objects.equal(numChunks, o.numChunks)
        && Arrays.equals(chunkSizes, o.chunkSizes);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return 8 + 4 + Encoders.IntArrays.encodedLength(chunkSizes);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
    buf.writeInt(numChunks);
    Encoders.IntArrays.encode(buf, chunkSizes);
  }

  public static StreamHandle decode(ByteBuf buf) {
    long streamId = buf.readLong();
    int numChunks = buf.readInt();
    int[] chunkSizes = EMPTY_CHUNK_SIZES_ARRAY;
    if (buf.readableBytes() != 0) {
      chunkSizes = Encoders.IntArrays.decode(buf);
    }
    return new StreamHandle(streamId, numChunks, chunkSizes);
  }
}
