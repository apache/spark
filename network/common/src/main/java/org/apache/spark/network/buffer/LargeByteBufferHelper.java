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
package org.apache.spark.network.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utils for creating {@link org.apache.spark.network.buffer.LargeByteBuffer}s, either from
 * pre-allocated byte arrays, ByteBuffers, or by memory mapping a file.
 */
public class LargeByteBufferHelper {

  // netty can't quite send msgs that are a full 2GB -- they need to be slightly smaller
  // not sure what the exact limit is, but 200 seems OK.
  /**
   * The maximum size of any ByteBuffer.
   * {@link org.apache.spark.network.buffer.LargeByteBuffer#asByteBuffer} will never return a
   * ByteBuffer larger than this.  This is close to the max ByteBuffer size (2GB), minus a small
   * amount for message overhead.
   */
  public static final int MAX_CHUNK_SIZE = Integer.MAX_VALUE - 200;

  public static LargeByteBuffer asLargeByteBuffer(ByteBuffer buffer) {
    return new WrappedLargeByteBuffer(new ByteBuffer[] { buffer } );
  }

  public static LargeByteBuffer asLargeByteBuffer(byte[] bytes) {
    return asLargeByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static LargeByteBuffer allocate(long size) {
    return allocate(size, MAX_CHUNK_SIZE);
  }

  @VisibleForTesting
  static LargeByteBuffer allocate(long size, int maxChunk) {
    int chunksNeeded = (int) ((size + maxChunk - 1) / maxChunk);
    ByteBuffer[] chunks = new ByteBuffer[chunksNeeded];
    long remaining = size;
    for (int i = 0; i < chunksNeeded; i++) {
      int nextSize = (int) Math.min(remaining, maxChunk);
      ByteBuffer next = ByteBuffer.allocate(nextSize);
      remaining -= nextSize;
      chunks[i] = next;
    }
    if (remaining != 0) {
      throw new IllegalStateException("remaining = " + remaining);
    }
    return new WrappedLargeByteBuffer(chunks, maxChunk);
  }


  public static LargeByteBuffer mapFile(
      FileChannel channel,
      FileChannel.MapMode mode,
      long offset,
      long length
  ) throws IOException {
    int chunksNeeded = (int) ((length  - 1) / MAX_CHUNK_SIZE) + 1;
    ByteBuffer[] chunks = new ByteBuffer[chunksNeeded];
    long curPos = offset;
    long end = offset + length;
    for (int i = 0; i < chunksNeeded; i++) {
      long nextPos = Math.min(curPos + MAX_CHUNK_SIZE, end);
      chunks[i] = channel.map(mode, curPos, nextPos - curPos);
      curPos = nextPos;
    }
    return new WrappedLargeByteBuffer(chunks);
  }


}
