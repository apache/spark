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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.google.common.base.Preconditions;

public class ChunkedByteBufferOutputStream extends OutputStream {

  private final int chunkSize;
  private final Allocator allocator;
  /**
   * Next position to write in the last chunk.
   *
   * If this equals chunkSize, it means for next write we need to allocate a new chunk.
   * This can also never be 0.
   */
  private int position;

  private ArrayList<ByteBuffer> chunks = new ArrayList<ByteBuffer>();
  /** Index of the last chunk. Starting with -1 when the chunks array is empty. */
  private int lastChunkIndex = -1;
  private boolean toChunkedByteBufferWasCalled = false;
  private long _size = 0;

  /**
   * An OutputStream that writes to fixed-size chunks of byte arrays.
   *
   * @param chunkSize size of each chunk, in bytes.
   */
  public ChunkedByteBufferOutputStream(int chunkSize, Allocator allocator) {
    this.chunkSize = chunkSize;
    this.allocator = allocator;
    this.position = chunkSize;
  }

  public ChunkedByteBufferOutputStream(int chunkSize) {
    this(chunkSize, new Allocator() {
      public ByteBuffer allocate(int len) {
        return ByteBuffer.allocate(len);
      }
    });
  }

  public long size() {
    return _size;
  }

  public void write(int b) throws IOException {
    allocateNewChunkIfNeeded();
    chunks.get(lastChunkIndex).put((byte) b);
    position += 1;
    _size += 1;
  }

  public void write(byte[] bytes, int off, int len) throws IOException {
    int written = 0;
    while (written < len) {
      allocateNewChunkIfNeeded();
      int thisBatch = Math.min(chunkSize - position, len - written);
      chunks.get(lastChunkIndex).put(bytes, written + off, thisBatch);
      written += thisBatch;
      position += thisBatch;
    }
    _size += len;
  }

  private void allocateNewChunkIfNeeded() {
    Preconditions.checkArgument(!toChunkedByteBufferWasCalled,
        "cannot write after toChunkedByteBuffer() is called");
    if (position == chunkSize) {
      chunks.add(allocator.allocate(chunkSize));
      lastChunkIndex += 1;
      position = 0;
    }
  }

  public ChunkedByteBuffer toChunkedByteBuffer() {
    Preconditions.checkArgument(!toChunkedByteBufferWasCalled,
        "toChunkedByteBuffer() can only be called once");
    toChunkedByteBufferWasCalled = true;
    if (lastChunkIndex == -1) {
      return new ChunkedByteBuffer(new ByteBuffer[0]);
    } else {
      // Copy the first n-1 chunks to the output, and then create an array that fits the last chunk.
      // An alternative would have been returning an array of ByteBuffers, with the last buffer
      // bounded to only the last chunk's position. However, given our use case in Spark (to put
      // the chunks in block manager), only limiting the view bound of the buffer would still
      // require the block manager to store the whole chunk.
      ByteBuffer[] ret = new ByteBuffer[chunks.size()];
      for (int i = 0; i < chunks.size() - 1; i++) {
        ret[i] = chunks.get(i);
        ret[i].flip();
      }

      if (position == chunkSize) {
        ret[lastChunkIndex] = chunks.get(lastChunkIndex);
        ret[lastChunkIndex].flip();
      } else {
        ret[lastChunkIndex] = allocator.allocate(position);
        chunks.get(lastChunkIndex).flip();
        ret[lastChunkIndex].put(chunks.get(lastChunkIndex));
        ret[lastChunkIndex].flip();
        ChunkedByteBuffer.dispose(chunks.get(lastChunkIndex));
      }
      return new ChunkedByteBuffer(ret);
    }
  }
}
