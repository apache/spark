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
package org.apache.spark.network.buffer.netty;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.apache.spark.network.buffer.ChunkedByteBuffer;
import org.apache.spark.network.buffer.ChunkedByteBufferOutputStream;
import org.apache.spark.network.buffer.ChunkedByteBufferUtil;

public class ChunkedByteBuffOutputStreamImpl extends ChunkedByteBufferOutputStream {

  private final ByteBufAllocator alloc;
  /**
   * Next position to write in the last chunk.
   * <p>
   * If this equals chunkSize, it means for next write we need to allocate a new chunk.
   * This can also never be 0.
   */
  private int position;
  private ArrayList<ByteBuf> chunks = new ArrayList<>();
  private ByteBuf curChunk = null;
  /**
   * Index of the last chunk. Starting with -1 when the chunks array is empty.
   */
  private int lastChunkIndex = -1;
  private boolean toChunkedByteBufferWasCalled = false;
  private long _size = 0;

  /**
   * An OutputStream that writes to fixed-size chunks of byte arrays.
   *
   * @param chunkSize size of each chunk, in bytes.
   */
  public ChunkedByteBuffOutputStreamImpl(int chunkSize, boolean isDirect, ByteBufAllocator allocator) {
    super(chunkSize, isDirect);
    this.alloc = allocator;
    this.position = chunkSize;
  }

  public ChunkedByteBuffOutputStreamImpl(int chunkSize, boolean isDirect) {
    this(chunkSize, isDirect, ChunkedByteBufImpl.DEFAULT);
  }

  public long size() {
    return _size;
  }

  public void write(int b) throws IOException {
    allocateNewChunkIfNeeded();
    curChunk.writeByte(b);
    position += 1;
    _size += 1;
  }

  public void write(byte[] bytes, int off, int len) throws IOException {
    int written = 0;
    while (written < len) {
      allocateNewChunkIfNeeded();
      int thisBatch = Math.min(chunkSize - position, len - written);
      Preconditions.checkState(thisBatch > 0);
      int oldCapacity = curChunk.capacity();
      curChunk.writeBytes(bytes, off + written, thisBatch);
      written += thisBatch;
      position += thisBatch;
      Preconditions.checkState(oldCapacity == curChunk.capacity());
      Preconditions.checkState(chunkSize == curChunk.capacity());
    }
    _size += len;
  }

  private void allocateNewChunkIfNeeded() {
    Preconditions.checkArgument(!toChunkedByteBufferWasCalled,
        "cannot write after toChunkedByteBuffer() is called");
    if (position == chunkSize) {
      if (curChunk != null) chunks.add(curChunk);
      curChunk = allocate(chunkSize);
      Preconditions.checkState(curChunk.writerIndex() == 0);
      lastChunkIndex += 1;
      position = 0;
    }
  }

  private ByteBuf allocate(int len) {
    return isDirect ? alloc.directBuffer(len, len) : alloc.heapBuffer(len, len);
  }

  public ChunkedByteBuffer toChunkedByteBuffer() {
    Preconditions.checkArgument(!toChunkedByteBufferWasCalled,
        "toChunkedByteBuffer() can only be called once");
    toChunkedByteBufferWasCalled = true;
    if (lastChunkIndex == -1) {
      return ChunkedByteBufferUtil.wrap();
    } else {
      // Copy the first n-1 chunks to the output, and then create an array that fits the last chunk.
      // An alternative would have been returning an array of ByteBuffers, with the last buffer
      // bounded to only the last chunk's position. However, given our use case in Spark (to put
      // the chunks in block manager), only limiting the view bound of the buffer would still
      // require the block manager to store the whole chunk.
      ByteBuf[] ret = new ByteBuf[lastChunkIndex + 1];
      for (int i = 0; i < lastChunkIndex; i++) {
        ret[i] = chunks.get(i);
      }

      if (position == chunkSize) {
        ret[lastChunkIndex] = curChunk;
      } else {
        ret[lastChunkIndex] = allocate(position);
        Preconditions.checkState(position == curChunk.readableBytes());
        ret[lastChunkIndex].writeBytes(curChunk);
        curChunk.release();
      }
      return ChunkedByteBufferUtil.wrap(ret);
    }
  }
}
