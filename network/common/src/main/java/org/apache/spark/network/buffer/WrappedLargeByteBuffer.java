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

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

public class WrappedLargeByteBuffer implements LargeByteBuffer {

  //only public for tests for the moment ...
  public final ByteBuffer[] underlying;
  private final Long totalCapacity;
  private final long[] chunkOffsets;

  private long _pos;
  private int currentBufferIdx;
  private ByteBuffer currentBuffer;
  private long size;


  public WrappedLargeByteBuffer(ByteBuffer[] underlying) {
    this.underlying = underlying;
    long sum = 0l;
    chunkOffsets = new long[underlying.length];
    for (int i = 0; i < underlying.length; i++) {
      chunkOffsets[i] = sum;
      sum += underlying[i].capacity();
    }
    totalCapacity = sum;
    _pos = 0l;
    currentBufferIdx = 0;
    currentBuffer = underlying[0];
    size = totalCapacity;
  }

  @Override
  public void get(byte[] dest, int offset, int length) {
    int moved = 0;
    while (moved < length) {
      int toRead = Math.min(length - moved, currentBuffer.remaining());
      currentBuffer.get(dest, offset + moved, toRead);
      moved += toRead;
      updateCurrentBuffer();
    }
    _pos += moved;
  }

  @Override
  public byte get() {
    byte r = currentBuffer.get();
    _pos += 1;
    updateCurrentBuffer();
    return r;
  }

  private void updateCurrentBuffer() {
    //TODO fix end condition
    while (currentBuffer != null && !currentBuffer.hasRemaining()) {
      currentBufferIdx += 1;
      currentBuffer = currentBufferIdx < underlying.length ? underlying[currentBufferIdx] : null;
    }
  }

  @Override
  public LargeByteBuffer put(LargeByteBuffer bytes) {
    throw new RuntimeException("not yet implemented");
  }

  @Override
  public long position() {
    return _pos;
  }

  @Override
  public LargeByteBuffer position(long newPosition) {
    //XXX check range?
    _pos = newPosition;
    return this;
  }

  @Override
  public long remaining() {
    return size - _pos;
  }

  @Override
  public WrappedLargeByteBuffer duplicate() {
    ByteBuffer[] duplicates = new ByteBuffer[underlying.length];
    for (int i = 0; i < underlying.length; i++) {
      duplicates[i] = underlying[i].duplicate();
    }
    //we could also avoid initializing offsets here, if we cared ...
    return new WrappedLargeByteBuffer(duplicates);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0l;
    for (ByteBuffer buffer : underlying) {
      //TODO test this
      written += buffer.remaining();
      while (buffer.hasRemaining())
        channel.write(buffer);
    }
    return written;
  }

  @Override
  public ByteBuffer asByteBuffer() {
    return underlying[0];
  }

  @Override
  public List<ByteBuffer> nioBuffers() {
    return Arrays.asList(underlying);
  }

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  private static void dispose(ByteBuffer buffer) {
    if (buffer != null && buffer instanceof MappedByteBuffer) {
      DirectBuffer db = (DirectBuffer) buffer;
      if (db.cleaner() != null) {
        db.cleaner().clean();
      }
    }
  }

  @Override
  public void dispose() {
    for (ByteBuffer bb : underlying) {
      dispose(bb);
    }
  }

}
