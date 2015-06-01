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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import sun.nio.ch.DirectBuffer;

/**
 * A {@link org.apache.spark.network.buffer.LargeByteBuffer} which may contain multiple
 * {@link java.nio.ByteBuffer}s.  In order to support <code>asByteBuffer</code>, all
 * of the underlying ByteBuffers must have size equal to
 * {@link org.apache.spark.network.buffer.LargeByteBufferHelper#MAX_CHUNK_SIZE} (except that last
 * one).  The underlying ByteBuffers may be on-heap, direct, or memory-mapped.
 */
public class WrappedLargeByteBuffer implements LargeByteBuffer {

  @VisibleForTesting
  final ByteBuffer[] underlying;

  private final long size;
  /**
   * each sub-ByteBuffer (except for the last one) must be exactly this size.  Note that this
   * class *really* expects this to be LargeByteBufferHelper.MAX_CHUNK_SIZE.  The only reason it isn't
   * is so that we can do tests without creating ginormous buffers.  Public methods force it to
   * be LargeByteBufferHelper.MAX_CHUNK_SIZE
   */
  private final int subBufferSize;
  private long _pos;
  @VisibleForTesting
  int currentBufferIdx;
  @VisibleForTesting
  ByteBuffer currentBuffer;

  /**
   * Construct a WrappedLargeByteBuffer from the given ByteBuffers.  Each of the ByteBuffers must
   * have size equal to {@link org.apache.spark.network.buffer.LargeByteBufferHelper#MAX_CHUNK_SIZE}
   * except for the final one.  The buffers are <code>duplicate</code>d, so the position of the
   * given buffers and the returned buffer will be independent, though the underlying data will be
   * shared.
   * <p/>
   * The <code>position</code> of the returned buffer is determined by the position of the given
   * buffers. TODO
   */
  public WrappedLargeByteBuffer(ByteBuffer[] underlying) {
    this(underlying, LargeByteBufferHelper.MAX_CHUNK_SIZE);
  }

  /**
   * you do **not** want to call this version.  It leads to a buffer which doesn't properly
   * support {@link #asByteBuffer}.  The only reason it exists is to we can have tests which
   * don't require 2GB of memory
   *
   * @param underlying
   * @param subBufferSize
   */
  @VisibleForTesting
  WrappedLargeByteBuffer(ByteBuffer[] underlying, int subBufferSize) {
    if (underlying.length == 0) {
      throw new IllegalArgumentException("must wrap at least one ByteBuffer");
    }
    this.underlying = underlying;
    this.subBufferSize = subBufferSize;
    long sum = 0L;
    boolean startFound = false;
    long initialPosition = -1;

    // figure out the position in this LargeByteBuffer, by looking at the positions of the each
    // of the given ByteBuffers.  The ByteBuffers need to have positions that are consistent
    // with each other.  Eg., say we have 5 ByteBuffers, and the position is somewhere in the
    // middle of ByteBuffer 2.  Then ByteBuffers 0 & 1 must have position == capacity,
    // and ByteBuffers 3 & 4 must have position == 0

    for (int i = 0; i < underlying.length; i++) {
      ByteBuffer b = underlying[i];
      if (i != underlying.length -1 && b.capacity() != subBufferSize) {
        throw new IllegalArgumentException("All buffers, except for the final one, must have " +
          "size = " + subBufferSize);
      }
      if (startFound) {
        if (b.position() != 0) {
          throw new IllegalArgumentException("ByteBuffers have inconsistent positions");
        }
      } else if (b.position() != b.capacity()) {
        startFound = true;
        initialPosition = sum + b.position();
      }
      sum += b.capacity();
    }
    _pos = initialPosition;
    currentBufferIdx = 0;
    currentBuffer = underlying[0];
    size = sum;
  }

  @Override
  public void get(byte[] dest, int offset, int length) {
    if (length > remaining()) {
      throw new BufferUnderflowException();
    }
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
  public LargeByteBuffer rewind() {
    if (currentBuffer != null) {
      currentBuffer.rewind();
    }
    while (currentBufferIdx > 0) {
      currentBufferIdx -= 1;
      currentBuffer = underlying[currentBufferIdx];
      currentBuffer.rewind();
    }
    _pos = 0;
    return this;
  }

  @Override
  public WrappedLargeByteBuffer deepCopy() {
    ByteBuffer[] dataCopy = new ByteBuffer[underlying.length];
    for (int i = 0; i < underlying.length; i++) {
      ByteBuffer b = underlying[i];
      dataCopy[i] = ByteBuffer.allocate(b.capacity());
      int originalPosition = b.position();
      b.rewind();
      dataCopy[i].put(b);
      dataCopy[i].position(0);
      b.position(originalPosition);
    }
    return new WrappedLargeByteBuffer(dataCopy, subBufferSize);
  }

  @Override
  public byte get() {
    if (remaining() < 1L) {
      throw new BufferUnderflowException();
    }
    byte r = currentBuffer.get();
    _pos += 1;
    updateCurrentBuffer();
    return r;
  }

  private void updateCurrentBuffer() {
    while (currentBuffer != null && !currentBuffer.hasRemaining()) {
      currentBufferIdx += 1;
      currentBuffer = currentBufferIdx < underlying.length ? underlying[currentBufferIdx] : null;
    }
  }

  @Override
  public long position() {
    return _pos;
  }

  @Override
  public long skip(long n) {
    if (n < 0) {
      final long moveTotal = Math.min(-n, _pos);
      long toMove = moveTotal;
      // move backwards and update the position of every buffer as we go
      if (currentBuffer != null) {
        currentBufferIdx += 1;
      }
      while (toMove > 0) {
        currentBufferIdx -= 1;
        currentBuffer = underlying[currentBufferIdx];
        int thisMove = (int) Math.min(toMove, currentBuffer.position());
        currentBuffer.position(currentBuffer.position() - thisMove);
        toMove -= thisMove;
      }
      _pos -= moveTotal;
      return -moveTotal;
    } else if (n > 0) {
      final long moveTotal = Math.min(n, remaining());
      long toMove = moveTotal;
      // move forwards and update the position of every buffer as we go
      currentBufferIdx -= 1;
      while (toMove > 0) {
        currentBufferIdx += 1;
        currentBuffer = underlying[currentBufferIdx];
        int thisMove = (int) Math.min(toMove, currentBuffer.remaining());
        currentBuffer.position(currentBuffer.position() + thisMove);
        toMove -= thisMove;
      }
      _pos += moveTotal;
      return moveTotal;
    } else {
      return 0;
    }
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
    return new WrappedLargeByteBuffer(duplicates, subBufferSize);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0L;
    for (; currentBufferIdx < underlying.length; currentBufferIdx++) {
      currentBuffer = underlying[currentBufferIdx];
      written += currentBuffer.remaining();
      while (currentBuffer.hasRemaining())
        channel.write(currentBuffer);
    }
    _pos = size();
    return written;
  }

  @Override
  public ByteBuffer asByteBuffer() throws BufferTooLargeException {
    if (underlying.length == 1) {
      ByteBuffer b = underlying[0].duplicate();
      b.rewind();
      return b;
    } else {
      // NOTE: if subBufferSize != LargeByteBufferHelper.MAX_CAPACITY, in theory
      // we could copy the data into a new buffer.  But we don't want to do any copying.
      // The only reason we allow smaller subBufferSize is so that we can have tests which
      // don't require 2GB of memory
      throw new BufferTooLargeException(size(), underlying[0].capacity());
    }
  }

  @VisibleForTesting
  List<ByteBuffer> nioBuffers() {
    return Arrays.asList(underlying);
  }

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  private static void dispose(ByteBuffer buffer) {
    if (buffer != null && buffer instanceof DirectBuffer) {
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
