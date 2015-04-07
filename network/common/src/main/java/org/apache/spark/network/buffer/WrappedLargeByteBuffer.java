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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

public class WrappedLargeByteBuffer implements LargeByteBuffer {

  //only public for tests
  public final ByteBuffer[] underlying;

  private final long size;
  private long _pos;
  private int currentBufferIdx;
  private ByteBuffer currentBuffer;


  public WrappedLargeByteBuffer(ByteBuffer[] underlying) {
    this(underlying, findExpectedInitialPosition(underlying));
  }

  private static long findExpectedInitialPosition(ByteBuffer[] bufs) {
    long sum = 0L;
    for (ByteBuffer b: bufs) {
      if (b.position() > 0) {
        // this could still lead to a mix of positions half-way through buffers that
        // would be inconsistent -- but we'll discover that in the constructor checks
        sum += b.position();
      } else {
        break;
      }
    }
    return sum;
  }

  private WrappedLargeByteBuffer(ByteBuffer[] underlying, long initialPosition) {
    this.underlying = underlying;
    long sum = 0L;
    for (int i = 0; i < underlying.length; i++) {
      ByteBuffer b = underlying[i];
      long nextSum = sum + b.capacity();
      int expectedPosition;
      if (nextSum < initialPosition) {
        expectedPosition = b.capacity();
      } else if (sum > initialPosition) {
        expectedPosition = 0;
      } else {
        expectedPosition = (int) (initialPosition - sum);
      }
      if (b.position() != expectedPosition) {
        throw new IllegalArgumentException("ByteBuffer[" + i + "]:" + b + " was expected to have" +
          " position = " + expectedPosition + " to be consistent with the overall " +
          "initialPosition = " + initialPosition);
      }
      sum = nextSum;
    }
    _pos = initialPosition;
    currentBufferIdx = 0;
    currentBuffer = underlying[0];
    size = sum;
  }

  @Override
  public void get(byte[] dest, int offset, int length) {
    if (length > remaining())
      throw new BufferUnderflowException();
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
    while (currentBufferIdx > 0) {
      if (currentBuffer != null) {
        currentBuffer.rewind();
      }
      currentBufferIdx -= 1;
      currentBuffer = underlying[currentBufferIdx];
    }
    currentBuffer.rewind();
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
      b.position(0);
      dataCopy[i].put(b);
      dataCopy[i].position(0);
      b.position(originalPosition);
    }
    return new WrappedLargeByteBuffer(dataCopy);
  }

  @Override
  public byte get() {
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
      // move backwards -- set the position to 0 of every buffer's we go back
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
      // move forwards-- set the position to the end of every buffer as we go forwards
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
    return new WrappedLargeByteBuffer(duplicates, _pos);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0l;
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
    if (underlying.length > 1) {
      throw new BufferTooLargeException(size());
    }
    return underlying[0];
  }

  // only needed for tests
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
