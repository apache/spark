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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

import sun.nio.ch.DirectBuffer;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.ByteArrayWritableChannel;

public class ChunkedByteBuffer implements Externalizable {
  private static final Logger logger = LoggerFactory.getLogger(ChunkedByteBuffer.class);
  private static final int BUF_SIZE = 0x1000; // 4K
  private static final ByteBuffer[] emptyChunks = new ByteBuffer[0];
  private ByteBuffer[] chunks = null;
  private boolean disposed = false;

  // For deserialization only
  public ChunkedByteBuffer() {
    this(emptyChunks);
  }

  /**
   * Read-only byte buffer which is physically stored as multiple chunks rather than a single
   * contiguous array.
   *
   * @param chunks an array of [[ByteBuffer]]s. Each buffer in this array must have position == 0.
   *               Ownership of these buffers is transferred to the ChunkedByteBuffer, so if these
   *               buffers may also be used elsewhere then the caller is responsible for copying
   *               them as needed.
   */
  public ChunkedByteBuffer(ByteBuffer[] chunks) {
    this.chunks = chunks;
    Preconditions.checkArgument(chunks != null, "chunks must not be null");
    for (int i = 0; i < chunks.length; i++) {
      ByteBuffer bytes = chunks[i];
      // Preconditions.checkArgument(bytes.remaining() > 0, "chunks must be non-empty");
    }
  }

  public ChunkedByteBuffer(ByteBuffer chunk) {
    this.chunks = new ByteBuffer[1];
    this.chunks[0] = chunk;
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeBoolean(disposed);
    out.writeInt(chunks.length);
    byte[] buf = null;
    for (int i = 0; i < chunks.length; i++) {
      ByteBuffer buffer = chunks[i].duplicate();
      out.writeInt(buffer.remaining());
      if (buffer.hasArray()) {
        out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
      } else {
        if (buf == null) buf = new byte[BUF_SIZE];
        while (buffer.hasRemaining()) {
          int r = Math.min(BUF_SIZE, buffer.remaining());
          buffer.get(buf, 0, r);
          out.write(buf, 0, r);
        }
      }
    }
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.disposed = in.readBoolean();
    ByteBuffer[] buffers = new ByteBuffer[in.readInt()];
    for (int i = 0; i < buffers.length; i++) {
      int length = in.readInt();
      byte[] buffer = new byte[length];
      in.readFully(buffer);
      buffers[i] = ByteBuffer.wrap(buffer);
    }
    this.chunks = buffers;
  }

  /**
   * This size of this buffer, in bytes.
   */
  public long size() {
    if (chunks == null) return 0L;
    int i = 0;
    long sum = 0L;
    while (i < chunks.length) {
      sum += chunks[i].remaining();
      i++;
    }
    return sum;
  }

  /**
   * Write this buffer to a channel.
   */
  public void writeFully(WritableByteChannel channel) throws IOException {
    for (int i = 0; i < chunks.length; i++) {
      ByteBuffer bytes = chunks[i].duplicate();
      while (bytes.remaining() > 0) {
        channel.write(bytes);
      }
    }
  }

  /**
   * Wrap this buffer to view it as a Netty ByteBuf.
   */
  public ByteBuf toNetty() {
    return Unpooled.wrappedBuffer(getChunks());
  }

  /**
   * Copy this buffer into a new byte array.
   *
   * @throws UnsupportedOperationException if this buffer's size exceeds the maximum array size.
   */
  public byte[] toArray() throws IOException, UnsupportedOperationException {
    long len = size();
    if (len >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
          "cannot call toArray because buffer size (" + len +
              " bytes) exceeds maximum array size");
    }
    ByteArrayWritableChannel byteChannel = new ByteArrayWritableChannel((int) len);
    writeFully(byteChannel);
    byteChannel.close();
    return byteChannel.getData();
  }

  /**
   * Copy this buffer into a new ByteBuffer.
   *
   * @throws UnsupportedOperationException if this buffer's size exceeds the max ByteBuffer size.
   */
  public ByteBuffer toByteBuffer() throws IOException, UnsupportedOperationException {
    if (chunks.length == 1) {
      return chunks[0].duplicate();
    } else {
      return ByteBuffer.wrap(this.toArray());
    }
  }

  public ChunkedByteBufferInputStream toInputStream() {
    return toInputStream(false);
  }

  /**
   * Creates an input stream to read data from this ChunkedByteBuffer.
   *
   * @param dispose if true, [[dispose()]] will be called at the end of the stream
   *                in order to close any memory-mapped files which back this buffer.
   */
  public ChunkedByteBufferInputStream toInputStream(boolean dispose) {
    return new ChunkedByteBufferInputStream(this, dispose);
  }

  /**
   * Make a copy of this ChunkedByteBuffer, copying all of the backing data into new buffers.
   * The new buffer will share no resources with the original buffer.
   *
   * @param allocator a method for allocating byte buffers
   */
  public ChunkedByteBuffer copy(Allocator allocator) {
    ByteBuffer[] copiedChunks = new ByteBuffer[chunks.length];
    for (int i = 0; i < chunks.length; i++) {
      ByteBuffer chunk = chunks[i].duplicate();
      ByteBuffer newChunk = allocator.allocate(chunk.remaining());
      newChunk.put(chunk);
      newChunk.flip();
      copiedChunks[i] = newChunk;
    }
    return new ChunkedByteBuffer(copiedChunks);
  }

  /**
   * Get duplicates of the ByteBuffers backing this ChunkedByteBuffer.
   */
  public ByteBuffer[] getChunks() {
    ByteBuffer[] buffs = new ByteBuffer[chunks.length];
    for (int i = 0; i < chunks.length; i++) {
      buffs[i] = chunks[i].duplicate();
    }
    return buffs;
  }


  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  public void dispose() {
    if (!disposed) {
      for (int i = 0; i < chunks.length; i++) {
        dispose(chunks[i]);
      }
      disposed = true;
    }
  }

  public ChunkedByteBuffer slice(long offset, long length) {
    long thisSize = size();
    if (offset < 0 || offset > thisSize - length) {
      throw new IndexOutOfBoundsException(String.format(
          "index: %d, length: %d (expected: range(0, %d))", offset, length, thisSize));
    }
    if (length == 0) {
      return wrap(new ByteBuffer[0]);
    }
    ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();
    int i = 0;
    long sum = 0L;
    while (i < chunks.length && length > 0) {
      long lastSum = sum + chunks[i].remaining();
      if (lastSum > offset) {
        ByteBuffer buffer = chunks[i].duplicate();
        int localLength = (int) Math.min(length, buffer.remaining());
        if (localLength < buffer.remaining()) {
          buffer.limit(buffer.position() + localLength);
        }
        length -= localLength;
        list.add(buffer);
      }
      sum = lastSum;
      i++;
    }
    return wrap(list.toArray(new ByteBuffer[list.size()]));
  }

  public ChunkedByteBuffer duplicate() {
    return new ChunkedByteBuffer(getChunks());
  }

  public static void dispose(ByteBuffer buffer) {
    if (buffer != null && buffer instanceof MappedByteBuffer) {
      logger.trace("Unmapping" + buffer);
      if (buffer instanceof DirectBuffer) {
        DirectBuffer directBuffer = (DirectBuffer) buffer;
        if (directBuffer.cleaner() != null) directBuffer.cleaner().clean();
      }
    }
  }

  public static ChunkedByteBuffer wrap(ByteBuffer chunk) {
    return new ChunkedByteBuffer(chunk);
  }

  public static ChunkedByteBuffer wrap(ByteBuffer[] chunks) {
    return new ChunkedByteBuffer(chunks);
  }

  public static ChunkedByteBuffer wrap(byte[] array) {
    return wrap(array, 0, array.length);
  }

  public static ChunkedByteBuffer wrap(byte[] array, int offset, int length) {
    return new ChunkedByteBuffer(ByteBuffer.wrap(array, offset, length));
  }

  public static ChunkedByteBuffer allocate(int capacity) {
    return new ChunkedByteBuffer(ByteBuffer.allocate(capacity));
  }

  public static ChunkedByteBuffer allocate(int capacity, Allocator allocator) {
    return new ChunkedByteBuffer(allocator.allocate(capacity));
  }
}
