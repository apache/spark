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

package org.apache.spark.network.buffer.nio;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

import com.google.common.base.Throwables;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.AbstractReferenceCounted;
import org.apache.spark.network.buffer.ChunkedByteBuffer;
import org.apache.spark.network.buffer.ChunkedByteBufferUtil;
import org.apache.spark.network.buffer.IllegalReferenceCountException;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.JavaUtils;

public class ChunkedByteBufferImpl extends AbstractReferenceCounted implements ChunkedByteBuffer {
  private static final Logger logger = LoggerFactory.getLogger(ChunkedByteBufferImpl.class);
  private static final int BUF_SIZE = 0x1000; // 4K
  private static final ByteBuffer[] emptyChunks = new ByteBuffer[0];
  private ByteBuffer[] chunks = null;

  // For deserialization only
  public ChunkedByteBufferImpl() {
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
  public ChunkedByteBufferImpl(ByteBuffer[] chunks) {
    this.chunks = chunks;
    Preconditions.checkArgument(chunks != null, "chunks must not be null");
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    ensureAccessible();
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

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    ensureAccessible();
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
  @Override
  public long size() {
    ensureAccessible();
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
    ensureAccessible();
    for (int i = 0; i < chunks.length; i++) {
      ByteBuffer bytes = chunks[i].duplicate();
      while (bytes.remaining() > 0) {
        channel.write(bytes);
      }
    }
  }

  /**
   * Write this buffer to a outputStream.
   */
  @Override
  public void writeFully(OutputStream outputStream) throws IOException {
    ensureAccessible();
    ByteStreams.copy(toInputStream(), outputStream);
  }

  /**
   * Wrap this buffer to view it as a Netty ByteBuf.
   */
  @Override
  public ByteBuf toNetty() {
    ensureAccessible();
    long len = size();
    Preconditions.checkArgument(size() <= Integer.MAX_VALUE,
        "Too large ByteBuf: %s", new Object[]{Long.valueOf(len)});
    return Unpooled.wrappedBuffer(toByteBuffers());
  }

  /**
   * Copy this buffer into a new byte array.
   *
   * @throws UnsupportedOperationException if this buffer's size exceeds the maximum array size.
   */
  @Override
  public byte[] toArray() {
    ensureAccessible();
    try {
      if (chunks.length == 1) {
        return JavaUtils.bufferToArray(chunks[0]);
      } else {
        long len = size();
        if (len >= Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("cannot call toArray because buffer size (" +
              len + " bytes) exceeds maximum array size");
        }
        ByteArrayWritableChannel byteChannel = new ByteArrayWritableChannel((int) len);
        writeFully(byteChannel);
        byteChannel.close();
        return byteChannel.getData();
      }
    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Copy this buffer into a new ByteBuffer.
   *
   * @throws UnsupportedOperationException if this buffer's size exceeds the max ByteBuffer size.
   */
  @Override
  public ByteBuffer toByteBuffer() {
    ensureAccessible();
    if (chunks.length == 1) {
      return chunks[0].duplicate();
    } else {
      return ByteBuffer.wrap(this.toArray());
    }
  }

  @Override
  public InputStream toInputStream() {
    return toInputStream(false);
  }

  /**
   * Creates an input stream to read data from this ChunkedByteBuffer.
   *
   * @param dispose if true, [[dispose()]] will be called at the end of the stream
   *                in order to close any memory-mapped files which back this buffer.
   */
  @Override
  public ChunkedByteBufferInputStream toInputStream(boolean dispose) {
    ensureAccessible();
    return new ChunkedByteBufferInputStream(this, dispose);
  }

  /**
   * Make a copy of this ChunkedByteBuffer, copying all of the backing data into new buffers.
   * The new buffer will share no resources with the original buffer.
   */
  @Override
  public ChunkedByteBuffer copy() {
    ensureAccessible();
    ByteBuffer[] copiedChunks = new ByteBuffer[chunks.length];
    for (int i = 0; i < chunks.length; i++) {
      ByteBuffer chunk = chunks[i].duplicate();
      ByteBuffer newChunk = ByteBuffer.allocate(chunk.remaining());
      newChunk.put(chunk);
      newChunk.flip();
      copiedChunks[i] = newChunk;
    }
    return ChunkedByteBufferUtil.wrap(copiedChunks);
  }

  /**
   * Get duplicates of the ByteBuffers backing this ChunkedByteBuffer.
   */
  @Override
  public ByteBuffer[] toByteBuffers() {
    ensureAccessible();
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
  @Override
  protected void deallocate() {
    for (int i = 0; i < chunks.length; i++) {
      ChunkedByteBufferUtil.dispose(chunks[i]);
    }
  }

  @Override
  public ChunkedByteBuffer slice(long offset, long length) {
    ensureAccessible();
    long thisSize = size();
    if (offset < 0 || offset > thisSize - length) {
      throw new IndexOutOfBoundsException(String.format(
          "index: %d, length: %d (expected: range(0, %d))", offset, length, thisSize));
    }
    if (length == 0) {
      return ChunkedByteBufferUtil.wrap();
    }
    ArrayList<ByteBuffer> list = new ArrayList<>();
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
    return new DerivedChunkedByteBuffer(list.toArray(new ByteBuffer[list.size()]), this);
  }

  @Override
  public ChunkedByteBuffer duplicate() {
    ensureAccessible();
    return new DerivedChunkedByteBuffer(toByteBuffers(), this);
  }

  @Override
  public ChunkedByteBuffer retain() {
    super.retain();
    return this;
  }

  @Override
  public ChunkedByteBuffer retain(int increment) {
    super.retain(increment);
    return this;
  }

  /**
   * Should be called by every method that tries to access the buffers content to check
   * if the buffer was released before.
   */
  protected final void ensureAccessible() {
    if (refCnt() == 0) throw new IllegalReferenceCountException(0);
  }
}
