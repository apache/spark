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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.ReferenceCountUtil;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.AbstractFileRegion;

/**
 * A wrapper message that holds two separate pieces (a header and a body).
 *
 * The header must be a ByteBuf, while the body can be a ByteBuf or a FileRegion.
 */
class MessageWithHeader extends AbstractFileRegion {

  @Nullable private final ManagedBuffer managedBuffer;
  private final ByteBuf header;
  private final int headerLength;
  private final Object body;
  private final long bodyLength;
  private long totalBytesTransferred;

  /**
   * When the write buffer size is larger than this limit, I/O will be done in chunks of this size.
   * The size should not be too large as it will waste underlying memory copy. e.g. If network
   * available buffer is smaller than this limit, the data cannot be sent within one single write
   * operation while it still will make memory copy with this size.
   */
  private static final int NIO_BUFFER_LIMIT = 256 * 1024;

  /**
   * Construct a new MessageWithHeader.
   *
   * @param managedBuffer the {@link ManagedBuffer} that the message body came from. This needs to
   *                      be passed in so that the buffer can be freed when this message is
   *                      deallocated. Ownership of the caller's reference to this buffer is
   *                      transferred to this class, so if the caller wants to continue to use the
   *                      ManagedBuffer in other messages then they will need to call retain() on
   *                      it before passing it to this constructor. This may be null if and only if
   *                      `body` is a {@link FileRegion}.
   * @param header the message header.
   * @param body the message body. Must be either a {@link ByteBuf} or a {@link FileRegion}.
   * @param bodyLength the length of the message body, in bytes.
     */
  MessageWithHeader(
      @Nullable ManagedBuffer managedBuffer,
      ByteBuf header,
      Object body,
      long bodyLength) {
    Preconditions.checkArgument(body instanceof ByteBuf || body instanceof FileRegion,
      "Body must be a ByteBuf or a FileRegion.");
    this.managedBuffer = managedBuffer;
    this.header = header;
    this.headerLength = header.readableBytes();
    this.body = body;
    this.bodyLength = bodyLength;
  }

  @Override
  public long count() {
    return headerLength + bodyLength;
  }

  @Override
  public long position() {
    return 0;
  }

  @Override
  public long transferred() {
    return totalBytesTransferred;
  }

  /**
   * This code is more complicated than you would think because we might require multiple
   * transferTo invocations in order to transfer a single MessageWithHeader to avoid busy waiting.
   *
   * The contract is that the caller will ensure position is properly set to the total number
   * of bytes transferred so far (i.e. value returned by transferred()).
   */
  @Override
  public long transferTo(final WritableByteChannel target, final long position) throws IOException {
    Preconditions.checkArgument(position == totalBytesTransferred, "Invalid position.");
    // Bytes written for header in this call.
    long writtenHeader = 0;
    if (header.readableBytes() > 0) {
      writtenHeader = copyByteBuf(header, target);
      totalBytesTransferred += writtenHeader;
      if (header.readableBytes() > 0) {
        return writtenHeader;
      }
    }

    // Bytes written for body in this call.
    long writtenBody = 0;
    if (body instanceof FileRegion) {
      writtenBody = ((FileRegion) body).transferTo(target, totalBytesTransferred - headerLength);
    } else if (body instanceof ByteBuf) {
      writtenBody = copyByteBuf((ByteBuf) body, target);
    }
    totalBytesTransferred += writtenBody;

    return writtenHeader + writtenBody;
  }

  @Override
  protected void deallocate() {
    header.release();
    ReferenceCountUtil.release(body);
    if (managedBuffer != null) {
      managedBuffer.release();
    }
  }

  private int copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
    // SPARK-24578: cap the sub-region's size of returned nio buffer to improve the performance
    // for the case that the passed-in buffer has too many components.
    int length = Math.min(buf.readableBytes(), NIO_BUFFER_LIMIT);
    // If the ByteBuf holds more then one ByteBuffer we should better call nioBuffers(...)
    // to eliminate extra memory copies.
    int written = 0;
    if (buf.nioBufferCount() == 1) {
      ByteBuffer buffer = buf.nioBuffer(buf.readerIndex(), length);
      written = target.write(buffer);
    } else {
      ByteBuffer[] buffers = buf.nioBuffers(buf.readerIndex(), length);
      for (ByteBuffer buffer: buffers) {
        int remaining = buffer.remaining();
        int w = target.write(buffer);
        written += w;
        if (w < remaining) {
          // Could not write all, we need to break now.
          break;
        }
      }
    }
    buf.skipBytes(written);
    return written;
  }

  @Override
  public MessageWithHeader touch(Object o) {
    super.touch(o);
    header.touch(o);
    ReferenceCountUtil.touch(body, o);
    return this;
  }

  @Override
  public MessageWithHeader retain(int increment) {
    super.retain(increment);
    header.retain(increment);
    ReferenceCountUtil.retain(body, increment);
    if (managedBuffer != null) {
      for (int i = 0; i < increment; i++) {
        managedBuffer.retain();
      }
    }
    return this;
  }

  @Override
  public boolean release(int decrement) {
    header.release(decrement);
    ReferenceCountUtil.release(body, decrement);
    if (managedBuffer != null) {
      for (int i = 0; i < decrement; i++) {
        managedBuffer.release();
      }
    }
    return super.release(decrement);
  }
}
