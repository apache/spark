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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * A wrapper message that holds two separate pieces (a header and a body).
 *
 * The header must be a ByteBuf, while the body can be a ByteBuf or a FileRegion.
 */
class MessageWithHeader extends AbstractReferenceCounted implements FileRegion {

  @Nullable private final ManagedBuffer managedBuffer;
  private final ByteBuf header;
  private final int headerLength;
  private final Object body;
  private final long bodyLength;
  private long totalBytesTransferred;
  private ByteBuffer buf = null;

  /**
   * When the write buffer size is larger than this limit, I/O will be done in chunks of this size.
   * The size should not be too large as it will waste underlying memory copy. e.g. If network
   * avaliable buffer is smaller than this limit, the data cannot be sent within one single write
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
    Preconditions.checkArgument(
        body instanceof ByteBuf || body instanceof FileRegion || body instanceof InputStream,
        "Body must be a ByteBuf or a FileRegion or a InputStream.");
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
  public long transfered() {
    return totalBytesTransferred;
  }

  /**
   * This code is more complicated than you would think because we might require multiple
   * transferTo invocations in order to transfer a single MessageWithHeader to avoid busy waiting.
   *
   * The contract is that the caller will ensure position is properly set to the total number
   * of bytes transferred so far (i.e. value returned by transfered()).
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
    } else if (body instanceof InputStream) {
      writtenBody = copyInputStream((InputStream) body, target);
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

  private int copyInputStream(InputStream in, WritableByteChannel target) throws IOException {
    if (buf == null) {
      buf = ByteBuffer.wrap(new byte[NIO_BUFFER_LIMIT]);
    } else if (buf.hasRemaining()) {
      return writeNioBuffer(target, buf);
    }

    byte[] bufArr = buf.array();
    int bufLen = bufArr.length;
    int len = (int) Math.min(bodyLength - (totalBytesTransferred - headerLength), bufLen);
    ByteStreams.readFully(in, bufArr, 0, len);

    buf.limit(len);
    buf.position(0);
    return writeNioBuffer(target, buf);
  }

  private int copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
    ByteBuffer buffer = buf.nioBuffer();
    int written = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
      target.write(buffer) : writeNioBuffer(target, buffer);
    buf.skipBytes(written);
    return written;
  }

  private int writeNioBuffer(
      WritableByteChannel writeCh,
      ByteBuffer buf) throws IOException {
    int originalLimit = buf.limit();
    int ret = 0;

    try {
      int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
      buf.limit(buf.position() + ioSize);
      ret = writeCh.write(buf);
    } finally {
      buf.limit(originalLimit);
    }

    return ret;
  }
}
