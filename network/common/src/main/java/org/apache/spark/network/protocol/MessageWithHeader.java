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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

/**
 * A wrapper message that holds two separate pieces (a header and a body).
 *
 * The header must be a ByteBuf, while the body can be a ByteBuf or a FileRegion.
 */
class MessageWithHeader extends AbstractReferenceCounted implements FileRegion {

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

  MessageWithHeader(ByteBuf header, Object body, long bodyLength) {
    Preconditions.checkArgument(body instanceof ByteBuf || body instanceof FileRegion,
      "Body must be a ByteBuf or a FileRegion.");
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
    }
    totalBytesTransferred += writtenBody;

    return writtenHeader + writtenBody;
  }

  @Override
  protected void deallocate() {
    header.release();
    ReferenceCountUtil.release(body);
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
