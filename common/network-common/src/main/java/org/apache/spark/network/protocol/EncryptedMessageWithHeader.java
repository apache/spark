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

import java.io.EOFException;
import java.io.InputStream;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedStream;
import io.netty.handler.stream.ChunkedInput;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * A wrapper message that holds two separate pieces (a header and a body).
 *
 * The header must be a ByteBuf, while the body can be any InputStream or ChunkedStream
 */
public class EncryptedMessageWithHeader implements ChunkedInput<ByteBuf> {

  @Nullable private final ManagedBuffer managedBuffer;
  private final ByteBuf header;
  private final int headerLength;
  private final Object body;
  private final long bodyLength;
  private long totalBytesTransferred;

  /**
   * Construct a new EncryptedMessageWithHeader.
   *
   * @param managedBuffer the {@link ManagedBuffer} that the message body came from. This needs to
   *                      be passed in so that the buffer can be freed when this message is
   *                      deallocated. Ownership of the caller's reference to this buffer is
   *                      transferred to this class, so if the caller wants to continue to use the
   *                      ManagedBuffer in other messages then they will need to call retain() on
   *                      it before passing it to this constructor.
   * @param header the message header.
   * @param body the message body.
   * @param bodyLength the length of the message body, in bytes.
   */

  public EncryptedMessageWithHeader(
      @Nullable ManagedBuffer managedBuffer, ByteBuf header, Object body, long bodyLength) {
    Preconditions.checkArgument(body instanceof InputStream || body instanceof ChunkedStream,
      "Body must be an InputStream or a ChunkedStream.");
    this.managedBuffer = managedBuffer;
    this.header = header;
    this.headerLength = header.readableBytes();
    this.body = body;
    this.bodyLength = bodyLength;
    this.totalBytesTransferred = 0;
  }

  @Override
  public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
    return readChunk(ctx.alloc());
  }

  @Override
  public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
    if (isEndOfInput()) {
      return null;
    }

    if (totalBytesTransferred < headerLength) {
      totalBytesTransferred += headerLength;
      return header.retain();
    } else if (body instanceof InputStream stream) {
      int available = stream.available();
      if (available <= 0) {
        available = (int) (length() - totalBytesTransferred);
      } else {
        available = (int) Math.min(available, length() - totalBytesTransferred);
      }
      ByteBuf buffer = allocator.buffer(available);
      int toRead = Math.min(available, buffer.writableBytes());
      int read = buffer.writeBytes(stream, toRead);
      if (read >= 0) {
        totalBytesTransferred += read;
        return buffer;
      } else {
        throw new EOFException("Unable to read bytes from InputStream");
      }
    } else if (body instanceof ChunkedStream stream) {
      long old = stream.transferredBytes();
      ByteBuf buffer = stream.readChunk(allocator);
      long read = stream.transferredBytes() - old;
      if (read >= 0) {
        totalBytesTransferred += read;
        assert(totalBytesTransferred <= length());
        return buffer;
      } else {
        throw new EOFException("Unable to read bytes from ChunkedStream");
      }
    } else {
      return null;
    }
  }

  @Override
  public long length() {
    return headerLength + bodyLength;
  }

  @Override
  public long progress() {
    return totalBytesTransferred;
  }

  @Override
  public boolean isEndOfInput() throws Exception {
    return (headerLength + bodyLength) == totalBytesTransferred;
  }

  @Override
  public void close() throws Exception {
    header.release();
    if (managedBuffer != null) {
      managedBuffer.release();
    }
    if (body instanceof InputStream stream) {
      stream.close();
    } else if (body instanceof ChunkedStream stream) {
      stream.close();
    }
  }
}
