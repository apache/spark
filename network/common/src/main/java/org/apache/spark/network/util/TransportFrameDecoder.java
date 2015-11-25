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

package org.apache.spark.network.util;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * A customized frame decoder that allows intercepting raw data.
 * <p>
 * This behaves like Netty's frame decoder (with harcoded parameters that match this library's
 * needs), except it allows an interceptor to be installed to read data directly before it's
 * framed.
 * <p>
 * Unlike Netty's frame decoder, each frame is dispatched to child handlers as soon as it's
 * decoded, instead of building as many frames as the current buffer allows and dispatching
 * all of them. This allows a child handler to install an interceptor if needed.
 * <p>
 * If an interceptor is installed, framing stops, and data is instead fed directly to the
 * interceptor. When the interceptor indicates that it doesn't need to read any more data,
 * framing resumes. Interceptors should not hold references to the data buffers provided
 * to their handle() method.
 */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {

  public static final String HANDLER_NAME = "frameDecoder";
  private static final int LENGTH_SIZE = 8;
  private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;

  private CompositeByteBuf buffer;
  private volatile Interceptor interceptor;

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
    ByteBuf in = (ByteBuf) data;

    if (buffer == null) {
      buffer = in.alloc().compositeBuffer();
    }

    buffer.addComponent(in).writerIndex(buffer.writerIndex() + in.readableBytes());

    while (buffer.isReadable()) {
      discardReadBytes();
      if (!feedInterceptor()) {
        ByteBuf frame = decodeNext();
        if (frame == null) {
          break;
        }

        ctx.fireChannelRead(frame);
      }
    }

    discardReadBytes();
  }

  private void discardReadBytes() {
    // If the buffer's been retained by downstream code, then make a copy of the remaining
    // bytes into a new buffer. Otherwise, just discard stale components.
    if (buffer.refCnt() > 1) {
      CompositeByteBuf newBuffer = buffer.alloc().compositeBuffer();

      if (buffer.readableBytes() > 0) {
        ByteBuf spillBuf = buffer.alloc().buffer(buffer.readableBytes());
        spillBuf.writeBytes(buffer);
        newBuffer.addComponent(spillBuf).writerIndex(spillBuf.readableBytes());
      }

      buffer.release();
      buffer = newBuffer;
    } else {
      buffer.discardReadComponents();
    }
  }

  private ByteBuf decodeNext() throws Exception {
    if (buffer.readableBytes() < LENGTH_SIZE) {
      return null;
    }

    int frameLen = (int) buffer.readLong() - LENGTH_SIZE;
    if (buffer.readableBytes() < frameLen) {
      buffer.readerIndex(buffer.readerIndex() - LENGTH_SIZE);
      return null;
    }

    Preconditions.checkArgument(frameLen < MAX_FRAME_SIZE, "Too large frame: %s", frameLen);
    Preconditions.checkArgument(frameLen > 0, "Frame length should be positive: %s", frameLen);

    ByteBuf frame = buffer.readSlice(frameLen);
    frame.retain();
    return frame;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (buffer != null) {
      if (buffer.isReadable()) {
        feedInterceptor();
      }
      buffer.release();
    }
    if (interceptor != null) {
      interceptor.channelInactive();
    }
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (interceptor != null) {
      interceptor.exceptionCaught(cause);
    }
    super.exceptionCaught(ctx, cause);
  }

  public void setInterceptor(Interceptor interceptor) {
    Preconditions.checkState(this.interceptor == null, "Already have an interceptor.");
    this.interceptor = interceptor;
  }

  /**
   * @return Whether the interceptor is still active after processing the data.
   */
  private boolean feedInterceptor() throws Exception {
    if (interceptor != null && !interceptor.handle(buffer)) {
      interceptor = null;
    }
    return interceptor != null;
  }

  public static interface Interceptor {

    /**
     * Handles data received from the remote end.
     *
     * @param data Buffer containing data.
     * @return "true" if the interceptor expects more data, "false" to uninstall the interceptor.
     */
    boolean handle(ByteBuf data) throws Exception;

    /** Called if an exception is thrown in the channel pipeline. */
    void exceptionCaught(Throwable cause) throws Exception;

    /** Called if the channel is closed and the interceptor is still installed. */
    void channelInactive() throws Exception;

  }

}
