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

import java.nio.ByteBuffer;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TransportFrameDecoderSuite {

  @Test
  public void testFrameDecoding() throws Exception {
    Random rnd = new Random();
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    final int frameCount = 100;
    ByteBuf data = Unpooled.buffer();
    try {
      for (int i = 0; i < frameCount; i++) {
        byte[] frame = new byte[1024 * (rnd.nextInt(31) + 1)];
        data.writeLong(frame.length + 8);
        data.writeBytes(frame);
      }

      while (data.isReadable()) {
        int size = rnd.nextInt(16 * 1024) + 256;
        decoder.channelRead(ctx, data.readSlice(Math.min(data.readableBytes(), size)));
      }

      verify(ctx, times(frameCount)).fireChannelRead(any(ByteBuf.class));
    } finally {
      data.release();
    }
  }

  @Test
  public void testInterception() throws Exception {
    final int interceptedReads = 3;
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    TransportFrameDecoder.Interceptor interceptor = spy(new MockInterceptor(interceptedReads));
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    byte[] data = new byte[8];
    ByteBuf len = Unpooled.copyLong(8 + data.length);
    ByteBuf dataBuf = Unpooled.wrappedBuffer(data);

    try {
      decoder.setInterceptor(interceptor);
      for (int i = 0; i < interceptedReads; i++) {
        decoder.channelRead(ctx, dataBuf);
        dataBuf.release();
        dataBuf = Unpooled.wrappedBuffer(data);
      }
      decoder.channelRead(ctx, len);
      decoder.channelRead(ctx, dataBuf);
      verify(interceptor, times(interceptedReads)).handle(any(ByteBuf.class));
      verify(ctx).fireChannelRead(any(ByteBuffer.class));
    } finally {
      len.release();
      dataBuf.release();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeFrameSize() throws Exception {
    testInvalidFrame(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyFrame() throws Exception {
    // 8 because frame size includes the frame length.
    testInvalidFrame(8);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLargeFrame() throws Exception {
    // Frame length includes the frame size field, so need to add a few more bytes.
    testInvalidFrame(Integer.MAX_VALUE + 9);
  }

  private void testInvalidFrame(long size) throws Exception {
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    ByteBuf frame = Unpooled.copyLong(size);
    try {
      decoder.channelRead(ctx, frame);
    } finally {
      frame.release();
    }
  }

  private static class MockInterceptor implements TransportFrameDecoder.Interceptor {

    private int remainingReads;

    MockInterceptor(int readCount) {
      this.remainingReads = readCount;
    }

    @Override
    public boolean handle(ByteBuf data) throws Exception {
      data.readerIndex(data.readerIndex() + data.readableBytes());
      assertFalse(data.isReadable());
      remainingReads -= 1;
      return remainingReads != 0;
    }

    @Override
    public void exceptionCaught(Throwable cause) throws Exception {

    }

    @Override
    public void channelInactive() throws Exception {

    }

  }

}
