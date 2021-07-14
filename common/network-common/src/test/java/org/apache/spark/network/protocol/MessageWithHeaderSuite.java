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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.util.AbstractFileRegion;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

import org.apache.spark.network.TestManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.util.ByteArrayWritableChannel;

public class MessageWithHeaderSuite {

  @Test
  public void testSingleWrite() throws Exception {
    testFileRegionBody(8, 8);
  }

  @Test
  public void testShortWrite() throws Exception {
    testFileRegionBody(8, 1);
  }

  @Test
  public void testByteBufBody() throws Exception {
    testByteBufBody(Unpooled.copyLong(42));
  }

  @Test
  public void testCompositeByteBufBodySingleBuffer() throws Exception {
    ByteBuf header = Unpooled.copyLong(42);
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
    compositeByteBuf.addComponent(true, header);
    assertEquals(1, compositeByteBuf.nioBufferCount());
    testByteBufBody(compositeByteBuf);
  }

  @Test
  public void testCompositeByteBufBodyMultipleBuffers() throws Exception {
    ByteBuf header = Unpooled.copyLong(42);
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
    compositeByteBuf.addComponent(true, header.retainedSlice(0, 4));
    compositeByteBuf.addComponent(true, header.slice(4, 4));
    assertEquals(2, compositeByteBuf.nioBufferCount());
    testByteBufBody(compositeByteBuf);
  }

  /**
   * Test writing a {@link MessageWithHeader} using the given {@link ByteBuf} as header.
   *
   * @param header the header to use.
   * @throws Exception thrown on error.
   */
  private void testByteBufBody(ByteBuf header) throws Exception {
    long expectedHeaderValue = header.getLong(header.readerIndex());
    ByteBuf bodyPassedToNettyManagedBuffer = Unpooled.copyLong(84);
    assertEquals(1, header.refCnt());
    assertEquals(1, bodyPassedToNettyManagedBuffer.refCnt());
    ManagedBuffer managedBuf = new NettyManagedBuffer(bodyPassedToNettyManagedBuffer);

    Object body = managedBuf.convertToNetty();
    assertEquals(2, bodyPassedToNettyManagedBuffer.refCnt());
    assertEquals(1, header.refCnt());

    MessageWithHeader msg = new MessageWithHeader(managedBuf, header, body, managedBuf.size());
    ByteBuf result = doWrite(msg, 1);
    assertEquals(msg.count(), result.readableBytes());
    assertEquals(expectedHeaderValue, result.readLong());
    assertEquals(84, result.readLong());

    assertTrue(msg.release());
    assertEquals(0, bodyPassedToNettyManagedBuffer.refCnt());
    assertEquals(0, header.refCnt());
  }

  @Test
  public void testDeallocateReleasesManagedBuffer() throws Exception {
    ByteBuf header = Unpooled.copyLong(42);
    ManagedBuffer managedBuf = Mockito.spy(new TestManagedBuffer(84));
    ByteBuf body = (ByteBuf) managedBuf.convertToNetty();
    assertEquals(2, body.refCnt());
    MessageWithHeader msg = new MessageWithHeader(managedBuf, header, body, body.readableBytes());
    assertTrue(msg.release());
    Mockito.verify(managedBuf, Mockito.times(1)).release();
    assertEquals(0, body.refCnt());
  }

  private void testFileRegionBody(int totalWrites, int writesPerCall) throws Exception {
    ByteBuf header = Unpooled.copyLong(42);
    int headerLength = header.readableBytes();
    TestFileRegion region = new TestFileRegion(totalWrites, writesPerCall);
    MessageWithHeader msg = new MessageWithHeader(null, header, region, region.count());

    ByteBuf result = doWrite(msg, totalWrites / writesPerCall);
    assertEquals(headerLength + region.count(), result.readableBytes());
    assertEquals(42, result.readLong());
    for (long i = 0; i < 8; i++) {
      assertEquals(i, result.readLong());
    }
    assertTrue(msg.release());
  }

  private ByteBuf doWrite(MessageWithHeader msg, int minExpectedWrites) throws Exception {
    int writes = 0;
    ByteArrayWritableChannel channel = new ByteArrayWritableChannel((int) msg.count());
    while (msg.transferred() < msg.count()) {
      msg.transferTo(channel, msg.transferred());
      writes++;
    }
    assertTrue("Not enough writes!", minExpectedWrites <= writes);
    return Unpooled.wrappedBuffer(channel.getData());
  }

  private static class TestFileRegion extends AbstractFileRegion {

    private final int writeCount;
    private final int writesPerCall;
    private int written;

    TestFileRegion(int totalWrites, int writesPerCall) {
      this.writeCount = totalWrites;
      this.writesPerCall = writesPerCall;
    }

    @Override
    public long count() {
      return 8 * writeCount;
    }

    @Override
    public long position() {
      return 0;
    }

    @Override
    public long transferred() {
      return 8 * written;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
      for (int i = 0; i < writesPerCall; i++) {
        ByteBuf buf = Unpooled.copyLong((position / 8) + i);
        ByteBuffer nio = buf.nioBuffer();
        while (nio.remaining() > 0) {
          target.write(nio);
        }
        buf.release();
        written++;
      }
      return 8 * writesPerCall;
    }

    @Override
    protected void deallocate() {
    }

  }

}
