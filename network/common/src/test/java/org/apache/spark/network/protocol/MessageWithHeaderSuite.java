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
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import org.junit.Test;

import static org.junit.Assert.*;

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
    ByteBuf header = Unpooled.copyLong(42);
    ByteBuf body = Unpooled.copyLong(84);
    MessageWithHeader msg = new MessageWithHeader(header, body, body.readableBytes());

    ByteBuf result = doWrite(msg, 1);
    assertEquals(msg.count(), result.readableBytes());
    assertEquals(42, result.readLong());
    assertEquals(84, result.readLong());
  }

  private void testFileRegionBody(int totalWrites, int writesPerCall) throws Exception {
    ByteBuf header = Unpooled.copyLong(42);
    int headerLength = header.readableBytes();
    TestFileRegion region = new TestFileRegion(totalWrites, writesPerCall);
    MessageWithHeader msg = new MessageWithHeader(header, region, region.count());

    ByteBuf result = doWrite(msg, totalWrites / writesPerCall);
    assertEquals(headerLength + region.count(), result.readableBytes());
    assertEquals(42, result.readLong());
    for (long i = 0; i < 8; i++) {
      assertEquals(i, result.readLong());
    }
  }

  private ByteBuf doWrite(MessageWithHeader msg, int minExpectedWrites) throws Exception {
    int writes = 0;
    ByteArrayWritableChannel channel = new ByteArrayWritableChannel((int) msg.count());
    while (msg.transfered() < msg.count()) {
      msg.transferTo(channel, msg.transfered());
      writes++;
    }
    assertTrue("Not enough writes!", minExpectedWrites <= writes);
    return Unpooled.wrappedBuffer(channel.getData());
  }

  private static class TestFileRegion extends AbstractReferenceCounted implements FileRegion {

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
    public long transfered() {
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
