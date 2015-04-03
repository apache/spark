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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.*;

public class WrappedLargeByteBufferSuite {

  byte[] data = new byte[500];
  {
    new Random(1234).nextBytes(data);
  }

  private WrappedLargeByteBuffer testDataBuf() {
    ByteBuffer[] bufs = new ByteBuffer[10];
    for (int i = 0; i < 10; i++) {
      byte[] b = new byte[50];
      System.arraycopy(data, i * 50, b, 0, 50);
      bufs[i] = ByteBuffer.wrap(b);
    }
    return new WrappedLargeByteBuffer(bufs);
  }

  @Test
  public void asByteBuffer() throws BufferTooLargeException {
    //test that it works when buffer is small, and the right error when buffer is big
    LargeByteBuffer buf = LargeByteBufferHelper.asLargeByteBuffer(new byte[100]);
    ByteBuffer nioBuf = buf.asByteBuffer();
    assertEquals(100, nioBuf.remaining());

    ByteBuffer[] bufs = new ByteBuffer[2];
    for (int i = 0; i < 2; i++) {
      bufs[i] = ByteBuffer.allocate(10);
    }
    try {
      new WrappedLargeByteBuffer(bufs).asByteBuffer();
      fail("expected an exception");
    } catch (BufferTooLargeException btl) {
    }
  }

  @Test
  public void deepCopy() {
    WrappedLargeByteBuffer b = testDataBuf();
    //intentionally move around sporadically
    for (int initialPosition: new int[]{10,475, 0, 19, 58, 499, 498, 32, 234, 378}) {
      b.rewind();
      b.skip(initialPosition);
      WrappedLargeByteBuffer copy = b.deepCopy();
      assertEquals(0, copy.position());
      assertConsistent(copy);
      assertConsistent(b);
      assertEquals(b.size(), copy.size());
      assertEquals(initialPosition, b.position());
      byte[] copyData = new byte[500];
      copy.get(copyData, 0, 500);
      assertArrayEquals(data, copyData);
    }
  }

  @Test
  public void skipAndGet() {
    WrappedLargeByteBuffer b = testDataBuf();
    int position = 0;
    for (int move: new int[]{20, 50, 100, 0, -80, 0, 200, -175, 500, 0, -1000, 0}) {
      long moved = b.skip(move);
      assertConsistent(b);
      long expMoved = move > 0 ? Math.min(move, 500 - position) : Math.max(move, -position);
      position += moved;
      assertEquals(expMoved, moved);
      assertEquals(position, b.position());
      byte[] copyData = new byte[500 - position];
      b.get(copyData, 0, 500 - position);
      assertConsistent(b);
      byte[] dataSubset = new byte[500 - position];
      System.arraycopy(data, position, dataSubset, 0, 500 - position);
      assertArrayEquals(dataSubset, copyData);
      b.rewind();
      assertConsistent(b);
      b.skip(position);
      assertConsistent(b);
    }
  }

  private void assertConsistent(WrappedLargeByteBuffer buffer) {
    long pos = buffer.position();
    long bufferStartPos = 0;
    for (ByteBuffer p: buffer.nioBuffers()) {
      if (pos < bufferStartPos) {
        assertEquals(0, p.position());
      } else if (pos < bufferStartPos + p.capacity()) {
        assertEquals(pos - bufferStartPos, p.position());
      } else {
        assertEquals(p.capacity(), p.position());
      }
      bufferStartPos += p.capacity();
    }
  }

  @Test
  public void get() {
    WrappedLargeByteBuffer b = testDataBuf();
    byte[] into = new byte[500];
    for (int[] offsetAndLength: new int[][]{{0, 200}, {10,10}, {300, 20}, {30, 100}}) {
      b.rewind();
      b.get(into, offsetAndLength[0], offsetAndLength[1]);
      assertConsistent(b);
      assertSubArrayEquals(data, 0, into, offsetAndLength[0], offsetAndLength[1]);
    }

    try {
      b.rewind();
      b.skip(400);
      b.get(into, 0, 500);
      fail("expected exception");
    } catch (BufferUnderflowException bue) {
    }
  }

  private void assertSubArrayEquals(byte[] exp, int expOffset, byte[] act, int actOffset, int length) {
    byte[] expCopy = new byte[length];
    byte[] actCopy = new byte[length];
    System.arraycopy(exp, expOffset, expCopy, 0, length);
    System.arraycopy(act, actOffset, actCopy, 0, length);
    assertArrayEquals(expCopy, actCopy);
  }


}
