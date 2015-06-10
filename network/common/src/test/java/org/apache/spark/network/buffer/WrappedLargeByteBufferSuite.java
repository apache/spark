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

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.*;

public class WrappedLargeByteBufferSuite {

  private byte[] data = new byte[500];
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
    return new WrappedLargeByteBuffer(bufs, 50);
  }

  @Test
  public void asByteBuffer() throws BufferTooLargeException {
    // test that it works when buffer is small
    LargeByteBuffer buf = LargeByteBufferHelper.asLargeByteBuffer(new byte[100]);
    ByteBuffer nioBuf = buf.asByteBuffer();
    assertEquals(0, nioBuf.position());
    assertEquals(100, nioBuf.remaining());
    // if we move the large byte buffer, the nio.ByteBuffer we have doesn't change
    buf.skip(10);
    assertEquals(0, nioBuf.position());
    assertEquals(100, nioBuf.remaining());
    // if we grab another byte buffer while the large byte buffer's position != 0,
    // the returned buffer still has position 0
    ByteBuffer nioBuf2 = buf.asByteBuffer();
    assertEquals(0, nioBuf2.position());
    assertEquals(100, nioBuf2.remaining());
    // the two byte buffers we grabbed are independent
    nioBuf2.position(20);
    assertEquals(0, nioBuf.position());
    assertEquals(100, nioBuf.remaining());
    assertEquals(20, nioBuf2.position());
    assertEquals(80, nioBuf2.remaining());

    // the right error when the buffer is too big
    try {
      WrappedLargeByteBuffer buf2 = new WrappedLargeByteBuffer(
        new ByteBuffer[]{ByteBuffer.allocate(10), ByteBuffer.allocate(10)}, 10);
      // you really shouldn't ever construct a WrappedLargeByteBuffer with
      // multiple small chunks, so this is somewhat contrived
      buf2.asByteBuffer();
      fail("expected an exception");
    } catch (BufferTooLargeException btl) {
    }
  }

  @Test
  public void checkSizesOfInternalBuffers() {
    errorOnBuffersSized(10, new int[]{9,10});
    errorOnBuffersSized(10, new int[]{10,10,0,10});
    errorOnBuffersSized(20, new int[]{10,10,10,10});
  }

  private void errorOnBuffersSized(int chunkSize, int[] sizes) {
    ByteBuffer[] bufs = new ByteBuffer[sizes.length];
    for (int i = 0; i < sizes.length; i++) {
      bufs[i] = ByteBuffer.allocate(sizes[i]);
    }
    try {
      new WrappedLargeByteBuffer(bufs, chunkSize);
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
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
    b.rewind();
    b.skip(495);
    assertEquals(data[495], b.get());
    assertEquals(data[496], b.get());
    assertEquals(data[497], b.get());
    assertEquals(data[498], b.get());
    assertEquals(data[499], b.get());
    try {
      b.get();
      fail("expected exception");
    } catch (BufferUnderflowException bue) {
    }
  }

  @Test
  public void writeTo() throws IOException {
    for (int initialPosition: new int[]{0,20, 400}) {
      File testFile = File.createTempFile("WrappedLargeByteBuffer-writeTo-" + initialPosition,".bin");
      testFile.deleteOnExit();
      FileChannel channel = new FileOutputStream(testFile).getChannel();
      WrappedLargeByteBuffer buf = testDataBuf();
      buf.skip(initialPosition);
      assertEquals(initialPosition, buf.position());
      int expN = 500 - initialPosition;
      long bytesWritten = buf.writeTo(channel);
      assertEquals(expN, bytesWritten);
      channel.close();

      byte[] fileBytes = new byte[expN];
      FileInputStream in = new FileInputStream(testFile);
      int n = 0;
      while (n < expN) {
        n += in.read(fileBytes, n, expN - n);
      }
      assertEquals(-1, in.read());
      byte[] dataSlice = Arrays.copyOfRange(data, initialPosition, 500);
      assertArrayEquals(dataSlice, fileBytes);
      assertEquals(0, buf.remaining());
      assertEquals(500, buf.position());
    }
  }

  @Test
  public void duplicate() {
    for (int initialPosition: new int[]{0,20, 400}) {
      WrappedLargeByteBuffer buf = testDataBuf();
      buf.skip(initialPosition);

      WrappedLargeByteBuffer dup = buf.duplicate();
      assertEquals(initialPosition, buf.position());
      assertEquals(initialPosition, dup.position());
      assertEquals(500, buf.size());
      assertEquals(500, dup.size());
      assertEquals(500 - initialPosition, buf.remaining());
      assertEquals(500 - initialPosition, dup.remaining());
      assertConsistent(buf);
      assertConsistent(dup);

      // check positions of both buffers are independent
      buf.skip(20);
      assertEquals(initialPosition + 20, buf.position());
      assertEquals(initialPosition, dup.position());
      assertConsistent(buf);
      assertConsistent(dup);
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testRequireAtLeastOneBuffer() {
    new WrappedLargeByteBuffer( new ByteBuffer[0]);
  }

  @Test
  public void positionIndependentOfInitialBuffers() {
    ByteBuffer[] byteBufs = testDataBuf().underlying;
    byteBufs[0].position(50);
    for (int initialPosition: new int[]{0,20, 400}) {
      WrappedLargeByteBuffer buf = new WrappedLargeByteBuffer(byteBufs, 50);
      assertEquals(0L, buf.position());
      assertEquals(50, byteBufs[0].position());
      buf.skip(initialPosition);
      assertEquals(initialPosition, buf.position());
      assertEquals(50, byteBufs[0].position());
    }
  }

  private void assertConsistent(WrappedLargeByteBuffer buffer) {
    long pos = buffer.position();
    long bufferStartPos = 0;
    if (buffer.currentBufferIdx < buffer.underlying.length) {
      assertEquals(buffer.currentBuffer, buffer.underlying[buffer.currentBufferIdx]);
    } else {
      assertNull(buffer.currentBuffer);
    }
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

  private void assertSubArrayEquals(
      byte[] exp,
      int expOffset,
      byte[] act,
      int actOffset,
      int length) {
    byte[] expCopy = new byte[length];
    byte[] actCopy = new byte[length];
    System.arraycopy(exp, expOffset, expCopy, 0, length);
    System.arraycopy(act, actOffset, actCopy, 0, length);
    assertArrayEquals(expCopy, actCopy);
  }

}
