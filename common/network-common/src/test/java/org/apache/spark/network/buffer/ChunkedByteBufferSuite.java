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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChunkedByteBufferSuite {

  private final Random rad = new Random();
  private final ByteBuffer empty = ByteBuffer.wrap(new byte[0]);

  @Test
  public void noChunks() throws Exception {
    ChunkedByteBuffer emptyChunkedByteBuffer = ChunkedByteBufferUtil.wrap(new ByteBuffer[0]);
    assertEquals(0, emptyChunkedByteBuffer.size());
    assertEquals(0, emptyChunkedByteBuffer.toByteBuffers().length);
    assertEquals(0, emptyChunkedByteBuffer.toArray().length);
    assertEquals(0, emptyChunkedByteBuffer.toByteBuffer().capacity());
    assertEquals(0, emptyChunkedByteBuffer.toNetty().capacity());
    emptyChunkedByteBuffer.toInputStream().close();
    emptyChunkedByteBuffer.toInputStream(true).close();
  }

  @Test
  public void toByteBuffers() throws Exception {
    ChunkedByteBuffer chunkedByteBuffer = ChunkedByteBufferUtil.wrap(ByteBuffer.allocate(8));
    chunkedByteBuffer.toByteBuffers()[0].position(4);
    assertEquals(0, chunkedByteBuffer.toByteBuffers()[0].position());

    chunkedByteBuffer = ChunkedByteBufferUtil.wrap(new ByteBuffer[]{ByteBuffer.allocate(8),
        ByteBuffer.allocate(5)});
    assertEquals(2, chunkedByteBuffer.toByteBuffers().length);
    assertEquals(13, chunkedByteBuffer.toByteBuffer().capacity());
  }

  @Test
  public void copy() throws Exception {
    byte[] arr = new byte[8];
    rad.nextBytes(arr);
    ChunkedByteBuffer chunkedByteBuffer = ChunkedByteBufferUtil.wrap(arr);
    ChunkedByteBuffer copiedChunkedByteBuffer = chunkedByteBuffer.copy();
    assertArrayEquals(chunkedByteBuffer.toArray(), copiedChunkedByteBuffer.toArray());
  }

  /**
   * writeFully() does not affect original buffer's position
   */
  @Test
  public void writeFully() throws Exception {
    byte[] arr = new byte[8];
    rad.nextBytes(arr);
    ChunkedByteBuffer chunkedByteBuffer = ChunkedByteBufferUtil.wrap(arr);
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) chunkedByteBuffer.size());
    chunkedByteBuffer.writeFully(out);
    assertArrayEquals(arr, out.toByteArray());
    assertArrayEquals(arr, chunkedByteBuffer.toArray());
  }

  @Test
  public void toArray() throws Exception {
    byte[] bytes = new byte[8];
    rad.nextBytes(bytes);
    ChunkedByteBuffer chunkedByteBuffer = ChunkedByteBufferUtil.wrap(new ByteBuffer[]{empty,
        ByteBuffer.wrap(bytes)});
    assertArrayEquals(bytes, chunkedByteBuffer.toArray());

    ByteBuffer fourMegabyteBuffer = ByteBuffer.allocate(1024 * 1024 * 4);
    fourMegabyteBuffer.limit(fourMegabyteBuffer.capacity());
    ByteBuffer[] buffers = new ByteBuffer[1024];
    for (int i = 0; i < 1024; i++) {
      buffers[i] = fourMegabyteBuffer;
    }
    chunkedByteBuffer = ChunkedByteBufferUtil.wrap(buffers);
    assertEquals((1024L * 1024L * 1024L * 4L), chunkedByteBuffer.size());
    Throwable exception = null;
    try {
      chunkedByteBuffer.toArray();
    } catch (UnsupportedOperationException e) {
      exception = e;
    }
    assertNotNull(exception);
  }

  @Test
  public void toInputStream() throws Exception {
    byte[] bytes1 = new byte[5];
    rad.nextBytes(bytes1);
    byte[] bytes2 = new byte[8];
    rad.nextBytes(bytes2);
    ChunkedByteBuffer chunkedByteBuffer = ChunkedByteBufferUtil.wrap(new ByteBuffer[]{
        ByteBuffer.wrap(bytes1), ByteBuffer.wrap(bytes2)});
    byte[] arr = new byte[13];
    DataInput in = new DataInputStream(chunkedByteBuffer.toInputStream());
    in.readFully(arr);
    assertEquals(bytes2[7], arr[12]);
    assertEquals(bytes1[4], arr[4]);
  }

  private ChunkedByteBuffer[] genChunkedByteBuffer() {
    byte[] bytes1 = new byte[5];
    byte[] bytes2 = new byte[8];
    byte[] bytes3 = new byte[12];
    rad.nextBytes(bytes1);
    rad.nextBytes(bytes2);
    rad.nextBytes(bytes3);

    return new ChunkedByteBuffer[]{
        ChunkedByteBufferUtil.wrap(new ByteBuffer[]{
            ByteBuffer.wrap(bytes1), ByteBuffer.wrap(bytes2), ByteBuffer.wrap(bytes3)}),
        ChunkedByteBufferUtil.wrap(new ByteBuf[]{
            Unpooled.wrappedBuffer(bytes1), Unpooled.wrappedBuffer(bytes2), Unpooled.wrappedBuffer(bytes3)})
    };
  }

  @Test
  public void derivedChunkedByteBufferAtBoundarySize() throws Exception {
    ChunkedByteBuffer[] buffers = genChunkedByteBuffer();
    for (int i = 0; i < buffers.length; i++) {
      ChunkedByteBuffer chunkedByteBuffer = buffers[i];
      ChunkedByteBuffer sliceBuffer = chunkedByteBuffer.slice(5, 13);
      assertEquals(13, sliceBuffer.toByteBuffer().remaining());
      assertEquals(2, sliceBuffer.toByteBuffers().length);
      assertArrayEquals(Arrays.copyOfRange(chunkedByteBuffer.toArray(), 5, 18), sliceBuffer.toArray());

      sliceBuffer = chunkedByteBuffer.slice(13, 12);
      assertEquals(12, sliceBuffer.toByteBuffer().remaining());
      assertEquals(1, sliceBuffer.toByteBuffers().length);
      assertArrayEquals(Arrays.copyOfRange(chunkedByteBuffer.toArray(), 13, 25), sliceBuffer.toArray());

      sliceBuffer.release();
      assertEquals(chunkedByteBuffer.refCnt(), 0);
    }
  }

  @Test
  public void derivedChunkedByteBuffer() throws Exception {
    ChunkedByteBuffer[] buffers = genChunkedByteBuffer();
    for (int i = 0; i < buffers.length; i++) {
      ChunkedByteBuffer chunkedByteBuffer = buffers[i];
      ChunkedByteBuffer sliceBuffer = chunkedByteBuffer.slice(4, 12);
      assertEquals(12, sliceBuffer.toByteBuffer().remaining());
      assertEquals(3, sliceBuffer.toByteBuffers().length);
      assertArrayEquals(Arrays.copyOfRange(chunkedByteBuffer.toArray(), 4, 16), sliceBuffer.toArray());

      ChunkedByteBuffer dupBufferBuffer = sliceBuffer.duplicate();
      assertEquals(dupBufferBuffer.size(), 12);
      assertArrayEquals(dupBufferBuffer.toArray(), sliceBuffer.toArray());

      dupBufferBuffer.release();
      assertEquals(chunkedByteBuffer.refCnt(), 0);
    }
  }

  @Test
  public void referenceCounted() throws Exception {
    ChunkedByteBuffer byteBuffer = ChunkedByteBufferUtil.wrap();
    assertEquals(1, byteBuffer.refCnt());
    byteBuffer.retain();
    assertEquals(2, byteBuffer.refCnt());
    byteBuffer.retain(2);
    assertEquals(4, byteBuffer.refCnt());
    byteBuffer.release(2);
    assertEquals(2, byteBuffer.refCnt());
    byteBuffer.release(1);
    assertEquals(1, byteBuffer.refCnt());
    byteBuffer.release();
    Throwable exception = null;
    try {
      byteBuffer.release();
    } catch (IllegalReferenceCountException e) {
      exception = e;
    }
    assertNotNull(exception);
  }

  @Test
  public void externalizable() throws Exception {
    byte[] bytes1 = new byte[5];
    rad.nextBytes(bytes1);
    byte[] bytes2 = new byte[8];
    rad.nextBytes(bytes2);
    ChunkedByteBuffer chunkedByteBuffer = ChunkedByteBufferUtil.wrap(new ByteBuffer[]{
        ByteBuffer.wrap(bytes1), ByteBuffer.wrap(bytes2)});
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(out);
    chunkedByteBuffer.writeExternal(objectOutput);
    objectOutput.close();
    ChunkedByteBuffer chunkedByteBuffer2 = ChunkedByteBufferUtil.wrap();
    ObjectInputStream objectInput = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
    chunkedByteBuffer2.readExternal(objectInput);
    assertEquals(2, chunkedByteBuffer2.toByteBuffers().length);
    assertArrayEquals(chunkedByteBuffer.toArray(), chunkedByteBuffer2.toArray());
  }
}
