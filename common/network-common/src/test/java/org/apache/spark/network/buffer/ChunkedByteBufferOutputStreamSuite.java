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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.*;

public class ChunkedByteBufferOutputStreamSuite {
  private final Random rad = new Random();
  private final ByteBuffer empty = ByteBuffer.wrap(new byte[0]);

  @Test
  public void emptyOutput() throws Exception {
    ChunkedByteBufferOutputStream o = ChunkedByteBufferOutputStream.newInstance(1024);
    o.close();
    assertEquals(o.toChunkedByteBuffer().size(), 0);
  }

  @Test
  public void writeASingleByte() throws Exception {
    ChunkedByteBufferOutputStream o = ChunkedByteBufferOutputStream.newInstance(1024);
    o.write(10);
    o.close();
    ChunkedByteBuffer chunkedByteBuffer = o.toChunkedByteBuffer();
    assertEquals(1, chunkedByteBuffer.toByteBuffers().length);
    assertEquals(1, chunkedByteBuffer.toByteBuffer().remaining());
    assertEquals((byte) 10, chunkedByteBuffer.toArray()[0]);
  }

  @Test
  public void writeAsingleNearBoundary() throws Exception {
    ChunkedByteBufferOutputStream o = ChunkedByteBufferOutputStream.newInstance(10);
    byte[] bytes = new byte[9];
    o.write(bytes);
    o.write(99);
    o.close();
    ChunkedByteBuffer chunkedByteBuffer = o.toChunkedByteBuffer();
    assertEquals(1, chunkedByteBuffer.toByteBuffers().length);
    assertEquals((byte) 99, chunkedByteBuffer.toByteBuffers()[0].get(9));
  }

  @Test
  public void writeASingleAtboundary() throws Exception {
    ChunkedByteBufferOutputStream o = ChunkedByteBufferOutputStream.newInstance(10);
    byte[] bytes = new byte[10];
    o.write(bytes);
    o.write(99);
    o.close();
    ByteBuffer[] byteBuffers = o.toChunkedByteBuffer().toByteBuffers();
    assertEquals(2, byteBuffers.length);
    assertEquals(1, byteBuffers[1].remaining());
    assertEquals((byte) 99, byteBuffers[1].get());
  }

  @Test
  public void singleChunkOutput() throws Exception {
    byte[] bytes = new byte[9];
    rad.nextBytes(bytes);
    ChunkedByteBufferOutputStream o = ChunkedByteBufferOutputStream.newInstance(10);
    o.write(bytes);
    o.close();
    ByteBuffer[] byteBuffers = o.toChunkedByteBuffer().toByteBuffers();
    assertEquals(1, byteBuffers.length);
    assertEquals(bytes.length, byteBuffers[0].remaining());
    byte[] arrRef = new byte[9];
    byteBuffers[0].get(arrRef);
    assertArrayEquals(bytes, arrRef);
  }

  @Test
  public void singleChunkOutputAtBoundarySize() throws Exception {
    byte[] ref = new byte[10];
    rad.nextBytes(ref);
    ChunkedByteBufferOutputStream o = ChunkedByteBufferOutputStream.newInstance(10);
    o.write(ref);
    o.close();
    ByteBuffer[] arrays = o.toChunkedByteBuffer().toByteBuffers();
    assertEquals(1, arrays.length);
    assertEquals(ref.length, arrays[0].remaining());
    byte[] arrRef = new byte[10];
    arrays[0].get(arrRef);
    assertArrayEquals(ref, arrRef);
  }

  @Test
  public void multipleChunkOutput() throws Exception {
    byte[] ref = new byte[26];
    rad.nextBytes(ref);
    ChunkedByteBufferOutputStream o = ChunkedByteBufferOutputStream.newInstance(10);
    o.write(ref);
    o.close();
    ByteBuffer[] arrays = o.toChunkedByteBuffer().toByteBuffers();
    assertEquals(arrays.length, 3);
    assertEquals(arrays[0].remaining(), 10);
    assertEquals(arrays[1].remaining(), 10);
    assertEquals(arrays[2].remaining(), 6);

    byte[] arrRef = new byte[10];
    arrays[0].get(arrRef);

    assertArrayEquals(Arrays.copyOfRange(ref, 0, 10), arrRef);

    arrays[1].get(arrRef);
    assertArrayEquals(Arrays.copyOfRange(ref, 10, 20), arrRef);

    arrays[2].get(arrRef, 0, 6);
    assertArrayEquals(Arrays.copyOfRange(ref, 20, 26), Arrays.copyOfRange(arrRef, 0, 6));
  }

  @Test
  public void multipleChunkOutputAtBoundarySize() throws Exception {
    byte[] ref = new byte[30];
    rad.nextBytes(ref);
    ChunkedByteBufferOutputStream o = ChunkedByteBufferOutputStream.newInstance(10);
    o.write(ref);
    o.close();
    ByteBuffer[] arrays = o.toChunkedByteBuffer().toByteBuffers();
    assertEquals(arrays.length, 3);
    assertEquals(arrays[0].remaining(), 10);
    assertEquals(arrays[1].remaining(), 10);
    assertEquals(arrays[2].remaining(), 10);

    byte[] arrRef = new byte[10];

    arrays[0].get(arrRef);
    assertArrayEquals(Arrays.copyOfRange(ref, 0, 10), arrRef);

    arrays[1].get(arrRef);
    assertArrayEquals(Arrays.copyOfRange(ref, 10, 20), arrRef);

    arrays[2].get(arrRef);
    assertArrayEquals(Arrays.copyOfRange(ref, 20, 30), arrRef);
  }
}
