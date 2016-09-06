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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;

import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.buffer.*;
import org.junit.Test;

import static org.junit.Assert.*;

public class ByteBufInputStreamSuite {
  private final Random rad = new Random();

  @Test
  public void multipleChunkOutputWithDispose() throws Exception {
    byte[] bytes1 = new byte[10];
    byte[] bytes2 = new byte[10];
    byte[] bytes3 = new byte[10];
    rad.nextBytes(bytes1);
    rad.nextBytes(bytes2);
    rad.nextBytes(bytes3);
    ByteBuf byteBuf1 = Unpooled.wrappedBuffer(bytes1);
    ByteBuf byteBuf2 = Unpooled.wrappedBuffer(bytes2);
    ByteBuf byteBuf3 = Unpooled.wrappedBuffer(bytes3);
    LinkedList<ByteBuf> list = new LinkedList<>();
    list.add(byteBuf1);
    list.add(byteBuf2);
    list.add(byteBuf3);

    ByteBufInputStream in = new ByteBufInputStream(list, false);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteStreams.copy(in, out);
    byte[] byteOut = out.toByteArray();

    assertEquals(byteOut.length, 30);
    assertArrayEquals(Arrays.copyOfRange(byteOut, 0, 10), bytes1);
    assertArrayEquals(Arrays.copyOfRange(byteOut, 10, 20), bytes2);
    assertArrayEquals(Arrays.copyOfRange(byteOut, 20, 30), bytes3);
    assertEquals(byteBuf1.refCnt(), 1);
  }

  @Test
  public void multipleChunkOutputWithoutDispose() throws Exception {
    byte[] bytes1 = new byte[10];
    byte[] bytes2 = new byte[10];
    byte[] bytes3 = new byte[10];
    rad.nextBytes(bytes1);
    rad.nextBytes(bytes2);
    rad.nextBytes(bytes3);
    ByteBuf byteBuf1 = Unpooled.wrappedBuffer(bytes1);
    ByteBuf byteBuf2 = Unpooled.wrappedBuffer(bytes2);
    ByteBuf byteBuf3 = Unpooled.wrappedBuffer(bytes3);
    LinkedList<ByteBuf> list = new LinkedList<>();
    list.add(byteBuf1);
    list.add(byteBuf2);
    list.add(byteBuf3);

    ByteBufInputStream in = new ByteBufInputStream(list, true);
    ChunkedByteBufferOutputStream out = ChunkedByteBufferOutputStream.newInstance(10);
    ByteStreams.copy(in, out);

    ByteBuffer[] buffers = out.toChunkedByteBuffer().toByteBuffers();
    assertEquals(buffers.length, 3);

    byte[] ref = new byte[10];

    buffers[0].get(ref);
    assertArrayEquals(ref, bytes1);

    buffers[1].get(ref);
    assertArrayEquals(ref, bytes2);

    buffers[2].get(ref);
    assertArrayEquals(ref, bytes3);
    assertEquals(byteBuf1.refCnt(), 0);
  }
}
