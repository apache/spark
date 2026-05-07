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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.stream.ChunkedStream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

public class EncryptedMessageWithHeaderSuite {

  // Tests the case where the body is an input stream and that we manage the refcounts of the
  // buffer properly
  @Test
  public void testInputStreamBodyFromManagedBuffer() throws Exception {
    byte[] randomData = new byte[128];
    new Random().nextBytes(randomData);
    ByteBuf sourceBuffer = Unpooled.copiedBuffer(randomData);
    InputStream body = new ByteArrayInputStream(sourceBuffer.array());
    ByteBuf header = Unpooled.copyLong(42);

    long expectedHeaderValue = header.getLong(header.readerIndex());
    assertEquals(1, header.refCnt());
    assertEquals(1, sourceBuffer.refCnt());
    ManagedBuffer managedBuf = new NettyManagedBuffer(sourceBuffer);

    EncryptedMessageWithHeader msg = new EncryptedMessageWithHeader(
      managedBuf, header, body, managedBuf.size());
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    // First read should just read the header
    ByteBuf headerResult = msg.readChunk(allocator);
    assertEquals(header.capacity(), headerResult.readableBytes());
    assertEquals(expectedHeaderValue, headerResult.readLong());
    assertEquals(header.capacity(), msg.progress());
    assertFalse(msg.isEndOfInput());

    // Second read should read the body
    ByteBuf bodyResult = msg.readChunk(allocator);
    assertEquals(randomData.length + header.capacity(), msg.progress());
    assertTrue(msg.isEndOfInput());

    // Validate we read it all
    assertEquals(bodyResult.readableBytes(), randomData.length);
    for (int i = 0; i < randomData.length; i++) {
      assertEquals(bodyResult.readByte(), randomData[i]);
    }

    // Closing the message should release the source buffer
    msg.close();
    assertEquals(0, sourceBuffer.refCnt());

    // The header still has a reference we got
    assertEquals(1, header.refCnt());
    headerResult.release();
    assertEquals(0, header.refCnt());
  }

  // Tests the case where the body is a chunked stream and that we are fine when there is no
  // input managed buffer
  @Test
  public void testChunkedStream() throws Exception {
    int bodyLength = 129;
    int chunkSize = 64;
    byte[] randomData = new byte[bodyLength];
    new Random().nextBytes(randomData);
    InputStream inputStream = new ByteArrayInputStream(randomData);
    ChunkedStream body = new ChunkedStream(inputStream, chunkSize);
    ByteBuf header = Unpooled.copyLong(42);

    long expectedHeaderValue = header.getLong(header.readerIndex());
    assertEquals(1, header.refCnt());

    EncryptedMessageWithHeader msg = new EncryptedMessageWithHeader(null, header, body, bodyLength);
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    // First read should just read the header
    ByteBuf headerResult = msg.readChunk(allocator);
    assertEquals(header.capacity(), headerResult.readableBytes());
    assertEquals(expectedHeaderValue, headerResult.readLong());
    assertEquals(header.capacity(), msg.progress());
    assertFalse(msg.isEndOfInput());

    // Next 2 reads should read full buffers
    int readIndex = 0;
    for (int i = 1; i <= 2; i++) {
      ByteBuf bodyResult = msg.readChunk(allocator);
      assertEquals(header.capacity() + (i*chunkSize), msg.progress());
      assertFalse(msg.isEndOfInput());

      // Validate we read data correctly
      assertEquals(bodyResult.readableBytes(), chunkSize);
      assertTrue(bodyResult.readableBytes() < (randomData.length - readIndex));
      while (bodyResult.readableBytes() > 0) {
        assertEquals(bodyResult.readByte(), randomData[readIndex++]);
      }
    }

    // Last read should be partial
    ByteBuf bodyResult = msg.readChunk(allocator);
    assertEquals(header.capacity() + bodyLength, msg.progress());
    assertTrue(msg.isEndOfInput());

    // Validate we read the byte properly
    assertEquals(bodyResult.readableBytes(), 1);
    assertEquals(bodyResult.readByte(), randomData[readIndex]);

    // Closing the message should close the input stream
    msg.close();
    assertTrue(body.isEndOfInput());

    // The header still has a reference we got
    assertEquals(1, header.refCnt());
    headerResult.release();
    assertEquals(0, header.refCnt());
  }

  @Test
  public void testByteBufIsNotSupported() throws Exception {
    // Validate that ByteBufs are not supported. This test can be updated
    // when we add support for them
    ByteBuf header = Unpooled.copyLong(42);
    assertThrows(IllegalArgumentException.class, () -> {
      EncryptedMessageWithHeader msg = new EncryptedMessageWithHeader(
        null, header, header, 4);
    });
  }
}
