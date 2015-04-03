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
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.*;

public class WrappedLargeByteBufferSuite {

  byte[] data = new byte[500];
  {
    new Random().nextBytes(data);
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
    LargeByteBuffer multiBuf = new WrappedLargeByteBuffer(bufs);
    try {
      multiBuf.asByteBuffer();
      fail("expected an exception");
    } catch (BufferTooLargeException btl) {
    }
  }

  @Test
  public void position() {
    WrappedLargeByteBuffer buf = testDataBuf();
    //intentionally move around sporadically
    for(int p: new int[]{10,475, 0, 19, 58, 499, 498, 32, 234, 378}) {
      checkBytesAt(buf, p);
    }
  }

  private void checkBytesAt(WrappedLargeByteBuffer buf, int position) {
    buf.position(position);
    assertEquals(position, buf.position());
    int remaining = 500 - position;
    assertEquals(remaining, buf.remaining());
    byte[] dataCopy = new byte[remaining];
    System.arraycopy(data, position, dataCopy, 0, remaining);
    byte[] bufCopy = new byte[remaining];
    buf.get(bufCopy, 0, remaining);
    assertArrayEquals(dataCopy, bufCopy);
    buf.position(position); //go back to this position for the next set of tests
  }

  @Test
  public void putLargeByteBuffer() {
    //copy from smaller chunks into larger ones

    ByteBuffer[] to = new ByteBuffer[2];
    for (int i = 0; i < 2; i++) {
      to[i] = ByteBuffer.wrap(new byte[300]);
    }
    LargeByteBuffer fromBuf = testDataBuf();
    LargeByteBuffer toBuf = new WrappedLargeByteBuffer(to);

    toBuf.put(fromBuf);

    assertEquals(fromBuf.size(), toBuf.position());
    toBuf.position(0L);
    byte[] toDataCopy = new byte[500];
    toBuf.get(toDataCopy, 0, 500);
    assertArrayEquals(data, toDataCopy);
  }

  @Test
  public void putLargeByteBufferException() {
    WrappedLargeByteBuffer dataBuf = testDataBuf();
    ByteBuffer[] to = new ByteBuffer[2];
    for (int i = 0; i < 2; i++) {
      to[i] = ByteBuffer.wrap(new byte[300]);
    }
    LargeByteBuffer toBuf = new WrappedLargeByteBuffer(to);
    toBuf.put(dataBuf);
    dataBuf.position(0);
    try {
      toBuf.put(dataBuf);
      fail("expected an exception");
    } catch (IllegalArgumentException iae) {
    }
  }
}
