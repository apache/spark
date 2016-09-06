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

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import org.apache.spark.network.buffer.netty.ChunkedByteBufImpl;
import org.apache.spark.network.buffer.nio.ChunkedByteBufferImpl;

public class ChunkedByteBufferUtil {
  private static final Logger logger = LoggerFactory.getLogger(ChunkedByteBufferUtil.class);

  public static void dispose(ByteBuffer buffer) {
    if (buffer != null && buffer instanceof MappedByteBuffer) {
      logger.trace("Unmapping" + buffer);
      if (buffer instanceof DirectBuffer) {
        DirectBuffer directBuffer = (DirectBuffer) buffer;
        if (directBuffer.cleaner() != null) directBuffer.cleaner().clean();
      }
    }
  }

  public static ChunkedByteBuffer wrap() {
    return new ChunkedByteBufferImpl();
  }

  public static ChunkedByteBuffer wrap(ByteBuffer chunk) {
    ByteBuffer[] chunks = new ByteBuffer[1];
    chunks[0] = chunk;
    return wrap(chunks);
  }

  public static ChunkedByteBuffer wrap(ByteBuf chunk) {
    ByteBuf[] chunks = new ByteBuf[1];
    chunks[0] = chunk;
    return wrap(chunks);
  }

  public static ChunkedByteBuffer wrap(ByteBuffer[] chunks) {
    return new ChunkedByteBufferImpl(chunks);
  }

  public static ChunkedByteBuffer wrap(ByteBuf[] chunks) {
    return new ChunkedByteBufImpl(chunks);
  }

  public static ChunkedByteBuffer wrap(byte[] array) {
    return wrap(array, 0, array.length);
  }

  public static ChunkedByteBuffer wrap(byte[] array, int offset, int length) {
    return wrap(ByteBuffer.wrap(array, offset, length));
  }

  public static ChunkedByteBuffer wrap(InputStream in, int chunkSize) throws IOException {
    ChunkedByteBufferOutputStream out = ChunkedByteBufferOutputStream.newInstance(chunkSize);
    ByteStreams.copy(in, out);
    out.close();
    return out.toChunkedByteBuffer();
  }

  public static ChunkedByteBuffer wrap(
      DataInput from, int chunkSize, long len) throws IOException {
    ChunkedByteBufferOutputStream out = ChunkedByteBufferOutputStream.newInstance(chunkSize);
    final int BUF_SIZE = Math.min(chunkSize, 4 * 1024);
    byte[] buf = new byte[BUF_SIZE];
    while (len > 0) {
      int r = (int) Math.min(len, BUF_SIZE);
      from.readFully(buf, 0, r);
      out.write(buf, 0, r);
      len -= r;
    }
    out.close();
    return out.toChunkedByteBuffer();
  }

  public static ChunkedByteBuffer wrap(InputStream in) throws IOException {
    return wrap(in, 32 * 1024);
  }
}
