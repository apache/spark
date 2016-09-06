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

package org.apache.spark.network.buffer.nio;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import com.google.common.primitives.UnsignedBytes;

import org.apache.spark.network.buffer.ChunkedByteBuffer;
import org.apache.spark.network.buffer.ChunkedByteBufferUtil;

public  class ChunkedByteBufferInputStream extends InputStream {

  private ChunkedByteBuffer chunkedByteBuffer;
  private boolean dispose;
  private Iterator<ByteBuffer> chunks;
  private ByteBuffer currentChunk;

  /**
   * Reads data from a ChunkedByteBuffer.
   *
   * @param dispose if true, [[ChunkedByteBuffer.dispose()]] will be called at the end of the stream
   *                in order to close any memory-mapped files which back the buffer.
   */
  public ChunkedByteBufferInputStream(ChunkedByteBuffer chunkedByteBuffer, boolean dispose) {
    this.chunkedByteBuffer = chunkedByteBuffer;
    this.dispose = dispose;
    this.chunks = Arrays.asList(chunkedByteBuffer.toByteBuffers()).iterator();
    if (chunks.hasNext()) {
      currentChunk = chunks.next();
    } else {
      currentChunk = null;
    }
  }

  public int read() throws IOException {
    if (currentChunk != null && !currentChunk.hasRemaining() && chunks.hasNext()) {
      currentChunk = chunks.next();
    }
    if (currentChunk != null && currentChunk.hasRemaining()) {
      return UnsignedBytes.toInt(currentChunk.get());
    } else {
      close();
      return -1;
    }
  }

  public int read(byte[] dest, int offset, int length) throws IOException {
    if (currentChunk != null && !currentChunk.hasRemaining() && chunks.hasNext()) {
      currentChunk = chunks.next();
    }
    if (currentChunk != null && currentChunk.hasRemaining()) {
      int amountToGet = Math.min(currentChunk.remaining(), length);
      currentChunk.get(dest, offset, amountToGet);
      return amountToGet;
    } else {
      close();
      return -1;
    }
  }

  public long skip(long bytes) throws IOException {
    if (currentChunk != null) {
      int amountToSkip = (int) Math.min(bytes, currentChunk.remaining());
      currentChunk.position(currentChunk.position() + amountToSkip);
      if (currentChunk.remaining() == 0) {
        if (chunks.hasNext()) {
          currentChunk = chunks.next();
        } else {
          close();
        }
      }
      return amountToSkip;
    } else {
      return 0L;
    }
  }

  public void close() throws IOException {
    if (chunkedByteBuffer != null && dispose) {
      chunkedByteBuffer.release();
    }
    chunkedByteBuffer = null;
    chunks = null;
    currentChunk = null;
  }

  public ChunkedByteBuffer toChunkedByteBuffer() {
    ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();
    if (currentChunk != null && !currentChunk.hasRemaining() && chunks.hasNext()) {
      currentChunk = chunks.next();
    }
    while (currentChunk != null) {
      list.add(currentChunk.slice());
      if (chunks.hasNext()) {
        currentChunk = chunks.next();
      } else {
        currentChunk = null;
      }
    }
    return ChunkedByteBufferUtil.wrap(list.toArray(new ByteBuffer[list.size()]));
  }
}
