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
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;

import com.google.common.primitives.UnsignedBytes;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBufInputStream extends InputStream {
  // private final Logger logger = LoggerFactory.getLogger(ByteBufInputStream.class);

  private final boolean dispose;
  private final LinkedList<ByteBuf> buffers;
  private ByteBuf curChunk;
  private boolean isClosed = false;

  public ByteBufInputStream(LinkedList<ByteBuf> buffers) {
    this(buffers, true);
  }

  public ByteBufInputStream(LinkedList<ByteBuf> buffers, boolean dispose) {
    this.buffers = buffers;
    this.dispose = dispose;
  }

  @Override
  public int read() throws IOException {
    pullChunk();
    if (curChunk != null) {
      byte b = curChunk.readByte();
      maybeReleaseCurChunk();
      return UnsignedBytes.toInt(b);
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] dest, int offset, int length) throws IOException {
    pullChunk();
    if (curChunk != null) {
      int amountToGet = Math.min(curChunk.readableBytes(), length);
      curChunk.readBytes(dest, offset, amountToGet);
      maybeReleaseCurChunk();
      return amountToGet;
    } else {
      return -1;
    }
  }

  @Override
  public long skip(long bytes) throws IOException {
    pullChunk();
    if (curChunk != null) {
      int amountToSkip = (int) Math.min(bytes, curChunk.readableBytes());
      curChunk.skipBytes(amountToSkip);
      maybeReleaseCurChunk();
      return amountToSkip;
    } else {
      return 0L;
    }
  }

  @Override
  public void close() throws IOException {
    if (isClosed) return;
    isClosed = true;
    releaseCurChunk();
    if (dispose) {
      while (buffers.size() > 0) {
        buffers.removeFirst().release();
      }
    } else {
      buffers.clear();
    }
  }

  private void pullChunk() throws IOException {
    if (curChunk == null && buffers.size() > 0) {
      curChunk = buffers.removeFirst();
    }
  }

  private void maybeReleaseCurChunk() {
    if (curChunk != null && !curChunk.isReadable()) releaseCurChunk();
  }

  private void releaseCurChunk() {
    if (curChunk != null) {
      if (dispose) curChunk.release();
      curChunk = null;
    }
  }
}
