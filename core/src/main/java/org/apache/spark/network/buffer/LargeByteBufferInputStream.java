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

import java.io.InputStream;

import com.google.common.annotations.VisibleForTesting;

/**
 * Reads data from a LargeByteBuffer, and optionally cleans it up using buffer.dispose()
 * when the stream is closed (e.g. to close a memory-mapped file).
 */
public class LargeByteBufferInputStream extends InputStream {

  private LargeByteBuffer buffer;
  private final boolean dispose;

  public LargeByteBufferInputStream(LargeByteBuffer buffer, boolean dispose) {
    this.buffer = buffer;
    this.dispose = dispose;
  }

  public LargeByteBufferInputStream(LargeByteBuffer buffer) {
    this(buffer, false);
  }

  public int read() {
    if (buffer == null || buffer.remaining() == 0) {
      return -1;
    } else {
      return buffer.get() & 0xFF;
    }
  }

  public int read(byte[] dest) {
    return read(dest, 0, dest.length);
  }

  public int read(byte[] dest, int offset, int length) {
    if (buffer == null || buffer.remaining() == 0) {
      return -1;
    } else {
      int amountToGet = (int) Math.min(buffer.remaining(), length);
      buffer.get(dest, offset, amountToGet);
      return amountToGet;
    }
  }

  public long skip(long toSkip) {
    if (buffer != null) {
      return buffer.skip(toSkip);
    } else {
      return 0L;
    }
  }

  // only for testing
  @VisibleForTesting
  boolean disposed = false;

  /**
   * Clean up the buffer, and potentially dispose of it
   */
  public void close() {
    if (buffer != null) {
      if (dispose) {
        buffer.dispose();
        disposed = true;
      }
      buffer = null;
    }
  }
}
