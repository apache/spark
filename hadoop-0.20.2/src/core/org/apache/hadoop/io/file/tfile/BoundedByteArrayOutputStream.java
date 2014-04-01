/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A byte array backed output stream with a limit. The limit should be smaller
 * than the buffer capacity. The object can be reused through <code>reset</code>
 * API and choose different limits in each round.
 */
class BoundedByteArrayOutputStream extends OutputStream {
  private final byte[] buffer;
  private int limit;
  private int count;

  public BoundedByteArrayOutputStream(int capacity) {
    this(capacity, capacity);
  }

  public BoundedByteArrayOutputStream(int capacity, int limit) {
    if ((capacity < limit) || (capacity | limit) < 0) {
      throw new IllegalArgumentException("Invalid capacity/limit");
    }
    this.buffer = new byte[capacity];
    this.limit = limit;
    this.count = 0;
  }

  @Override
  public void write(int b) throws IOException {
    if (count >= limit) {
      throw new EOFException("Reaching the limit of the buffer.");
    }
    buffer[count++] = (byte) b;
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (count + len > limit) {
      throw new EOFException("Reach the limit of the buffer");
    }

    System.arraycopy(b, off, buffer, count, len);
    count += len;
  }

  public void reset(int newlim) {
    if (newlim > buffer.length) {
      throw new IndexOutOfBoundsException("Limit exceeds buffer size");
    }
    this.limit = newlim;
    this.count = 0;
  }

  public void reset() {
    this.limit = buffer.length;
    this.count = 0;
  }

  public int getLimit() {
    return limit;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public int size() {
    return count;
  }
}
