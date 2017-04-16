/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import java.io.*;

/** A reusable {@link OutputStream} implementation that writes to an in-memory
 * buffer.
 *
 * <p>This saves memory over creating a new OutputStream and
 * ByteArrayOutputStream each time data is written.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * OutputBuffer buffer = new OutputBuffer();
 * while (... loop condition ...) {
 *   buffer.reset();
 *   ... write buffer using OutputStream methods ...
 *   byte[] data = buffer.getData();
 *   int dataLength = buffer.getLength();
 *   ... write data to its ultimate destination ...
 * }
 * </pre>
 * @see DataOutputBuffer
 * @see InputBuffer
 */
public class OutputBuffer extends FilterOutputStream {

  private static class Buffer extends ByteArrayOutputStream {
    public byte[] getData() { return buf; }
    public int getLength() { return count; }
    public void reset() { count = 0; }

    public void write(InputStream in, int len) throws IOException {
      int newcount = count + len;
      if (newcount > buf.length) {
        byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }
      IOUtils.readFully(in, buf, count, len);
      count = newcount;
    }
  }

  private Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public OutputBuffer() {
    this(new Buffer());
  }
  
  private OutputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Returns the current contents of the buffer.
   *  Data is only valid to {@link #getLength()}.
   */
  public byte[] getData() { return buffer.getData(); }

  /** Returns the length of the valid data currently in the buffer. */
  public int getLength() { return buffer.getLength(); }

  /** Resets the buffer to empty. */
  public OutputBuffer reset() {
    buffer.reset();
    return this;
  }

  /** Writes bytes from a InputStream directly into the buffer. */
  public void write(InputStream in, int length) throws IOException {
    buffer.write(in, length);
  }
}
