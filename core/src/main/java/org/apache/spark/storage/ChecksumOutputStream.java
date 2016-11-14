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

package org.apache.spark.storage;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.Checksum;

/**
 * A output stream that generate checksum for written data and write the checksum as long at
 * the end of stream.
 */
public class ChecksumOutputStream extends FilterOutputStream {
  private Checksum cksum;
  private boolean closed;

  public ChecksumOutputStream(OutputStream out, Checksum cksum) {
    super(out);
    cksum.reset();
    this.cksum = cksum;
    this.closed = false;
  }

  public void write(int b) throws IOException {
    out.write(b);
    cksum.update(b);
  }

  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
    cksum.update(b, off, len);
  }

  public void close() throws IOException {
    flush();
    if (!closed) {
      closed = true;
      ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
      buffer.putLong(cksum.getValue());
      out.write(buffer.array());
      out.close();
    }
  }
}
