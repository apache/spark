/*
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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream; 
import java.io.FilterOutputStream;

import org.apache.hadoop.util.DataChecksum;
/**
 * A Checksum output stream.
 * Checksum for the contents of the file is calculated and
 * appended to the end of the file on close of the stream.
 * Used for IFiles
 */
class IFileOutputStream extends FilterOutputStream {
  /**
   * The output stream to be checksummed. 
   */
  private final DataChecksum sum;
  private byte[] barray;
  private boolean closed = false;
  private boolean finished = false;

  /**
   * Create a checksum output stream that writes
   * the bytes to the given stream.
   * @param out
   */
  public IFileOutputStream(OutputStream out) {
    super(out);
    sum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
        Integer.MAX_VALUE);
    barray = new byte[sum.getChecksumSize()];
  }
  
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    finish();
    out.close();
  }

  /**
   * Finishes writing data to the output stream, by writing
   * the checksum bytes to the end. The underlying stream is not closed.
   * @throws IOException
   */
  public void finish() throws IOException {
    if (finished) {
      return;
    }
    finished = true;
    sum.writeValue(barray, 0, false);
    out.write (barray, 0, sum.getChecksumSize());
    out.flush();
  }

  /**
   * Write bytes to the stream.
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    sum.update(b, off,len);
    out.write(b,off,len);
  }
 
  @Override
  public void write(int b) throws IOException {
    barray[0] = (byte) (b & 0xFF);
    write(barray,0,1);
  }

}
