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

package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

public class CompressorStream extends CompressionOutputStream {
  protected Compressor compressor;
  protected byte[] buffer;
  protected boolean closed = false;
  
  public CompressorStream(OutputStream out, Compressor compressor, int bufferSize) {
    super(out);

    if (out == null || compressor == null) {
      throw new NullPointerException();
    } else if (bufferSize <= 0) {
      throw new IllegalArgumentException("Illegal bufferSize");
    }

    this.compressor = compressor;
    buffer = new byte[bufferSize];
  }

  public CompressorStream(OutputStream out, Compressor compressor) {
    this(out, compressor, 512);
  }
  
  /**
   * Allow derived classes to directly set the underlying stream.
   * 
   * @param out Underlying output stream.
   */
  protected CompressorStream(OutputStream out) {
    super(out);
  }

  public void write(byte[] b, int off, int len) throws IOException {
    // Sanity checks
    if (compressor.finished()) {
      throw new IOException("write beyond end of stream");
    }
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    compressor.setInput(b, off, len);
    while (!compressor.needsInput()) {
      compress();
    }
  }

  protected void compress() throws IOException {
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      out.write(buffer, 0, len);
    }
  }

  public void finish() throws IOException {
    if (!compressor.finished()) {
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }

  public void resetState() throws IOException {
    compressor.reset();
  }
  
  public void close() throws IOException {
    if (!closed) {
      finish();
      out.close();
      closed = true;
    }
  }

  private byte[] oneByte = new byte[1];
  public void write(int b) throws IOException {
    oneByte[0] = (byte)(b & 0xff);
    write(oneByte, 0, oneByte.length);
  }

}
