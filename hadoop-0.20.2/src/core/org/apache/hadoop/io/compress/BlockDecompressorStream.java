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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link org.apache.hadoop.io.compress.DecompressorStream} which works
 * with 'block-based' based compression algorithms, as opposed to 
 * 'stream-based' compression algorithms.
 *  
 */
public class BlockDecompressorStream extends DecompressorStream {
  private int originalBlockSize = 0;
  private int noUncompressedBytes = 0;

  /**
   * Create a {@link BlockDecompressorStream}.
   * 
   * @param in input stream
   * @param decompressor decompressor to use
   * @param bufferSize size of buffer
 * @throws IOException
   */
  public BlockDecompressorStream(InputStream in, Decompressor decompressor, 
                                 int bufferSize) throws IOException {
    super(in, decompressor, bufferSize);
  }
  
  /**
   * Create a {@link BlockDecompressorStream}.
   * 
   * @param in input stream
   * @param decompressor decompressor to use
 * @throws IOException
   */
  public BlockDecompressorStream(InputStream in, Decompressor decompressor) throws IOException {
    super(in, decompressor);
  }

  protected BlockDecompressorStream(InputStream in) throws IOException {
    super(in);
  }

  protected int decompress(byte[] b, int off, int len) throws IOException {
    // Check if we are the beginning of a block
    if (noUncompressedBytes == originalBlockSize) {
      // Get original data size
      try {
        originalBlockSize =  rawReadInt();
      } catch (IOException ioe) {
        return -1;
      }
      noUncompressedBytes = 0;
      // EOF if originalBlockSize is 0
      // This will occur only when decompressing previous compressed empty file
      if (originalBlockSize == 0) {
        eof = true;
        return -1;
      }
    }
    
    int n = 0;
    while ((n = decompressor.decompress(b, off, len)) == 0) {
      if (decompressor.finished() || decompressor.needsDictionary()) {
        if (noUncompressedBytes >= originalBlockSize) {
          eof = true;
          return -1;
        }
      }
      if (decompressor.needsInput()) {
        getCompressedData();
      }
    }
    
    // Note the no. of decompressed bytes read from 'current' block
    noUncompressedBytes += n;

    return n;
  }

  protected void getCompressedData() throws IOException {
    checkStream();

    // Get the size of the compressed chunk
    int len = rawReadInt();

    // Read len bytes from underlying stream 
    if (len > buffer.length) {
      buffer = new byte[len];
    }
    int n = 0, off = 0;
    while (n < len) {
      int count = in.read(buffer, off + n, len - n);
      if (count < 0) {
        throw new EOFException();
      }
      n += count;
    }
    
    // Send the read data to the decompressor
    decompressor.setInput(buffer, 0, len);
  }

  public void resetState() throws IOException {
    super.resetState();
  }

  private int rawReadInt() throws IOException {
    int b1 = in.read();
    int b2 = in.read();
    int b3 = in.read();
    int b4 = in.read();
    if ((b1 | b2 | b3 | b4) < 0)
      throw new EOFException();
    return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4 << 0));
  }
}
