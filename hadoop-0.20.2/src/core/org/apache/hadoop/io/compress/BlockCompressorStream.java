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

/**
 * A {@link org.apache.hadoop.io.compress.CompressorStream} which works
 * with 'block-based' based compression algorithms, as opposed to 
 * 'stream-based' compression algorithms.
 *
 * It should be noted that this wrapper does not guarantee that blocks will
 * be sized for the compressor. If the
 * {@link org.apache.hadoop.io.compress.Compressor} requires buffering to
 * effect meaningful compression, it is responsible for it.
 */
public class BlockCompressorStream extends CompressorStream {

  // The 'maximum' size of input data to be compressed, to account
  // for the overhead of the compression algorithm.
  private final int MAX_INPUT_SIZE;

  /**
   * Create a {@link BlockCompressorStream}.
   * 
   * @param out stream
   * @param compressor compressor to be used
   * @param bufferSize size of buffer
   * @param compressionOverhead maximum 'overhead' of the compression 
   *                            algorithm with given bufferSize
   */
  public BlockCompressorStream(OutputStream out, Compressor compressor, 
                               int bufferSize, int compressionOverhead) {
    super(out, compressor, bufferSize);
    MAX_INPUT_SIZE = bufferSize - compressionOverhead;
  }

  /**
   * Create a {@link BlockCompressorStream} with given output-stream and 
   * compressor.
   * Use default of 512 as bufferSize and compressionOverhead of 
   * (1% of bufferSize + 12 bytes) =  18 bytes (zlib algorithm).
   * 
   * @param out stream
   * @param compressor compressor to be used
   */
  public BlockCompressorStream(OutputStream out, Compressor compressor) {
    this(out, compressor, 512, 18);
  }

  /**
   * Write the data provided to the compression codec, compressing no more
   * than the buffer size less the compression overhead as specified during
   * construction for each block.
   *
   * Each block contains the uncompressed length for the block, followed by
   * one or more length-prefixed blocks of compressed data.
   */
  public void write(byte[] b, int off, int len) throws IOException {
    // Sanity checks
    if (compressor.finished()) {
      throw new IOException("write beyond end of stream");
    }
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
               ((off + len) > b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    long limlen = compressor.getBytesRead();
    if (len + limlen > MAX_INPUT_SIZE && limlen > 0) {
      // Adding this segment would exceed the maximum size.
      // Flush data if we have it.
      finish();
      compressor.reset();
    }

    if (len > MAX_INPUT_SIZE) {
      // The data we're given exceeds the maximum size. Any data
      // we had have been flushed, so we write out this chunk in segments
      // not exceeding the maximum size until it is exhausted.
      rawWriteInt(len);
      do {
        int bufLen = Math.min(len, MAX_INPUT_SIZE);
        
        compressor.setInput(b, off, bufLen);
        compressor.finish();
        while (!compressor.finished()) {
          compress();
        }
        compressor.reset();
        off += bufLen;
        len -= bufLen;
      } while (len > 0);
      return;
    }

    // Give data to the compressor
    compressor.setInput(b, off, len);
    if (!compressor.needsInput()) {
      // compressor buffer size might be smaller than the maximum
      // size, so we permit it to flush if required.
      rawWriteInt((int)compressor.getBytesRead());
      do {
        compress();
      } while (!compressor.needsInput());
    }
  }

  public void finish() throws IOException {
    if (!compressor.finished()) {
      rawWriteInt((int)compressor.getBytesRead());
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }

  protected void compress() throws IOException {
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      // Write out the compressed chunk
      rawWriteInt(len);
      out.write(buffer, 0, len);
    }
  }
  
  private void rawWriteInt(int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>>  0) & 0xFF);
  }

}
