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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.util.io.ByteArrayChunkOutputStream;

/**
 * An OutputStream that will write all data to memory.  It supports writing over 2GB
 * and the resulting data can be retrieved as a
 * {@link org.apache.spark.network.buffer.LargeByteBuffer}
 */
public class LargeByteBufferOutputStream extends OutputStream {

  private final ByteArrayChunkOutputStream output;

  /**
   * Create a new LargeByteBufferOutputStream which writes to byte arrays of the given size.  Note
   * that <code>chunkSize</code> has <b>no effect</b> on the LargeByteBuffer returned by
   * {@link #largeBuffer()}.
   *
   * @param chunkSize size of the byte arrays used by this output stream, in bytes
   */
  public LargeByteBufferOutputStream(int chunkSize) {
    output = new ByteArrayChunkOutputStream(chunkSize);
  }

  public void write(int b) {
    output.write(b);
  }

  public void write(byte[] bytes, int off, int len) {
    output.write(bytes, off, len);
  }

  /**
   * Get all of the data written to the stream so far as a LargeByteBuffer.  This method can be
   * called multiple times, and each returned buffer will be completely independent (the data
   * is copied for each returned buffer).  It does not close the stream.
   *
   * @return the data written to the stream as a LargeByteBuffer
   */
  public LargeByteBuffer largeBuffer() {
    return largeBuffer(LargeByteBufferHelper.MAX_CHUNK_SIZE);
  }

  /**
   * exposed for testing.  You don't really ever want to call this method -- the returned
   * buffer will not implement {{asByteBuffer}} correctly.
   */
  @VisibleForTesting
  LargeByteBuffer largeBuffer(int maxChunk) {
    long totalSize = output.size();
    int chunksNeeded = (int) ((totalSize + maxChunk - 1) / maxChunk);
    ByteBuffer[] chunks = new ByteBuffer[chunksNeeded];
    long remaining = totalSize;
    long pos = 0;
    for (int idx = 0; idx < chunksNeeded; idx++) {
      int nextSize = (int) Math.min(maxChunk, remaining);
      chunks[idx] = ByteBuffer.wrap(output.slice(pos, pos + nextSize));
      pos += nextSize;
      remaining -= nextSize;
    }
    return new WrappedLargeByteBuffer(chunks, maxChunk);
  }

  public void close() throws IOException {
    output.close();
  }
}
