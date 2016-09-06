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

import java.io.OutputStream;
import com.google.common.base.Preconditions;

import org.apache.spark.network.buffer.netty.ChunkedByteBuffOutputStreamImpl;
import org.apache.spark.network.buffer.nio.ChunkedByteBufferOutputStreamImpl;

public abstract class ChunkedByteBufferOutputStream extends OutputStream {

  protected final boolean isDirect;
  protected final int chunkSize;

  /**
   * An OutputStream that writes to fixed-size chunks of byte arrays.
   *
   * @param chunkSize size of each chunk, in bytes.
   */
  public ChunkedByteBufferOutputStream(int chunkSize, boolean isDirect) {
    this.chunkSize = chunkSize;
    this.isDirect = isDirect;
    Preconditions.checkArgument(chunkSize > 0);
  }

  public abstract long size();

  public abstract ChunkedByteBuffer toChunkedByteBuffer();

  public static ChunkedByteBufferOutputStream newInstance(int chunkSize, boolean isDirect) {
    return new ChunkedByteBuffOutputStreamImpl(chunkSize, isDirect);
  }

  public static ChunkedByteBufferOutputStream newInstance(int chunkSize) {
    return newInstance(chunkSize, false);
  }

  public static ChunkedByteBufferOutputStream newInstance(int chunkSize, Allocator alloc) {
    return new ChunkedByteBufferOutputStreamImpl(chunkSize, false, alloc);
  }

  public static ChunkedByteBufferOutputStream newInstance() {
    return newInstance(4 * 1024);
  }
}
