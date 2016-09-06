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
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

/**
 * A {@link ManagedBuffer} backed by {@link ByteBuffer}.
 */
public class NioManagedBuffer extends ManagedBuffer {
  private final ChunkedByteBuffer buf;

  public NioManagedBuffer(ChunkedByteBuffer buf) {
    this.buf = buf;
  }

  public NioManagedBuffer(ByteBuffer buf) {
    this(ChunkedByteBufferUtil.wrap(buf));
  }

  @Override
  public long size() {
    return buf.size();
  }

  @Override
  public ChunkedByteBuffer nioByteBuffer() throws IOException {
    return buf.retain();
  }

  @Override
  public InputStream createInputStream() throws IOException {
    return buf.toInputStream();
  }

  @Override
  public Object convertToNetty() throws IOException {
    if (size() > Integer.MAX_VALUE - 1024 * 1024) {
      return buf.toInputStream();
    } else {
      return buf.toNetty();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("buf", buf)
      .toString();
  }

  @Override
  protected void deallocate() {
    buf.release();
  }
}

