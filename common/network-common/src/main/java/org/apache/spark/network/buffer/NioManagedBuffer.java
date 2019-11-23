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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

/**
 * A {@link ManagedBuffer} backed by {@link ByteBuffer}.
 */
public class NioManagedBuffer extends ManagedBuffer {
  private final ByteBuffer buf;

  public NioManagedBuffer(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public long size() {
    return buf.remaining();
  }

  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    return buf.duplicate();
  }

  @Override
  public InputStream createInputStream() throws IOException {
    return new ByteBufInputStream(Unpooled.wrappedBuffer(buf));
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    return this;
  }

  @Override
  public Object convertToNetty() throws IOException {
    return Unpooled.wrappedBuffer(buf);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("buf", buf)
      .toString();
  }
}

