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

package org.apache.spark.network;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import org.apache.spark.network.buffer.ChunkedByteBuffer;
import org.apache.spark.network.buffer.ChunkedByteBufferUtil;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;

/**
 * A ManagedBuffer implementation that contains 0, 1, 2, 3, ..., (len-1).
 *
 * Used for testing.
 */
public class TestManagedBuffer extends ManagedBuffer {

  private final int len;
  private NioManagedBuffer underlying;

  public TestManagedBuffer(int len) {
    Preconditions.checkArgument(len <= Byte.MAX_VALUE);
    this.len = len;
    byte[] byteArray = new byte[len];
    for (int i = 0; i < len; i ++) {
      byteArray[i] = (byte) i;
    }
    this.underlying = new NioManagedBuffer(ChunkedByteBufferUtil.wrap(byteArray));
  }


  @Override
  public long size() {
    return underlying.size();
  }

  @Override
  public ChunkedByteBuffer nioByteBuffer() throws IOException {
    return underlying.nioByteBuffer();
  }

  @Override
  public InputStream createInputStream() throws IOException {
    return underlying.createInputStream();
  }

  @Override
  public int refCnt() {
    return underlying.refCnt();
  }

  @Override
  public ManagedBuffer retain() {
    underlying.retain();
    return this;
  }

  @Override
  public boolean release() {
    return underlying.release();
  }

  @Override
  public Object convertToNetty() throws IOException {
    return underlying.convertToNetty();
  }

  @Override
  public int hashCode() {
    return underlying.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ManagedBuffer) {
      try {
        ByteBuffer nioBuf = ((ManagedBuffer) other).nioByteBuffer().toByteBuffer();
        if (nioBuf.remaining() != len) {
          return false;
        } else {
          for (int i = 0; i < len; i ++) {
            if (nioBuf.get() != i) {
              return false;
            }
          }
          return true;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }
}
