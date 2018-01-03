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

package org.apache.spark.sql.catalyst.expressions.codegen;

import java.nio.charset.StandardCharsets;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;

/**
 * A helper class to write array elements into a string buffer using `BufferHolder`.
 */
public class StringWriterBuffer {

  private BufferHolder buffer;

  public StringWriterBuffer() {
    this.buffer = new BufferHolder(new UnsafeRow(1), 256);
  }

  public void reset() {
    buffer.reset();
  }

  public void append(String value) {
    append(value.getBytes(StandardCharsets.UTF_8));
  }

  public void append(byte[] value) {
    final int numBytes = value.length;
    buffer.grow(numBytes);
    Platform.copyMemory(
      value, Platform.BYTE_ARRAY_OFFSET, buffer.buffer, buffer.cursor, numBytes);
    buffer.cursor += numBytes;
  }

  public byte[] getBytes() {
    // Compute a length of strings written in this buffer
    final int strlen = buffer.totalSize() - buffer.fixedSize();
    final byte[] bytes = new byte[strlen];
    Platform.copyMemory(
      buffer.buffer, Platform.BYTE_ARRAY_OFFSET + buffer.fixedSize(),
      bytes, Platform.BYTE_ARRAY_OFFSET, strlen);
    return bytes;
  }
}
