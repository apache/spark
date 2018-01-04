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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A helper class to write `UTF8String`, `String`, and `byte[]` data into an internal byte buffer
 * and get written data as `UTF8String`.
 */
public class UTF8StringBuilder {

  private static final int ARRAY_MAX = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

  private byte[] buffer;
  private int cursor = Platform.BYTE_ARRAY_OFFSET;

  public UTF8StringBuilder() {
    // Since initial buffer size is 16 in `StringBuilder`, we set the same size here
    this.buffer = new byte[16];
  }

  // Grows the buffer by at least `neededSize`
  private void grow(int neededSize) {
    if (neededSize > ARRAY_MAX - totalSize()) {
      throw new UnsupportedOperationException(
        "Cannot grow internal buffer by size " + neededSize + " because the size after growing " +
          "exceeds size limitation " + ARRAY_MAX);
    }
    final int length = totalSize() + neededSize;
    if (buffer.length < length) {
      int newLength = length < ARRAY_MAX / 2 ? length * 2 : ARRAY_MAX;
      final byte[] tmp = new byte[newLength];
      Platform.copyMemory(
        buffer,
        Platform.BYTE_ARRAY_OFFSET,
        tmp,
        Platform.BYTE_ARRAY_OFFSET,
        totalSize());
      buffer = tmp;
    }
  }

  public void append(UTF8String value) {
    grow(value.numBytes());
    value.writeToMemory(buffer, cursor);
    cursor += value.numBytes();
  }

  public void append(String value) {
    append(value.getBytes(StandardCharsets.UTF_8));
  }

  public void append(byte[] value) {
    grow(value.length);
    Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET, buffer, cursor, value.length);
    cursor += value.length;
  }

  public UTF8String toUTF8String() {
    final int len = totalSize();
    final byte[] bytes = new byte[len];
    Platform.copyMemory(buffer, Platform.BYTE_ARRAY_OFFSET, bytes, Platform.BYTE_ARRAY_OFFSET, len);
    return UTF8String.fromBytes(bytes);
  }

  public int totalSize() {
    return cursor - Platform.BYTE_ARRAY_OFFSET;
  }
}
