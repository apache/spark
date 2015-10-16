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

import org.apache.spark.unsafe.Platform;

/**
 * A helper class to manage the row buffer used in `GenerateUnsafeProjection`.
 *
 * Note that it is only used in `GenerateUnsafeProjection`, so it's safe to mark member variables
 * public for ease of use.
 */
public class BufferHolder {
  public byte[] buffer = new byte[64];
  public int cursor = Platform.BYTE_ARRAY_OFFSET;

  public void grow(int neededSize) {
    final int length = totalSize() + neededSize;
    if (buffer.length < length) {
      // This will not happen frequently, because the buffer is re-used.
      final byte[] tmp = new byte[length * 2];
      Platform.copyMemory(
        buffer,
        Platform.BYTE_ARRAY_OFFSET,
        tmp,
        Platform.BYTE_ARRAY_OFFSET,
        totalSize());
      buffer = tmp;
    }
  }

  public void reset() {
    cursor = Platform.BYTE_ARRAY_OFFSET;
  }

  public int totalSize() {
    return cursor - Platform.BYTE_ARRAY_OFFSET;
  }
}
