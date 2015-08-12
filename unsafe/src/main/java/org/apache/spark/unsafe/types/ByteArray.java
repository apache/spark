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

package org.apache.spark.unsafe.types;

import org.apache.spark.unsafe.Platform;

import static org.apache.spark.unsafe.PlatformDependent.*;

public class ByteArray {

  /**
   * Writes the content of a byte array into a memory address, identified by an object and an
   * offset. The target memory address must already been allocated, and have enough space to
   * hold all the bytes in this string.
   */
  public static void writeToMemory(byte[] src, Object target, long targetOffset) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET, target, targetOffset, src.length);
  }

  /**
   * Concatenates input multiple binary together into a single binary.
   * Returns null if any input is null.
   */
  public static byte[] concat(byte[]... inputs) {
    // Compute the total length of the result.
    int totalLength = 0;
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        totalLength += inputs[i].length;
      } else {
        return null;
      }
    }

    // Allocate a new byte array, and copy the inputs one by one into it.
    final byte[] result = new byte[totalLength];
    int offset = 0;
    for (int i = 0; i < inputs.length; i++) {
      int len = inputs[i].length;
      copyMemory(
        inputs[i], BYTE_ARRAY_OFFSET,
        result, BYTE_ARRAY_OFFSET + offset,
        len);
      offset += len;
    }
    return result;
  }
}
