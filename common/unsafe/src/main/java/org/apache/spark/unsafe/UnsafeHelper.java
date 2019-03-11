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

package org.apache.spark.unsafe;

import java.nio.ByteBuffer;

public final class UnsafeHelper {
  private UnsafeHelper() {}

  public static void writeTo(
      ByteBuffer buffer,
      Object baseObject,
      long baseOffset,
      int sizeInBytes) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(baseObject, baseOffset, target,
        Platform.BYTE_ARRAY_OFFSET + offset + pos, sizeInBytes);
    buffer.position(pos + sizeInBytes);
  }

  public static void writeToMemory(
      Object baseObject,
      long baseOffset,
      Object target,
      long targetOffset,
      int sizeInBytes) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  public static byte[] copyToMemory(Object baseObject, long baseOffset, int sizeInBytes) {
    final byte[] arrayDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(baseObject, baseOffset, arrayDataCopy, Platform.BYTE_ARRAY_OFFSET,
        sizeInBytes);
    return arrayDataCopy;
  }
}
