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

import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;

public final class UnsafeHelper {
  private UnsafeHelper() {}

  public static byte[] getBinary(long offsetAndSize, Object baseObject, long baseOffset) {
    final int offset = getOffsetFromOffsetAndSize(offsetAndSize);
    final int size = getSizeFromOffsetAndSize(offsetAndSize);
    final byte[] bytes = new byte[size];
    Platform.copyMemory(baseObject, baseOffset + offset, bytes, Platform.BYTE_ARRAY_OFFSET, size);
    return bytes;
  }

  public static UTF8String getUTF8String(long offsetAndSize, Object baseObject, long baseOffset) {
    final int offset = getOffsetFromOffsetAndSize(offsetAndSize);
    final int size = getSizeFromOffsetAndSize(offsetAndSize);
    return UTF8String.fromAddress(baseObject, baseOffset + offset, size);
  }

  public static CalendarInterval getInterval(
      long offsetAndSize,
      Object baseObject,
      long baseOffset) {
    final int offset = getOffsetFromOffsetAndSize(offsetAndSize);
    final int months = (int) Platform.getLong(baseObject, baseOffset + offset);
    final long microseconds = Platform.getLong(baseObject, baseOffset + offset + 8);
    return new CalendarInterval(months, microseconds);
  }

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

  public static int getOffsetFromOffsetAndSize(long offsetAndSize) {
    return (int) (offsetAndSize >> 32);
  }

  public static int getSizeFromOffsetAndSize(long offsetAndSize) {
    return (int) offsetAndSize;
  }
}
