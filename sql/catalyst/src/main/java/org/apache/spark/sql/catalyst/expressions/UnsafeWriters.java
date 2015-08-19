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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A set of helper methods to write data into the variable length portion.
 */
public class UnsafeWriters {
  public static void writeToMemory(
      Object inputObject,
      long inputOffset,
      Object targetObject,
      long targetOffset,
      int numBytes) {

    // zero-out the padding bytes
//    if ((numBytes & 0x07) > 0) {
//      Platform.putLong(targetObject, targetOffset + ((numBytes >> 3) << 3), 0L);
//    }

    // Write the UnsafeData to the target memory.
    Platform.copyMemory(inputObject, inputOffset, targetObject, targetOffset, numBytes);
  }

  public static int getRoundedSize(int size) {
    //return ByteArrayMethods.roundNumberOfBytesToNearestWord(size);
    // todo: do word alignment
    return size;
  }

  /** Writer for Decimal with precision larger than 18. */
  public static class DecimalWriter {

    public static int getSize(Decimal input) {
      return 16;
    }

    public static int write(Object targetObject, long targetOffset, Decimal input) {
      final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
      final int numBytes = bytes.length;
      assert(numBytes <= 16);

      // zero-out the bytes
      Platform.putLong(targetObject, targetOffset, 0L);
      Platform.putLong(targetObject, targetOffset + 8, 0L);

      // Write the bytes to the variable length portion.
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, targetObject, targetOffset, numBytes);
      return 16;
    }
  }

  /** Writer for UTF8String. */
  public static class UTF8StringWriter {

    public static int getSize(UTF8String input) {
      return getRoundedSize(input.numBytes());
    }

    public static int write(Object targetObject, long targetOffset, UTF8String input) {
      final int numBytes = input.numBytes();

      // Write the bytes to the variable length portion.
      writeToMemory(input.getBaseObject(), input.getBaseOffset(),
        targetObject, targetOffset, numBytes);

      return getRoundedSize(numBytes);
    }
  }

  /** Writer for binary (byte array) type. */
  public static class BinaryWriter {

    public static int getSize(byte[] input) {
      return getRoundedSize(input.length);
    }

    public static int write(Object targetObject, long targetOffset, byte[] input) {
      final int numBytes = input.length;

      // Write the bytes to the variable length portion.
      writeToMemory(input, Platform.BYTE_ARRAY_OFFSET, targetObject, targetOffset, numBytes);

      return getRoundedSize(numBytes);
    }
  }

  /** Writer for UnsafeRow. */
  public static class StructWriter {

    public static int getSize(UnsafeRow input) {
      return getRoundedSize(input.getSizeInBytes());
    }

    public static int write(Object targetObject, long targetOffset, UnsafeRow input) {
      final int numBytes = input.getSizeInBytes();

      // Write the bytes to the variable length portion.
      writeToMemory(input.getBaseObject(), input.getBaseOffset(),
        targetObject, targetOffset, numBytes);

      return getRoundedSize(numBytes);
    }
  }

  /** Writer for interval type. */
  public static class IntervalWriter {

    public static int getSize(UnsafeRow input) {
      return 16;
    }

    public static int write(Object targetObject, long targetOffset, CalendarInterval input) {
      // Write the months and microseconds fields of Interval to the variable length portion.
      Platform.putLong(targetObject, targetOffset, input.months);
      Platform.putLong(targetObject, targetOffset + 8, input.microseconds);
      return 16;
    }
  }

  /** Writer for UnsafeArrayData. */
  public static class ArrayWriter {

    public static int getSize(UnsafeArrayData input) {
      // we need extra 4 bytes the store the number of elements in this array.
      return getRoundedSize(input.getSizeInBytes() + 4);
    }

    public static int write(Object targetObject, long targetOffset, UnsafeArrayData input) {
      final int numBytes = input.getSizeInBytes();

      // write the number of elements into first 4 bytes.
      Platform.putInt(targetObject, targetOffset, input.numElements());

      // Write the bytes to the variable length portion.
      writeToMemory(
        input.getBaseObject(), input.getBaseOffset(), targetObject, targetOffset + 4, numBytes);

      return getRoundedSize(numBytes + 4);
    }
  }

  public static class MapWriter {

    public static int getSize(UnsafeMapData input) {
      // we need extra 8 bytes to store number of elements and numBytes of key array.
      return getRoundedSize(4 + 4 + input.getSizeInBytes());
    }

    public static int write(Object targetObject, long targetOffset, UnsafeMapData input) {
      final UnsafeArrayData keyArray = input.keys;
      final UnsafeArrayData valueArray = input.values;
      final int keysNumBytes = keyArray.getSizeInBytes();
      final int valuesNumBytes = valueArray.getSizeInBytes();
      final int numBytes = 4 + 4 + keysNumBytes + valuesNumBytes;

      // write the number of elements into first 4 bytes.
      Platform.putInt(targetObject, targetOffset, input.numElements());
      // write the numBytes of key array into second 4 bytes.
      Platform.putInt(targetObject, targetOffset + 4, keysNumBytes);

      // Write the bytes of key array to the variable length portion.
      writeToMemory(keyArray.getBaseObject(), keyArray.getBaseOffset(),
        targetObject, targetOffset + 8, keysNumBytes);

      // Write the bytes of value array to the variable length portion.
      writeToMemory(valueArray.getBaseObject(), valueArray.getBaseOffset(),
        targetObject, targetOffset + 8 + keysNumBytes, valuesNumBytes);

      return getRoundedSize(numBytes);
    }
  }
}
