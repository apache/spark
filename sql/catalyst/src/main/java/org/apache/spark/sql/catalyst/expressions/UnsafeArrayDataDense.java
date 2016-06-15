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
 * An Unsafe implementation of Array which is backed by raw memory instead of Java objects.
 * This array assumes that each element is non-null
 *
 * Each tuple has three parts: [numElements] [0] [values]
 *
 * The `numElements` is 4 bytes storing the number of elements of this array.
 *
 * [0] is a 4-byte identifier for a dense array
 *
 * In the `values` region, we store the content of elements. As we can get length info, so elements
 * can be variable-length.
 *
 * Instances of `UnsafeArrayDataDense` act as pointers to row data stored in this format.
 */
public final class UnsafeArrayDataDense extends UnsafeArrayData {

  protected UnsafeArrayDataDense() {
    format = Format.Dense;
  }

  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    // Read the number of elements from the first 4 bytes.
    final int numElements = Platform.getInt(baseObject, baseOffset);
    assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";
    assert(Platform.getInt(baseObject, baseOffset + 4) == 0);

    this.numElements = numElements;
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  private void assertIndexIsValid(int ordinal) {
    assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
    assert ordinal < numElements : "ordinal (" + ordinal + ") should < " + numElements;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return false;
  }

  @Override
  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getBoolean(baseObject, baseOffset + 8 + ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getByte(baseObject, baseOffset + 8 + ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getShort(baseObject, baseOffset + 8 + ordinal * 2);
  }

  @Override
  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getInt(baseObject, baseOffset + 8 + ordinal * 4);
  }

  @Override
  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getLong(baseObject, baseOffset + 8 + ordinal * 8);
  }

  @Override
  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getFloat(baseObject, baseOffset + 8 + ordinal * 4);
  }

  @Override
  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getDouble(baseObject, baseOffset + 8 + ordinal * 8);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    throw new UnsupportedOperationException(
            "Not support Decimal type in UnsafeArrayDataDense");
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    throw new UnsupportedOperationException(
            "Not support UTF8String type in UnsafeArrayDataDense");
  }

  @Override
  public byte[] getBinary(int ordinal) {
    throw new UnsupportedOperationException(
            "Not support Binary type in UnsafeArrayDataDense");
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    throw new UnsupportedOperationException(
            "Not support Interval type in UnsafeArrayDataDense");
  }

  @Override
  public UnsafeRow getStruct(int ordinal, int numFields) {
    throw new UnsupportedOperationException(
            "Not support Struct type in UnsafeArrayDataDense");
  }

  @Override
  public UnsafeArrayData getArray(int ordinal) {
    throw new UnsupportedOperationException(
            "Not support Array type in UnsafeArrayDataDense");
  }

  @Override
  public UnsafeMapData getMap(int ordinal) {
    throw new UnsupportedOperationException(
            "Not support Map type in UnsafeArrayDataDense");
  }

  @Override
  public UnsafeArrayData copy() {
    UnsafeArrayData arrayCopy = UnsafeArrayData.allocate(Format.Dense);
    final byte[] arrayDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
            baseObject, baseOffset, arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    arrayCopy.pointTo(arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return arrayCopy;
  }

  protected static UnsafeArrayData _fromPrimitiveArray(int[] arr) {
    if (arr.length > (Integer.MAX_VALUE - 8) / 4) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
              "it's too big.");
    }

    final int valueRegionSize = 4 * arr.length;
    final int totalSize = 4 + 4 + valueRegionSize;
    final byte[] data = new byte[totalSize];

    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET, arr.length);
    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET + 4, 0);

    Platform.copyMemory(arr, Platform.INT_ARRAY_OFFSET, data,
            Platform.BYTE_ARRAY_OFFSET + 8, valueRegionSize);

    UnsafeArrayData result = UnsafeArrayData.allocate(Format.Dense);
    result.pointTo(data, Platform.BYTE_ARRAY_OFFSET, totalSize);
    return result;
  }

  protected static UnsafeArrayData _fromPrimitiveArray(double[] arr) {
    if (arr.length > (Integer.MAX_VALUE - 4) / 8) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
              "it's too big.");
    }

    final int valueRegionSize = 8 * arr.length;
    final int totalSize = 4 + 4 + valueRegionSize;
    final byte[] data = new byte[totalSize];

    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET, arr.length);
    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET + 4, 0);

    Platform.copyMemory(arr, Platform.DOUBLE_ARRAY_OFFSET, data,
            Platform.BYTE_ARRAY_OFFSET + 8, valueRegionSize);

    UnsafeArrayData result = UnsafeArrayData.allocate(Format.Dense);
    result.pointTo(data, Platform.BYTE_ARRAY_OFFSET, totalSize);
    return result;
  }

  // TODO: add more specialized methods.
}
