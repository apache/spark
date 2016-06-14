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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * An Unsafe implementation of Array which is backed by raw memory instead of Java objects.
 *
 * Each tuple has three parts: [numElements] [offsets] [values]
 *
 * The `numElements` is 4 bytes storing the number of elements of this array.
 *
 * In the `offsets` region, we store 4 bytes per element, represents the relative offset (w.r.t. the
 * base address of the array) of this element in `values` region. We can get the length of this
 * element by subtracting next offset.
 * Note that offset can by negative which means this element is null.
 *
 * In the `values` region, we store the content of elements. As we can get length info, so elements
 * can be variable-length.
 *
 * Instances of `UnsafeArrayDataSparse` act as pointers to row data stored in this format.
 */
public final class UnsafeArrayDataSparse extends UnsafeArrayData {

  protected UnsafeArrayDataSparse() {
    format = Format.Sparse;
  }

  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    // Read the number of elements from the first 4 bytes.
    final int numElements = Platform.getInt(baseObject, baseOffset);
    assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";
    assert(Platform.getInt(baseObject, baseOffset + 4) != 0);

    this.numElements = numElements;
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  private int getElementOffset(int ordinal) {
    return Platform.getInt(baseObject, baseOffset + 4 + ordinal * 4L);
  }

  private int getElementSize(int offset, int ordinal) {
    if (ordinal == numElements - 1) {
      return sizeInBytes - offset;
    } else {
      return Math.abs(getElementOffset(ordinal + 1)) - offset;
    }
  }

  private void assertIndexIsValid(int ordinal) {
    assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
    assert ordinal < numElements : "ordinal (" + ordinal + ") should < " + numElements;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return getElementOffset(ordinal) < 0;
  }

  @Override
  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return false;
    return Platform.getBoolean(baseObject, baseOffset + offset);
  }

  @Override
  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return 0;
    return Platform.getByte(baseObject, baseOffset + offset);
  }

  @Override
  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return 0;
    return Platform.getShort(baseObject, baseOffset + offset);
  }

  @Override
  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return 0;
    return Platform.getInt(baseObject, baseOffset + offset);
  }

  @Override
  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return 0;
    return Platform.getLong(baseObject, baseOffset + offset);
  }

  @Override
  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return 0;
    return Platform.getFloat(baseObject, baseOffset + offset);
  }

  @Override
  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return 0;
    return Platform.getDouble(baseObject, baseOffset + offset);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;

    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      final long value = Platform.getLong(baseObject, baseOffset + offset);
      return Decimal.apply(value, precision, scale);
    } else {
      final byte[] bytes = getBinary(ordinal);
      final BigInteger bigInteger = new BigInteger(bytes);
      final BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(new scala.math.BigDecimal(javaDecimal), precision, scale);
    }
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int size = getElementSize(offset, ordinal);
    return UTF8String.fromAddress(baseObject, baseOffset + offset, size);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int size = getElementSize(offset, ordinal);
    final byte[] bytes = new byte[size];
    Platform.copyMemory(baseObject, baseOffset + offset, bytes, Platform.BYTE_ARRAY_OFFSET, size);
    return bytes;
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int months = (int) Platform.getLong(baseObject, baseOffset + offset);
    final long microseconds = Platform.getLong(baseObject, baseOffset + offset + 8);
    return new CalendarInterval(months, microseconds);
  }

  @Override
  public UnsafeRow getStruct(int ordinal, int numFields) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int size = getElementSize(offset, ordinal);
    final UnsafeRow row = new UnsafeRow(numFields);
    row.pointTo(baseObject, baseOffset + offset, size);
    return row;
  }

  @Override
  public UnsafeArrayData getArray(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int size = getElementSize(offset, ordinal);
    final UnsafeArrayData array = UnsafeArrayData.allocate(Format.Sparse);
    array.pointTo(baseObject, baseOffset + offset, size);
    return array;
  }

  @Override
  public UnsafeMapData getMap(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int size = getElementSize(offset, ordinal);
    final UnsafeMapData map = new UnsafeMapData();
    map.pointTo(baseObject, baseOffset + offset, size);
    return map;
  }

  @Override
  public UnsafeArrayData copy() {
    UnsafeArrayData arrayCopy = UnsafeArrayData.allocate(Format.Sparse);
    final byte[] arrayDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
            baseObject, baseOffset, arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    arrayCopy.pointTo(arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return arrayCopy;
  }

  protected static UnsafeArrayData _fromPrimitiveArray(int[] arr) {
    if (arr.length > (Integer.MAX_VALUE - 4) / 8) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
              "it's too big.");
    }

    final int offsetRegionSize = 4 * arr.length;
    final int valueRegionSize = 4 * arr.length;
    final int totalSize = 4 + offsetRegionSize + valueRegionSize;
    final byte[] data = new byte[totalSize];

    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET, arr.length);

    int offsetPosition = Platform.BYTE_ARRAY_OFFSET + 4;
    int valueOffset = 4 + offsetRegionSize;
    for (int i = 0; i < arr.length; i++) {
      Platform.putInt(data, offsetPosition, valueOffset);
      offsetPosition += 4;
      valueOffset += 4;
    }

    Platform.copyMemory(arr, Platform.INT_ARRAY_OFFSET, data,
            Platform.BYTE_ARRAY_OFFSET + 4 + offsetRegionSize, valueRegionSize);

    UnsafeArrayData result = UnsafeArrayData.allocate(Format.Sparse);
    result.pointTo(data, Platform.BYTE_ARRAY_OFFSET, totalSize);
    return result;
  }

  protected static UnsafeArrayData _fromPrimitiveArray(double[] arr) {
    if (arr.length > (Integer.MAX_VALUE - 4) / 12) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
              "it's too big.");
    }

    final int offsetRegionSize = 4 * arr.length;
    final int valueRegionSize = 8 * arr.length;
    final int totalSize = 4 + offsetRegionSize + valueRegionSize;
    final byte[] data = new byte[totalSize];

    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET, arr.length);

    int offsetPosition = Platform.BYTE_ARRAY_OFFSET + 4;
    int valueOffset = 4 + offsetRegionSize;
    for (int i = 0; i < arr.length; i++) {
      Platform.putInt(data, offsetPosition, valueOffset);
      offsetPosition += 4;
      valueOffset += 8;
    }

    Platform.copyMemory(arr, Platform.DOUBLE_ARRAY_OFFSET, data,
            Platform.BYTE_ARRAY_OFFSET + 4 + offsetRegionSize, valueRegionSize);

    UnsafeArrayData result = UnsafeArrayData.allocate(Format.Sparse);
    result.pointTo(data, Platform.BYTE_ARRAY_OFFSET, totalSize);
    return result;
  }

  // TODO: add more specialized methods.
}
