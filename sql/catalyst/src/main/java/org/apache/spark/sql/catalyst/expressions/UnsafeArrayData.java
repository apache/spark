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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * An Unsafe implementation of Array which is backed by raw memory instead of Java objects.
 *
 * Each tuple has two parts: [offsets] [values]
 *
 * In the `offsets` region, we store 4 bytes per element, represents the start address of this
 * element in `values` region. We can get the length of this element by subtracting next offset.
 * Note that offset can by negative which means this element is null.
 *
 * In the `values` region, we store the content of elements. As we can get length info, so elements
 * can be variable-length.
 *
 * Note that when we write out this array, we should write out the `numElements` at first 4 bytes,
 * then follows content. When we read in an array, we should read first 4 bytes as `numElements`
 * and take the rest as content.
 *
 * Instances of `UnsafeArrayData` act as pointers to row data stored in this format.
 */
// todo: there is a lof of duplicated code between UnsafeRow and UnsafeArrayData.
public class UnsafeArrayData extends ArrayData {

  private Object baseObject;
  private long baseOffset;

  // The number of elements in this array
  private int numElements;

  // The size of this array's backing data, in bytes
  private int sizeInBytes;

  private int getElementOffset(int ordinal) {
    return Platform.getInt(baseObject, baseOffset + ordinal * 4L);
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

  /**
   * Construct a new UnsafeArrayData. The resulting UnsafeArrayData won't be usable until
   * `pointTo()` has been called, since the value returned by this constructor is equivalent
   * to a null pointer.
   */
  public UnsafeArrayData() { }

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  @Override
  public int numElements() { return numElements; }

  /**
   * Update this UnsafeArrayData to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this row's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int numElements, int sizeInBytes) {
    assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";
    this.numElements = numElements;
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return getElementOffset(ordinal) < 0;
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal) || dataType instanceof NullType) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      return getDecimal(ordinal, dt.precision(), dt.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof CalendarIntervalType) {
      return getInterval(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType) dataType).size());
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
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
  public InternalRow getStruct(int ordinal, int numFields) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int size = getElementSize(offset, ordinal);
    final UnsafeRow row = new UnsafeRow();
    row.pointTo(baseObject, baseOffset + offset, numFields, size);
    return row;
  }

  @Override
  public ArrayData getArray(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int size = getElementSize(offset, ordinal);
    return UnsafeReaders.readArray(baseObject, baseOffset + offset, size);
  }

  @Override
  public MapData getMap(int ordinal) {
    assertIndexIsValid(ordinal);
    final int offset = getElementOffset(ordinal);
    if (offset < 0) return null;
    final int size = getElementSize(offset, ordinal);
    return UnsafeReaders.readMap(baseObject, baseOffset + offset, size);
  }

  @Override
  public int hashCode() {
    int result = 37;
    for (int i = 0; i < sizeInBytes; i++) {
      result = 37 * result + Platform.getByte(baseObject, baseOffset + i);
    }
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UnsafeArrayData) {
      UnsafeArrayData o = (UnsafeArrayData) other;
      return (sizeInBytes == o.sizeInBytes) &&
        ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
          sizeInBytes);
    }
    return false;
  }

  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  @Override
  public UnsafeArrayData copy() {
    UnsafeArrayData arrayCopy = new UnsafeArrayData();
    final byte[] arrayDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
      baseObject, baseOffset, arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    arrayCopy.pointTo(arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, numElements, sizeInBytes);
    return arrayCopy;
  }
}
