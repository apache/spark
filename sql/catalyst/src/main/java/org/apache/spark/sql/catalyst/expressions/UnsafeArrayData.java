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
import java.nio.ByteBuffer;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * An Unsafe implementation of Array which is backed by raw memory instead of Java objects.
 *
 * Each tuple has three four: [numElements] [null bits] [values] [variable length portion]
 *
 * The `numElements` is 4 bytes storing the number of elements of this array.
 *
 * In the `null bits` region, we store 1 bit per element, represents whether a element has null
 * Its total size is ceil(numElements / 8) bytes, and  it is aligned to 8-byte word boundaries.
 *
 * In the `offsets` region, we store 4 bytes per element, represents the relative offset (w.r.t. the
 * base address of the array) of this element in `values` region. We can get the length of this
 * element by subtracting next offset.
 * Note that offset can by negative which means this element is null.
 *
 * In the `values` region, we store the content of elements. As we can get length info, so elements
 * can be variable-length.
 *
 */

// todo: there is a lof of duplicated code between UnsafeRow and UnsafeArrayData.
public final class UnsafeArrayData extends ArrayData {

  public static int calculateHeaderPortionInBytes(int numFields) {
    return 4 + ((numFields + 63)/ 64) * 8;
  }

  private Object baseObject;
  private long baseOffset;

  // The number of elements in this array
  private int numElements;

  // The size of this array's backing data, in bytes.
  // The 4-bytes header of `numElements` is also included.
  private int sizeInBytes;

  /** The width of the null tracking bit set, in bytes */
  private int headerInBytes;

  private long getFieldOffset(int ordinal, int scale) {
    return baseOffset + headerInBytes + ordinal * scale;
  }

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  private void assertIndexIsValid(int ordinal) {
    assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
    assert ordinal < numElements : "ordinal (" + ordinal + ") should < " + numElements;
  }

  public Object[] array() {
    throw new UnsupportedOperationException("Not supported on UnsafeArrayData.");
  }

  /**
   * Construct a new UnsafeArrayData. The resulting UnsafeArrayData won't be usable until
   * `pointTo()` has been called, since the value returned by this constructor is equivalent
   * to a null pointer.
   */
  public UnsafeArrayData() {  }

  @Override
  public int numElements() { return numElements; }

  /**
   * Update this UnsafeArrayData to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this array's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    // Read the number of elements from the first 4 bytes.
    final int numElements = Platform.getInt(baseObject, baseOffset);
    assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";

    this.numElements = numElements;
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
    this.headerInBytes = calculateHeaderPortionInBytes(numElements);
  }

  @Override
  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return BitSetMethods.isSet(baseObject, baseOffset + 4, ordinal);
  }

  @Override
  public final Object get(int ordinal, DataType dataType) {
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
    } else if (dataType instanceof UserDefinedType) {
      return get(ordinal, ((UserDefinedType)dataType).sqlType());
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
  }

  @Override
  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getBoolean(baseObject, getFieldOffset(ordinal, 1));
  }

  @Override
  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getByte(baseObject, getFieldOffset(ordinal, 1));
  }

  @Override
  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getShort(baseObject, getFieldOffset(ordinal, 2));
  }

  @Override
  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getInt(baseObject, getFieldOffset(ordinal, 4));
  }

  @Override
  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getLong(baseObject, getFieldOffset(ordinal, 8));
  }

  @Override
  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getFloat(baseObject, getFieldOffset(ordinal, 4));
  }

  @Override
  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getDouble(baseObject, getFieldOffset(ordinal, 8));
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    if (isNullAt(ordinal)) {
      return null;
    }
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.apply(getLong(ordinal), precision, scale);
    } else {
      final byte[] bytes = getBinary(ordinal);
      final BigInteger bigInteger = new BigInteger(bytes);
      final BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(new scala.math.BigDecimal(javaDecimal), precision, scale);
    }
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    return UTF8String.fromAddress(baseObject, baseOffset + offset, size);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final byte[] bytes = new byte[size];
    Platform.copyMemory(baseObject, baseOffset + offset, bytes, Platform.BYTE_ARRAY_OFFSET, size);
    return bytes;
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int months = (int) Platform.getLong(baseObject, baseOffset + offset);
    final long microseconds = Platform.getLong(baseObject, baseOffset + offset + 8);
    return new CalendarInterval(months, microseconds);
  }

  @Override
  public UnsafeRow getStruct(int ordinal, int numFields) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final UnsafeRow row = new UnsafeRow(numFields);
    row.pointTo(baseObject, baseOffset + offset, size);
    return row;
  }

  @Override
  public UnsafeArrayData getArray(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final UnsafeArrayData array = new UnsafeArrayData();
    array.pointTo(baseObject, baseOffset + offset, size);
    return array;
  }

  @Override
  public UnsafeMapData getMap(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final UnsafeMapData map = new UnsafeMapData();
    map.pointTo(baseObject, baseOffset + offset, size);
    return map;
  }

  public final void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  public final void writeTo(ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  // This `hashCode` computation could consume much processor time for large data.
  // If the computation becomes a bottleneck, we can use a light-weight logic; the first fixed bytes
  // are used to compute `hashCode` (See `Vector.hashCode`).
  // The same issue exists in `UnsafeRow.hashCode`.
  @Override
  public UnsafeArrayData copy() {
    UnsafeArrayData arrayCopy = new UnsafeArrayData();
    final byte[] arrayDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
      baseObject, baseOffset, arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    arrayCopy.pointTo(arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return arrayCopy;
  }

  @Override
  public boolean[] toBooleanArray() {
    int size = numElements();
    boolean[] values = new boolean[size];
    Platform.copyMemory(
      baseObject, baseOffset + headerInBytes, values, Platform.BYTE_ARRAY_OFFSET, size);
    return values;
  }

  @Override
  public byte[] toByteArray() {
    int size = numElements();
    byte[] values = new byte[size];
    Platform.copyMemory(
      baseObject, baseOffset + headerInBytes, values, Platform.BYTE_ARRAY_OFFSET, size);
    return values;
  }

  @Override
  public short[] toShortArray() {
    int size = numElements();
    short[] values = new short[size];
    Platform.copyMemory(
      baseObject, baseOffset + headerInBytes, values, Platform.BYTE_ARRAY_OFFSET, size * 2);
    return values;
  }

  @Override
  public int[] toIntArray() {
    int size = numElements();
    int[] values = new int[size];
    Platform.copyMemory(
      baseObject, baseOffset + headerInBytes, values, Platform.BYTE_ARRAY_OFFSET, size * 4);
    return values;
  }

  @Override
  public long[] toLongArray() {
    int size = numElements();
    long[] values = new long[size];
    Platform.copyMemory(
      baseObject, baseOffset + headerInBytes, values, Platform.BYTE_ARRAY_OFFSET, size * 8);
    return values;
  }

  @Override
  public float[] toFloatArray() {
    int size = numElements();
    float[] values = new float[size];
    Platform.copyMemory(
      baseObject, baseOffset + headerInBytes, values, Platform.BYTE_ARRAY_OFFSET, size * 4);
    return values;
  }

  @Override
  public double[] toDoubleArray() {
    int size = numElements();
    double[] values = new double[size];
    Platform.copyMemory(
      baseObject, baseOffset + headerInBytes, values, Platform.BYTE_ARRAY_OFFSET, size * 8);
    return values;
  }

  public static UnsafeArrayData fromPrimitiveArray(int[] arr) {
    final int elementSize = 4;
    final int headerSize = calculateHeaderPortionInBytes(arr.length);
    if (arr.length > (Integer.MAX_VALUE - headerSize) / elementSize) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
              "it's too big.");
    }

    final int valueRegionSize = elementSize * arr.length;
    final byte[] data = new byte[valueRegionSize + headerSize];

    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET, arr.length);
    Platform.copyMemory(arr, Platform.INT_ARRAY_OFFSET, data,
            Platform.BYTE_ARRAY_OFFSET + headerSize, valueRegionSize);

    UnsafeArrayData result = new UnsafeArrayData();
    result.pointTo(data, Platform.BYTE_ARRAY_OFFSET, valueRegionSize + headerSize);
    return result;
  }

  public static UnsafeArrayData fromPrimitiveArray(double[] arr) {
    final int elementSize = 8;
    final int headerSize = calculateHeaderPortionInBytes(arr.length);
    if (arr.length > (Integer.MAX_VALUE - headerSize) / elementSize) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
              "it's too big.");
    }

    final int valueRegionSize = elementSize * arr.length;
    final byte[] data = new byte[valueRegionSize + headerSize];

    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET, arr.length);
    Platform.copyMemory(arr, Platform.DOUBLE_ARRAY_OFFSET, data,
            Platform.BYTE_ARRAY_OFFSET + headerSize, valueRegionSize);

    UnsafeArrayData result = new UnsafeArrayData();
    result.pointTo(data, Platform.BYTE_ARRAY_OFFSET, valueRegionSize + headerSize);
    return result;
  }

  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeBytes(baseObject, baseOffset, sizeInBytes, 42);
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
}
