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
import javax.annotation.Nonnull;

import com.google.common.primitives.Ints;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.memory.ByteArrayMemoryBlock;
import org.apache.spark.unsafe.memory.OnHeapMemoryBlock;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * An Unsafe implementation of Array which is backed by raw memory instead of Java objects.
 *
 * Each array has four parts:
 *   [numElements][null bits][values or offset&length][variable length portion]
 *
 * The `numElements` is 8 bytes storing the number of elements of this array.
 *
 * In the `null bits` region, we store 1 bit per element, represents whether an element is null
 * Its total size is ceil(numElements / 8) bytes, and it is aligned to 8-byte boundaries.
 *
 * In the `values or offset&length` region, we store the content of elements. For fields that hold
 * fixed-length primitive types, such as long, double, or int, we store the value directly
 * in the field. The whole fixed-length portion (even for byte) is aligned to 8-byte boundaries.
 * For fields with non-primitive or variable-length values, we store a relative offset
 * (w.r.t. the base address of the array) that points to the beginning of the variable-length field
 * and length (they are combined into a long). For variable length portion, each is aligned
 * to 8-byte boundaries.
 *
 * Instances of `UnsafeArrayData` act as pointers to row data stored in this format.
 */

public final class UnsafeArrayData extends ArrayData {

  public static int calculateHeaderPortionInBytes(int numFields) {
    return 8 + ((numFields + 63)/ 64) * 8;
  }

  @Nonnull
  private MemoryBlock base;

  // The number of elements in this array
  private int numElements;

  // The size of this array's backing data, in bytes.
  // The 8-bytes header of `numElements` is also included.
  private int sizeInBytes;

  /** The position to start storing array elements, */
  private long elementOffset;

  private long getElementOffset(int ordinal, int elementSize) {
    return elementOffset + ordinal * elementSize;
  }

  public Object getBaseObject() { return base.getBaseObject(); }
  public long getBaseOffset() { return base.getBaseOffset(); }
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
  public UnsafeArrayData() { }

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
    MemoryBlock block = MemoryBlock.allocateFromObject(baseObject, baseOffset, sizeInBytes);
    pointTo(block);
  }

  public void pointTo(MemoryBlock base) {
    long baseOffset = base.getBaseOffset();
    // Read the number of elements from the first 8 bytes.
    final long numElements = base.getLong(baseOffset);
    assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";
    assert numElements <= Integer.MAX_VALUE :
      "numElements (" + numElements + ") should <= Integer.MAX_VALUE";

    this.numElements = (int)numElements;
    this.base = base;
    this.sizeInBytes = Ints.checkedCast(base.size());
    this.elementOffset = baseOffset + calculateHeaderPortionInBytes(this.numElements);
  }

  @Override
  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return BitSetMethods.isSetBlock(base, base.getBaseOffset() + 8, ordinal);
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
    } else if (dataType instanceof UserDefinedType) {
      return get(ordinal, ((UserDefinedType)dataType).sqlType());
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
  }

  @Override
  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    return base.getBoolean(getElementOffset(ordinal, 1));
  }

  @Override
  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    return base.getByte(getElementOffset(ordinal, 1));
  }

  @Override
  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    return base.getShort(getElementOffset(ordinal, 2));
  }

  @Override
  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    return base.getInt( getElementOffset(ordinal, 4));
  }

  @Override
  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    return base.getLong(getElementOffset(ordinal, 8));
  }

  @Override
  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    return base.getFloat(getElementOffset(ordinal, 4));
  }

  @Override
  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    return base.getDouble(getElementOffset(ordinal, 8));
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    if (isNullAt(ordinal)) return null;
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
    MemoryBlock mb = base.subBlock(offset, size);
    return new UTF8String(mb);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final byte[] bytes = new byte[size];
    base.writeTo(offset, bytes, Platform.BYTE_ARRAY_OFFSET, size);
    return bytes;
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int months = (int) base.getLong(base.getBaseOffset() + offset);
    final long microseconds = base.getLong(base.getBaseOffset() + offset + 8);
    return new CalendarInterval(months, microseconds);
  }

  @Override
  public UnsafeRow getStruct(int ordinal, int numFields) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final UnsafeRow row = new UnsafeRow(numFields);
    row.pointTo(base.getBaseObject(), base.getBaseOffset() + offset, size);
    return row;
  }

  @Override
  public UnsafeArrayData getArray(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final UnsafeArrayData array = new UnsafeArrayData();
    array.pointTo(base.subBlock(offset, size));
    return array;
  }

  @Override
  public UnsafeMapData getMap(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    final UnsafeMapData map = new UnsafeMapData();
    map.pointTo(base.subBlock(offset, size));
    return map;
  }

  @Override
  public void update(int ordinal, Object value) { throw new UnsupportedOperationException(); }

  public void setNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    BitSetMethods.setBlock(base, base.getBaseOffset() + 8, ordinal);

    /* we assume the corresponding column was already 0 or
       will be set to 0 later by the caller side */
  }

  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    base.putBoolean(getElementOffset(ordinal, 1), value);
  }

  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    base.putByte(getElementOffset(ordinal, 1), value);
  }

  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    base.putShort(getElementOffset(ordinal, 2), value);
  }

  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    base.putInt(getElementOffset(ordinal, 4), value);
  }

  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    base.putLong(getElementOffset(ordinal, 8), value);
  }

  public void setFloat(int ordinal, float value) {
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    assertIndexIsValid(ordinal);
    base.putFloat(getElementOffset(ordinal, 4), value);
  }

  public void setDouble(int ordinal, double value) {
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    assertIndexIsValid(ordinal);
    base.putDouble(getElementOffset(ordinal, 8), value);
  }

  // This `hashCode` computation could consume much processor time for large data.
  // If the computation becomes a bottleneck, we can use a light-weight logic; the first fixed bytes
  // are used to compute `hashCode` (See `Vector.hashCode`).
  // The same issue exists in `UnsafeRow.hashCode`.
  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeBytesBlock(base, 42);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UnsafeArrayData) {
      UnsafeArrayData o = (UnsafeArrayData) other;
      return (sizeInBytes == o.sizeInBytes) &&
        ByteArrayMethods.arrayEqualsBlock(base, 0, o.base, 0, sizeInBytes);
    }
    return false;
  }

  public void writeToMemory(Object target, long targetOffset) {
    base.writeTo(0, target, targetOffset, sizeInBytes);
  }

  public void writeTo(ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  @Override
  public UnsafeArrayData copy() {
    UnsafeArrayData arrayCopy = new UnsafeArrayData();
    final byte[] arrayDataCopy = new byte[sizeInBytes];
    MemoryBlock mb = ByteArrayMemoryBlock.fromArray(arrayDataCopy);
    MemoryBlock.copyMemory(base, mb, sizeInBytes);
    arrayCopy.pointTo(mb);
    return arrayCopy;
  }

  @Override
  public boolean[] toBooleanArray() {
    boolean[] values = new boolean[numElements];
    base.writeTo(
      elementOffset - base.getBaseOffset(), values, Platform.BOOLEAN_ARRAY_OFFSET, numElements);
    return values;
  }

  @Override
  public byte[] toByteArray() {
    byte[] values = new byte[numElements];
    base.writeTo(
      elementOffset - base.getBaseOffset(), values, Platform.BYTE_ARRAY_OFFSET, numElements);
    return values;
  }

  @Override
  public short[] toShortArray() {
    short[] values = new short[numElements];
    base.writeTo(
      elementOffset - base.getBaseOffset(), values, Platform.SHORT_ARRAY_OFFSET, numElements * 2);
    return values;
  }

  @Override
  public int[] toIntArray() {
    int[] values = new int[numElements];
    base.writeTo(
      elementOffset - base.getBaseOffset(), values, Platform.INT_ARRAY_OFFSET, numElements * 4);
    return values;
  }

  @Override
  public long[] toLongArray() {
    long[] values = new long[numElements];
    base.writeTo(
      elementOffset - base.getBaseOffset(), values, Platform.LONG_ARRAY_OFFSET, numElements * 8);
    return values;
  }

  @Override
  public float[] toFloatArray() {
    float[] values = new float[numElements];
    base.writeTo(
      elementOffset - base.getBaseOffset(), values, Platform.FLOAT_ARRAY_OFFSET, numElements * 4);
    return values;
  }

  @Override
  public double[] toDoubleArray() {
    double[] values = new double[numElements];
    base.writeTo(
      elementOffset - base.getBaseOffset(), values, Platform.DOUBLE_ARRAY_OFFSET, numElements * 8);
    return values;
  }

  private static UnsafeArrayData fromPrimitiveArray(
       Object arr, int offset, int length, int elementSize) {
    final long headerInBytes = calculateHeaderPortionInBytes(length);
    final long valueRegionInBytes = elementSize * length;
    final long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
    if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
      throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
        "it's too big.");
    }

    final long[] data = new long[(int)totalSizeInLongs];

    Platform.putLong(data, Platform.LONG_ARRAY_OFFSET, length);
    Platform.copyMemory(arr, offset, data,
      Platform.LONG_ARRAY_OFFSET + headerInBytes, valueRegionInBytes);

    UnsafeArrayData result = new UnsafeArrayData();
    MemoryBlock mb = OnHeapMemoryBlock.fromArray(data);
    result.pointTo(mb);
    return result;
  }

  public static UnsafeArrayData fromPrimitiveArray(boolean[] arr) {
    return fromPrimitiveArray(arr, Platform.BOOLEAN_ARRAY_OFFSET, arr.length, 1);
  }

  public static UnsafeArrayData fromPrimitiveArray(byte[] arr) {
    return fromPrimitiveArray(arr, Platform.BYTE_ARRAY_OFFSET, arr.length, 1);
  }

  public static UnsafeArrayData fromPrimitiveArray(short[] arr) {
    return fromPrimitiveArray(arr, Platform.SHORT_ARRAY_OFFSET, arr.length, 2);
  }

  public static UnsafeArrayData fromPrimitiveArray(int[] arr) {
    return fromPrimitiveArray(arr, Platform.INT_ARRAY_OFFSET, arr.length, 4);
  }

  public static UnsafeArrayData fromPrimitiveArray(long[] arr) {
    return fromPrimitiveArray(arr, Platform.LONG_ARRAY_OFFSET, arr.length, 8);
  }

  public static UnsafeArrayData fromPrimitiveArray(float[] arr) {
    return fromPrimitiveArray(arr, Platform.FLOAT_ARRAY_OFFSET, arr.length, 4);
  }

  public static UnsafeArrayData fromPrimitiveArray(double[] arr) {
    return fromPrimitiveArray(arr, Platform.DOUBLE_ARRAY_OFFSET, arr.length, 8);
  }
}
