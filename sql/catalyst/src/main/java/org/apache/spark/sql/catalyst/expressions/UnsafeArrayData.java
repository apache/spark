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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

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

public final class UnsafeArrayData extends ArrayData implements Externalizable, KryoSerializable {
  public static int calculateHeaderPortionInBytes(int numFields) {
    return (int)calculateHeaderPortionInBytes((long)numFields);
  }

  public static long calculateHeaderPortionInBytes(long numFields) {
    return 8 + ((numFields + 63)/ 64) * 8;
  }

  private Object baseObject;
  private long baseOffset;

  // The number of elements in this array
  private int numElements;

  // The size of this array's backing data, in bytes.
  // The 8-bytes header of `numElements` is also included.
  private int sizeInBytes;

  /** The position to start storing array elements, */
  private long elementOffset;

  private long getElementOffset(int ordinal, int elementSize) {
    return elementOffset + ordinal * (long)elementSize;
  }

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  private void assertIndexIsValid(int ordinal) {
    assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
    assert ordinal < numElements : "ordinal (" + ordinal + ") should < " + numElements;
  }

  @Override
  public Object[] array() {
    throw SparkUnsupportedOperationException.apply();
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
    // Read the number of elements from the first 8 bytes.
    final long numElements = Platform.getLong(baseObject, baseOffset);
    assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";
    assert numElements <= Integer.MAX_VALUE :
      "numElements (" + numElements + ") should <= Integer.MAX_VALUE";

    this.numElements = (int)numElements;
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
    this.elementOffset = baseOffset + calculateHeaderPortionInBytes(this.numElements);
  }

  @Override
  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return BitSetMethods.isSet(baseObject, baseOffset + 8, ordinal);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    return SpecializedGettersReader.read(this, ordinal, dataType, true, true);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getBoolean(baseObject, getElementOffset(ordinal, 1));
  }

  @Override
  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getByte(baseObject, getElementOffset(ordinal, 1));
  }

  @Override
  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getShort(baseObject, getElementOffset(ordinal, 2));
  }

  @Override
  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getInt(baseObject, getElementOffset(ordinal, 4));
  }

  @Override
  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getLong(baseObject, getElementOffset(ordinal, 8));
  }

  @Override
  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getFloat(baseObject, getElementOffset(ordinal, 4));
  }

  @Override
  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getDouble(baseObject, getElementOffset(ordinal, 8));
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
    final long monthAndDays = Platform.getLong(baseObject, baseOffset + offset);
    final int months = (int) (0xFFFFFFFFL & monthAndDays);
    final int days = (int) ((0xFFFFFFFF00000000L & monthAndDays) >> 32);
    final long microseconds = Platform.getLong(baseObject, baseOffset + offset + 8);
    return new CalendarInterval(months, days, microseconds);
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    if (isNullAt(ordinal)) return null;
    return VariantVal.readFromUnsafeRow(getLong(ordinal), baseObject, baseOffset);
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

  @Override
  public void update(int ordinal, Object value) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void setNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    BitSetMethods.set(baseObject, baseOffset + 8, ordinal);

    /* we assume the corresponding column was already 0 or
       will be set to 0 later by the caller side */
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    Platform.putBoolean(baseObject, getElementOffset(ordinal, 1), value);
  }

  @Override
  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    Platform.putByte(baseObject, getElementOffset(ordinal, 1), value);
  }

  @Override
  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    Platform.putShort(baseObject, getElementOffset(ordinal, 2), value);
  }

  @Override
  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    Platform.putInt(baseObject, getElementOffset(ordinal, 4), value);
  }

  @Override
  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    Platform.putLong(baseObject, getElementOffset(ordinal, 8), value);
  }

  @Override
  public void setFloat(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    Platform.putFloat(baseObject, getElementOffset(ordinal, 4), value);
  }

  @Override
  public void setDouble(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    Platform.putDouble(baseObject, getElementOffset(ordinal, 8), value);
  }

  // This `hashCode` computation could consume much processor time for large data.
  // If the computation becomes a bottleneck, we can use a light-weight logic; the first fixed bytes
  // are used to compute `hashCode` (See `Vector.hashCode`).
  // The same issue exists in `UnsafeRow.hashCode`.
  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeBytes(baseObject, baseOffset, sizeInBytes, 42);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UnsafeArrayData o) {
      return (sizeInBytes == o.sizeInBytes) &&
        ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
          sizeInBytes);
    }
    return false;
  }

  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
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
    Platform.copyMemory(
      baseObject, baseOffset, arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    arrayCopy.pointTo(arrayDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return arrayCopy;
  }

  @Override
  public boolean[] toBooleanArray() {
    boolean[] values = new boolean[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.BOOLEAN_ARRAY_OFFSET, numElements);
    return values;
  }

  @Override
  public byte[] toByteArray() {
    byte[] values = new byte[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.BYTE_ARRAY_OFFSET, numElements);
    return values;
  }

  @Override
  public short[] toShortArray() {
    short[] values = new short[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.SHORT_ARRAY_OFFSET, numElements * 2L);
    return values;
  }

  @Override
  public int[] toIntArray() {
    int[] values = new int[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.INT_ARRAY_OFFSET, numElements * 4L);
    return values;
  }

  @Override
  public long[] toLongArray() {
    long[] values = new long[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.LONG_ARRAY_OFFSET, numElements * 8L);
    return values;
  }

  @Override
  public float[] toFloatArray() {
    float[] values = new float[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.FLOAT_ARRAY_OFFSET, numElements * 4L);
    return values;
  }

  @Override
  public double[] toDoubleArray() {
    double[] values = new double[numElements];
    Platform.copyMemory(
      baseObject, elementOffset, values, Platform.DOUBLE_ARRAY_OFFSET, numElements * 8L);
    return values;
  }

  public static UnsafeArrayData fromPrimitiveArray(
       Object arr, int offset, int length, int elementSize) {
    final long headerInBytes = calculateHeaderPortionInBytes(length);
    final long valueRegionInBytes = (long)elementSize * length;
    final long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
    if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
      throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3129");
    }

    final long[] data = new long[(int)totalSizeInLongs];

    Platform.putLong(data, Platform.LONG_ARRAY_OFFSET, length);
    if (arr != null) {
      Platform.copyMemory(arr, offset, data,
        Platform.LONG_ARRAY_OFFSET + headerInBytes, valueRegionInBytes);
    }

    UnsafeArrayData result = new UnsafeArrayData();
    result.pointTo(data, Platform.LONG_ARRAY_OFFSET, (int)totalSizeInLongs * 8);
    return result;
  }

  public static UnsafeArrayData createFreshArray(int length, int elementSize) {
    final long headerInBytes = calculateHeaderPortionInBytes(length);
    final long valueRegionInBytes = (long)elementSize * length;
    final long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
    if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
      throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3129");
    }

    final long[] data = new long[(int)totalSizeInLongs];

    Platform.putLong(data, Platform.LONG_ARRAY_OFFSET, length);

    UnsafeArrayData result = new UnsafeArrayData();
    result.pointTo(data, Platform.LONG_ARRAY_OFFSET, (int)totalSizeInLongs * 8);
    return result;
  }

  public static boolean shouldUseGenericArrayData(int elementSize, long length) {
    final long headerInBytes = calculateHeaderPortionInBytes(length);
    final long valueRegionInBytes = elementSize * length;
    final long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
    return totalSizeInLongs > Integer.MAX_VALUE / 8;
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

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    byte[] bytes = UnsafeDataUtils.getBytes(baseObject, baseOffset, sizeInBytes);
    out.writeInt(bytes.length);
    out.writeInt(this.numElements);
    out.write(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.baseOffset = BYTE_ARRAY_OFFSET;
    this.sizeInBytes = in.readInt();
    this.numElements = in.readInt();
    this.elementOffset = baseOffset + calculateHeaderPortionInBytes(this.numElements);
    this.baseObject = new byte[sizeInBytes];
    in.readFully((byte[]) baseObject);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    byte[] bytes = UnsafeDataUtils.getBytes(baseObject, baseOffset, sizeInBytes);
    output.writeInt(bytes.length);
    output.writeInt(this.numElements);
    output.write(bytes);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    this.baseOffset = BYTE_ARRAY_OFFSET;
    this.sizeInBytes = input.readInt();
    this.numElements = input.readInt();
    this.elementOffset = baseOffset + calculateHeaderPortionInBytes(this.numElements);
    this.baseObject = new byte[sizeInBytes];
    input.read((byte[]) baseObject);
  }
}
