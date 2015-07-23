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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * An Unsafe implementation of Row which is backed by raw memory instead of Java objects.
 *
 * Each tuple has three parts: [null bit set] [values] [variable length portion]
 *
 * The bit set is used for null tracking and is aligned to 8-byte word boundaries.  It stores
 * one bit per field.
 *
 * In the `values` region, we store one 8-byte word per field. For fields that hold fixed-length
 * primitive types, such as long, double, or int, we store the value directly in the word. For
 * fields with non-primitive or variable-length values, we store a relative offset (w.r.t. the
 * base address of the row) that points to the beginning of the variable-length field, and length
 * (they are combined into a long).
 *
 * Instances of `UnsafeRow` act as pointers to row data stored in this format.
 */
public final class UnsafeRow extends MutableRow {

  private Object baseObject;
  private long baseOffset;

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  /** The number of fields in this row, used for calculating the bitset width (and in assertions) */
  private int numFields;

  /** The size of this row's backing data, in bytes) */
  private int sizeInBytes;

  public int length() { return numFields; }

  /** The width of the null tracking bit set, in bytes */
  private int bitSetWidthInBytes;

  private long getFieldOffset(int ordinal) {
   return baseOffset + bitSetWidthInBytes + ordinal * 8L;
  }

  public static int calculateBitSetWidthInBytes(int numFields) {
    return ((numFields / 64) + (numFields % 64 == 0 ? 0 : 1)) * 8;
  }

  /**
   * Field types that can be updated in place in UnsafeRows (e.g. we support set() for these types)
   */
  public static final Set<DataType> settableFieldTypes;

  /**
   * Fields types can be read(but not set (e.g. set() will throw UnsupportedOperationException).
   */
  public static final Set<DataType> readableFieldTypes;

  // TODO: support DecimalType
  static {
    settableFieldTypes = Collections.unmodifiableSet(
      new HashSet<>(
        Arrays.asList(new DataType[] {
          NullType,
          BooleanType,
          ByteType,
          ShortType,
          IntegerType,
          LongType,
          FloatType,
          DoubleType,
          DateType,
          TimestampType
    })));

    // We support get() on a superset of the types for which we support set():
    final Set<DataType> _readableFieldTypes = new HashSet<>(
      Arrays.asList(new DataType[]{
        StringType,
        BinaryType
      }));
    _readableFieldTypes.addAll(settableFieldTypes);
    readableFieldTypes = Collections.unmodifiableSet(_readableFieldTypes);
  }

  /**
   * Construct a new UnsafeRow. The resulting row won't be usable until `pointTo()` has been called,
   * since the value returned by this constructor is equivalent to a null pointer.
   */
  public UnsafeRow() { }

  /**
   * Update this UnsafeRow to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param numFields the number of fields in this row
   * @param sizeInBytes the size of this row's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int numFields, int sizeInBytes) {
    assert numFields >= 0 : "numFields should >= 0";
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.numFields = numFields;
    this.sizeInBytes = sizeInBytes;
  }

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numFields : "index (" + index + ") should < " + numFields;
  }

  @Override
  public void setNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.set(baseObject, baseOffset, i);
    // To preserve row equality, zero out the value when setting the column to null.
    // Since this row does does not currently support updates to variable-length values, we don't
    // have to worry about zeroing out that data.
    PlatformDependent.UNSAFE.putLong(baseObject, getFieldOffset(i), 0);
  }

  private void setNotNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.unset(baseObject, baseOffset, i);
  }

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putInt(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putLong(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setDouble(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    PlatformDependent.UNSAFE.putDouble(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putBoolean(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putShort(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putByte(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setFloat(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    PlatformDependent.UNSAFE.putFloat(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public int size() {
    return numFields;
  }

  @Override
  public Object get(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int i) {
    assertIndexIsValid(i);
    return BitSetMethods.isSet(baseObject, baseOffset, i);
  }

  @Override
  public boolean getBoolean(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getBoolean(baseObject, getFieldOffset(i));
  }

  @Override
  public byte getByte(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getByte(baseObject, getFieldOffset(i));
  }

  @Override
  public short getShort(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getShort(baseObject, getFieldOffset(i));
  }

  @Override
  public int getInt(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getInt(baseObject, getFieldOffset(i));
  }

  @Override
  public long getLong(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getLong(baseObject, getFieldOffset(i));
  }

  @Override
  public float getFloat(int i) {
    assertIndexIsValid(i);
    if (isNullAt(i)) {
      return Float.NaN;
    } else {
      return PlatformDependent.UNSAFE.getFloat(baseObject, getFieldOffset(i));
    }
  }

  @Override
  public double getDouble(int i) {
    assertIndexIsValid(i);
    if (isNullAt(i)) {
      return Float.NaN;
    } else {
      return PlatformDependent.UNSAFE.getDouble(baseObject, getFieldOffset(i));
    }
  }

  @Override
  public UTF8String getUTF8String(int i) {
    assertIndexIsValid(i);
    return isNullAt(i) ? null : UTF8String.fromBytes(getBinary(i));
  }

  @Override
  public byte[] getBinary(int i) {
    if (isNullAt(i)) {
      return null;
    } else {
      assertIndexIsValid(i);
      final long offsetAndSize = getLong(i);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) (offsetAndSize & ((1L << 32) - 1));
      final byte[] bytes = new byte[size];
      PlatformDependent.copyMemory(
        baseObject,
        baseOffset + offset,
        bytes,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        size
      );
      return bytes;
    }
  }

  @Override
  public String getString(int i) {
    return getUTF8String(i).toString();
  }

  /**
   * Copies this row, returning a self-contained UnsafeRow that stores its data in an internal
   * byte array rather than referencing data stored in a data page.
   * <p>
   * This method is only supported on UnsafeRows that do not use ObjectPools.
   */
  @Override
  public UnsafeRow copy() {
    UnsafeRow rowCopy = new UnsafeRow();
    final byte[] rowDataCopy = new byte[sizeInBytes];
    PlatformDependent.copyMemory(
      baseObject,
      baseOffset,
      rowDataCopy,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      sizeInBytes
    );
    rowCopy.pointTo(rowDataCopy, PlatformDependent.BYTE_ARRAY_OFFSET, numFields, sizeInBytes);
    return rowCopy;
  }

  /**
   * Write this UnsafeRow's underlying bytes to the given OutputStream.
   *
   * @param out the stream to write to.
   * @param writeBuffer a byte array for buffering chunks of off-heap data while writing to the
   *                    output stream. If this row is backed by an on-heap byte array, then this
   *                    buffer will not be used and may be null.
   */
  public void writeToStream(OutputStream out, byte[] writeBuffer) throws IOException {
    if (baseObject instanceof byte[]) {
      int offsetInByteArray = (int) (PlatformDependent.BYTE_ARRAY_OFFSET - baseOffset);
      out.write((byte[]) baseObject, offsetInByteArray, sizeInBytes);
    } else {
      int dataRemaining = sizeInBytes;
      long rowReadPosition = baseOffset;
      while (dataRemaining > 0) {
        int toTransfer = Math.min(writeBuffer.length, dataRemaining);
        PlatformDependent.copyMemory(
          baseObject,
          rowReadPosition,
          writeBuffer,
          PlatformDependent.BYTE_ARRAY_OFFSET,
          toTransfer);
        out.write(writeBuffer, 0, toTransfer);
        rowReadPosition += toTransfer;
        dataRemaining -= toTransfer;
      }
    }
  }

  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeWords(baseObject, baseOffset, sizeInBytes, 42);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UnsafeRow) {
      UnsafeRow o = (UnsafeRow) other;
      return (sizeInBytes == o.sizeInBytes) &&
        ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
          sizeInBytes);
    }
    return false;
  }

  /**
   * Returns the underlying bytes for this UnsafeRow.
   */
  public byte[] getBytes() {
    if (baseObject instanceof byte[] && baseOffset == PlatformDependent.BYTE_ARRAY_OFFSET
        && (((byte[]) baseObject).length == sizeInBytes)) {
      return (byte[]) baseObject;
    } else {
      byte[] bytes = new byte[sizeInBytes];
      PlatformDependent.copyMemory(baseObject, baseOffset, bytes,
        PlatformDependent.BYTE_ARRAY_OFFSET, sizeInBytes);
      return bytes;
    }
  }

  // This is for debugging
  @Override
  public String toString() {
    StringBuilder build = new StringBuilder("[");
    for (int i = 0; i < sizeInBytes; i += 8) {
      build.append(PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + i));
      build.append(',');
    }
    build.append(']');
    return build.toString();
  }

  @Override
  public boolean anyNull() {
    return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes / 8);
  }
}
