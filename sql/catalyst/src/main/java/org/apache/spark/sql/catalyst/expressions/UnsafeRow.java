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

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

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
public final class UnsafeRow extends InternalRow implements Externalizable, KryoSerializable {

  public static final int WORD_SIZE = 8;

  //////////////////////////////////////////////////////////////////////////////
  // Static methods
  //////////////////////////////////////////////////////////////////////////////

  public static int calculateBitSetWidthInBytes(int numFields) {
    return ((numFields + 63)/ 64) * 8;
  }

  /**
   * Field types that can be updated in place in UnsafeRows (e.g. we support set() for these types)
   */
  public static final Set<DataType> mutableFieldTypes;

  // DecimalType, DayTimeIntervalType and YearMonthIntervalType are also mutable
  static {
    mutableFieldTypes = Collections.unmodifiableSet(
      new HashSet<>(
        Arrays.asList(
          NullType,
          BooleanType,
          ByteType,
          ShortType,
          IntegerType,
          LongType,
          FloatType,
          DoubleType,
          DateType,
          TimestampType,
          TimestampNTZType
        )));
  }

  public static boolean isFixedLength(DataType dt) {
    if (dt instanceof UserDefinedType) {
      return isFixedLength(((UserDefinedType) dt).sqlType());
    }

    if (dt instanceof DecimalType) {
      return ((DecimalType) dt).precision() <= Decimal.MAX_LONG_DIGITS();
    } else {
      return dt instanceof DayTimeIntervalType || dt instanceof YearMonthIntervalType ||
        mutableFieldTypes.contains(dt);
    }
  }

  public static boolean isMutable(DataType dt) {
    if (dt instanceof UserDefinedType) {
      return isMutable(((UserDefinedType) dt).sqlType());
    }

    return mutableFieldTypes.contains(dt) || dt instanceof DecimalType ||
      dt instanceof CalendarIntervalType || dt instanceof DayTimeIntervalType ||
      dt instanceof YearMonthIntervalType;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Private fields and methods
  //////////////////////////////////////////////////////////////////////////////

  private Object baseObject;
  private long baseOffset;

  /** The number of fields in this row, used for calculating the bitset width (and in assertions) */
  private int numFields;

  /** The size of this row's backing data, in bytes) */
  private int sizeInBytes;

  /** The width of the null tracking bit set, in bytes */
  private int bitSetWidthInBytes;

  private long getFieldOffset(int ordinal) {
    return baseOffset + bitSetWidthInBytes + ordinal * 8L;
  }

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numFields : "index (" + index + ") should < " + numFields;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Public methods
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Construct a new UnsafeRow. The resulting row won't be usable until `pointTo()` has been called,
   * since the value returned by this constructor is equivalent to a null pointer.
   *
   * @param numFields the number of fields in this row
   */
  public UnsafeRow(int numFields) {
    this.numFields = numFields;
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
  }

  // for serializer
  public UnsafeRow() {}

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  @Override
  public int numFields() { return numFields; }

  /**
   * Update this UnsafeRow to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this row's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    assert numFields >= 0 : "numFields (" + numFields + ") should >= 0";
    assert sizeInBytes % 8 == 0 : "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8";
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  /**
   * Update this UnsafeRow to point to the underlying byte array.
   *
   * @param buf byte array to point to
   * @param sizeInBytes the number of bytes valid in the byte array
   */
  public void pointTo(byte[] buf, int sizeInBytes) {
    pointTo(buf, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
  }

  public void setTotalSize(int sizeInBytes) {
    assert sizeInBytes % 8 == 0 : "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8";
    this.sizeInBytes = sizeInBytes;
  }

  public void setNotNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.unset(baseObject, baseOffset, i);
  }

  @Override
  public void setNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.set(baseObject, baseOffset, i);
    // To preserve row equality, zero out the value when setting the column to null.
    // Since this row does not currently support updates to variable-length values, we don't
    // have to worry about zeroing out that data.
    Platform.putLong(baseObject, getFieldOffset(i), 0);
  }

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putInt(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putLong(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setDouble(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putDouble(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putBoolean(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putShort(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putByte(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setFloat(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putFloat(baseObject, getFieldOffset(ordinal), value);
  }

  /**
   * Updates the decimal column.
   *
   * Note: In order to support update a decimal with precision > 18, CAN NOT call
   * setNullAt() for this column.
   */
  @Override
  public void setDecimal(int ordinal, Decimal value, int precision) {
    assertIndexIsValid(ordinal);
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // compact format
      if (value == null) {
        setNullAt(ordinal);
      } else {
        setLong(ordinal, value.toUnscaledLong());
      }
    } else {
      // fixed length
      long cursor = getLong(ordinal) >>> 32;
      assert cursor > 0 : "invalid cursor " + cursor;
      // zero-out the bytes
      Platform.putLong(baseObject, baseOffset + cursor, 0L);
      Platform.putLong(baseObject, baseOffset + cursor + 8, 0L);

      if (value == null || !value.changePrecision(precision, value.scale())) {
        setNullAt(ordinal);
        // keep the offset for future update
        Platform.putLong(baseObject, getFieldOffset(ordinal), cursor << 32);
      } else {

        final BigInteger integer = value.toJavaBigDecimal().unscaledValue();
        byte[] bytes = integer.toByteArray();
        assert(bytes.length <= 16);

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, baseObject, baseOffset + cursor, bytes.length);
        setLong(ordinal, (cursor << 32) | ((long) bytes.length));
      }
    }
  }

  @Override
  public void setInterval(int ordinal, CalendarInterval value) {
    assertIndexIsValid(ordinal);
    long cursor = getLong(ordinal) >>> 32;
    assert cursor > 0 : "invalid cursor " + cursor;
    if (value == null) {
      setNullAt(ordinal);
      // zero-out the bytes
      Platform.putLong(baseObject, baseOffset + cursor, 0L);
      Platform.putLong(baseObject, baseOffset + cursor + 8, 0L);
      // keep the offset for future update
      Platform.putLong(baseObject, getFieldOffset(ordinal), (cursor << 32) | 16L);
    } else {
      Platform.putInt(baseObject, baseOffset + cursor, value.months);
      Platform.putInt(baseObject, baseOffset + cursor + 4, value.days);
      Platform.putLong(baseObject, baseOffset + cursor + 8, value.microseconds);
      setLong(ordinal, (cursor << 32) | 16L);
    }
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    return SpecializedGettersReader.read(this, ordinal, dataType, true, true);
  }

  @Override
  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return BitSetMethods.isSet(baseObject, baseOffset, ordinal);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getBoolean(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getByte(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getShort(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getInt(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getLong(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getFloat(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getDouble(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    if (isNullAt(ordinal)) {
      return null;
    }
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(ordinal), precision, scale);
    } else {
      byte[] bytes = getBinary(ordinal);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
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
    if (isNullAt(ordinal)) {
      return null;
    } else {
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      final byte[] bytes = new byte[size];
      Platform.copyMemory(
        baseObject,
        baseOffset + offset,
        bytes,
        Platform.BYTE_ARRAY_OFFSET,
        size
      );
      return bytes;
    }
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int months = Platform.getInt(baseObject, baseOffset + offset);
      final int days = Platform.getInt(baseObject, baseOffset + offset + 4);
      final long microseconds = Platform.getLong(baseObject, baseOffset + offset + 8);
      return new CalendarInterval(months, days, microseconds);
    }
  }

  @Override
  public UnsafeRow getStruct(int ordinal, int numFields) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      final UnsafeRow row = new UnsafeRow(numFields);
      row.pointTo(baseObject, baseOffset + offset, size);
      return row;
    }
  }

  @Override
  public UnsafeArrayData getArray(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      final UnsafeArrayData array = new UnsafeArrayData();
      array.pointTo(baseObject, baseOffset + offset, size);
      return array;
    }
  }

  @Override
  public UnsafeMapData getMap(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      final UnsafeMapData map = new UnsafeMapData();
      map.pointTo(baseObject, baseOffset + offset, size);
      return map;
    }
  }

  /**
   * Copies this row, returning a self-contained UnsafeRow that stores its data in an internal
   * byte array rather than referencing data stored in a data page.
   */
  @Override
  public UnsafeRow copy() {
    UnsafeRow rowCopy = new UnsafeRow(numFields);
    final byte[] rowDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
      baseObject,
      baseOffset,
      rowDataCopy,
      Platform.BYTE_ARRAY_OFFSET,
      sizeInBytes
    );
    rowCopy.pointTo(rowDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return rowCopy;
  }

  /**
   * Creates an empty UnsafeRow from a byte array with specified numBytes and numFields.
   * The returned row is invalid until we call copyFrom on it.
   */
  public static UnsafeRow createFromByteArray(int numBytes, int numFields) {
    final UnsafeRow row = new UnsafeRow(numFields);
    row.pointTo(new byte[numBytes], numBytes);
    return row;
  }

  /**
   * Copies the input UnsafeRow to this UnsafeRow, and resize the underlying byte[] when the
   * input row is larger than this row.
   */
  public void copyFrom(UnsafeRow row) {
    // copyFrom is only available for UnsafeRow created from byte array.
    assert (baseObject instanceof byte[]) && baseOffset == Platform.BYTE_ARRAY_OFFSET;
    if (row.sizeInBytes > this.sizeInBytes) {
      // resize the underlying byte[] if it's not large enough.
      this.baseObject = new byte[row.sizeInBytes];
    }
    Platform.copyMemory(
      row.baseObject, row.baseOffset, this.baseObject, this.baseOffset, row.sizeInBytes);
    // update the sizeInBytes.
    this.sizeInBytes = row.sizeInBytes;
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
      int offsetInByteArray = (int) (baseOffset - Platform.BYTE_ARRAY_OFFSET);
      out.write((byte[]) baseObject, offsetInByteArray, sizeInBytes);
    } else {
      int dataRemaining = sizeInBytes;
      long rowReadPosition = baseOffset;
      while (dataRemaining > 0) {
        int toTransfer = Math.min(writeBuffer.length, dataRemaining);
        Platform.copyMemory(
          baseObject, rowReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
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
    return UnsafeDataUtils.getBytes(baseObject, baseOffset, sizeInBytes);
  }

  // This is for debugging
  @Override
  public String toString() {
    StringBuilder build = new StringBuilder("[");
    for (int i = 0; i < sizeInBytes; i += 8) {
      if (i != 0) build.append(',');
      build.append(java.lang.Long.toHexString(Platform.getLong(baseObject, baseOffset + i)));
    }
    build.append(']');
    return build.toString();
  }

  @Override
  public boolean anyNull() {
    return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes / 8);
  }

  /**
   * Writes the content of this row into a memory address, identified by an object and an offset.
   * The target memory address must already been allocated, and have enough space to hold all the
   * bytes in this string.
   */
  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  public void writeTo(ByteBuffer buffer) {
    assert (buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  /**
   * Write the bytes of var-length field into ByteBuffer
   *
   * Note: only work with HeapByteBuffer
   */
  public void writeFieldTo(int ordinal, ByteBuffer buffer) {
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;

    buffer.putInt(size);
    int pos = buffer.position();
    buffer.position(pos + size);
    Platform.copyMemory(
      baseObject,
      baseOffset + offset,
      buffer.array(),
      Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + pos,
      size);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    byte[] bytes = getBytes();
    out.writeInt(bytes.length);
    out.writeInt(this.numFields);
    out.write(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.baseOffset = BYTE_ARRAY_OFFSET;
    this.sizeInBytes = in.readInt();
    this.numFields = in.readInt();
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    this.baseObject = new byte[sizeInBytes];
    in.readFully((byte[]) baseObject);
  }

  @Override
  public void write(Kryo kryo, Output out) {
    byte[] bytes = getBytes();
    out.writeInt(bytes.length);
    out.writeInt(this.numFields);
    out.write(bytes);
  }

  @Override
  public void read(Kryo kryo, Input in) {
    this.baseOffset = BYTE_ARRAY_OFFSET;
    this.sizeInBytes = in.readInt();
    this.numFields = in.readInt();
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    this.baseObject = new byte[sizeInBytes];
    in.read((byte[]) baseObject);
  }
}
