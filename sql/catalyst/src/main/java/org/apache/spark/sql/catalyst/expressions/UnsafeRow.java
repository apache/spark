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

import scala.collection.Map;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import static org.apache.spark.sql.types.DataTypes.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UTF8String;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.bitset.BitSetMethods;

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
 * base address of the row) that points to the beginning of the variable-length field.
 *
 * Instances of `UnsafeRow` act as pointers to row data stored in this format.
 */
public final class UnsafeRow implements MutableRow {

  private Object baseObject;
  private long baseOffset;

  Object getBaseObject() { return baseObject; }
  long getBaseOffset() { return baseOffset; }

  /** The number of fields in this row, used for calculating the bitset width (and in assertions) */
  private int numFields;

  /** The width of the null tracking bit set, in bytes */
  private int bitSetWidthInBytes;
  /**
   * This optional schema is required if you want to call generic get() and set() methods on
   * this UnsafeRow, but is optional if callers will only use type-specific getTYPE() and setTYPE()
   * methods. This should be removed after the planned InternalRow / Row split; right now, it's only
   * needed by the generic get() method, which is only called internally by code that accesses
   * UTF8String-typed columns.
   */
  @Nullable
  private StructType schema;

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

  static {
    settableFieldTypes = Collections.unmodifiableSet(
      new HashSet<DataType>(
        Arrays.asList(new DataType[] {
          NullType,
          BooleanType,
          ByteType,
          ShortType,
          IntegerType,
          LongType,
          FloatType,
          DoubleType
    })));

    // We support get() on a superset of the types for which we support set():
    final Set<DataType> _readableFieldTypes = new HashSet<DataType>(
      Arrays.asList(new DataType[]{
        StringType
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
   * @param schema an optional schema; this is necessary if you want to call generic get() or set()
   *               methods on this row, but is optional if the caller will only use type-specific
   *               getTYPE() and setTYPE() methods.
   */
  public void pointTo(
      Object baseObject,
      long baseOffset,
      int numFields,
      @Nullable StructType schema) {
    assert numFields >= 0 : "numFields should >= 0";
    assert schema == null || schema.fields().length == numFields;
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.numFields = numFields;
    this.schema = schema;
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
    PlatformDependent.UNSAFE.putFloat(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setString(int ordinal, String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return numFields;
  }

  @Override
  public int length() {
    return size();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Object apply(int i) {
    return get(i);
  }

  @Override
  public Object get(int i) {
    assertIndexIsValid(i);
    assert (schema != null) : "Schema must be defined when calling generic get() method";
    final DataType dataType = schema.fields()[i].dataType();
    // UnsafeRow is only designed to be invoked by internal code, which only invokes this generic
    // get() method when trying to access UTF8String-typed columns. If we refactor the codebase to
    // separate the internal and external row interfaces, then internal code can fetch strings via
    // a new getUTF8String() method and we'll be able to remove this method.
    if (isNullAt(i)) {
      return null;
    } else if (dataType == StringType) {
      return getUTF8String(i);
    } else {
      throw new UnsupportedOperationException();
    }
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

  public UTF8String getUTF8String(int i) {
    assertIndexIsValid(i);
    final UTF8String str = new UTF8String();
    final long offsetToStringSize = getLong(i);
    final int stringSizeInBytes =
      (int) PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + offsetToStringSize);
    final byte[] strBytes = new byte[stringSizeInBytes];
    PlatformDependent.copyMemory(
      baseObject,
      baseOffset + offsetToStringSize + 8,  // The `+ 8` is to skip past the size to get the data
      strBytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      stringSizeInBytes
    );
    str.set(strBytes);
    return str;
  }

  @Override
  public String getString(int i) {
    return getUTF8String(i).toString();
  }

  @Override
  public BigDecimal getDecimal(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Seq<T> getSeq(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<T> getList(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> Map<K, V> getMap(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> fieldNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> java.util.Map<K, V> getJavaMap(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Row getStruct(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getAs(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getAs(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int fieldIndex(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Row copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean anyNull() {
    return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes);
  }

  @Override
  public Seq<Object> toSeq() {
    final ArraySeq<Object> values = new ArraySeq<Object>(numFields);
    for (int fieldNumber = 0; fieldNumber < numFields; fieldNumber++) {
      values.update(fieldNumber, get(fieldNumber));
    }
    return values;
  }

  @Override
  public String toString() {
    return mkString("[", ",", "]");
  }

  @Override
  public String mkString() {
    return toSeq().mkString();
  }

  @Override
  public String mkString(String sep) {
    return toSeq().mkString(sep);
  }

  @Override
  public String mkString(String start, String sep, String end) {
    return toSeq().mkString(start, sep, end);
  }
}
