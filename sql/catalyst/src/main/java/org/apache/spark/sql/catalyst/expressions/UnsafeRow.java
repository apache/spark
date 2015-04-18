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


import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import scala.collection.Map;
import scala.collection.Seq;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;


// TODO: pick a better name for this class, since this is potentially confusing.

/**
 * An Unsafe implementation of Row which is backed by raw memory instead of Java objets.
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
 */
public final class UnsafeRow implements MutableRow {

  private Object baseObject;
  private long baseOffset;
  private int numFields;
  /** The width of the null tracking bit set, in bytes */
  private int bitSetWidthInBytes;
  @Nullable
  private StructType schema;

  private long getFieldOffset(int ordinal) {
   return baseOffset + bitSetWidthInBytes + ordinal * 8;
  }

  public UnsafeRow() { }

  public void set(Object baseObject, long baseOffset, int numFields, StructType schema) {
    assert numFields >= 0 : "numFields should >= 0";
    assert schema == null || schema.fields().length == numFields;
    this.bitSetWidthInBytes = ((numFields / 64) + ((numFields % 64 == 0 ? 0 : 1))) * 8;
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.numFields = numFields;
    this.schema = schema;
  }

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numFields : "index (" + index + ") should <= " + numFields;
  }

  @Override
  public void setNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.set(baseObject, baseOffset, i);
  }

  @Override
  public void update(int ordinal, Object value) {
    assert schema != null : "schema cannot be null when calling the generic update()";
    final DataType type = schema.fields()[ordinal].dataType();
    // TODO: match based on the type, then set.  This will be slow.
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    PlatformDependent.UNSAFE.putInt(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    PlatformDependent.UNSAFE.putLong(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setDouble(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    PlatformDependent.UNSAFE.putDouble(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    PlatformDependent.UNSAFE.putBoolean(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    PlatformDependent.UNSAFE.putShort(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    PlatformDependent.UNSAFE.putByte(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setFloat(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    PlatformDependent.UNSAFE.putFloat(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setString(int ordinal, String value) {
    // TODO: need to ensure that array has been suitably sized.
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
    // TODO: dispatching based on field type
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
    return PlatformDependent.UNSAFE.getFloat(baseObject, getFieldOffset(i));
  }

  @Override
  public double getDouble(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getDouble(baseObject, getFieldOffset(i));
  }

  @Override
  public String getString(int i) {
    assertIndexIsValid(i);
    // TODO

    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getDecimal(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Seq<T> getSeq(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<T> getList(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> Map<K, V> getMap(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> java.util.Map<K, V> getJavaMap(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public Row getStruct(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getAs(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public Row copy() {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean anyNull() {
    return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes);
  }

  @Override
  public Seq<Object> toSeq() {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public String mkString() {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public String mkString(String sep) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public String mkString(String start, String sep, String end) {
    // TODO
    throw new UnsupportedOperationException();
  }
}
