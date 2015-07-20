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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ObjectPool;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.types.UTF8String;


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
 * (they are combined into a long). For other objects, they are stored in a pool, the indexes of
 * them are hold in the the word.
 *
 * In order to support fast hashing and equality checks for UnsafeRows that contain objects
 * when used as grouping key in BytesToBytesMap, we put the objects in an UniqueObjectPool to make
 * sure all the key have the same index for same object, then we can hash/compare the objects by
 * hash/compare the index.
 *
 * For non-primitive types, the word of a field could be:
 *   UNION {
 *     [1] [offset: 31bits] [length: 31bits]  // StringType
 *     [0] [offset: 31bits] [length: 31bits]  // BinaryType
 *     - [index: 63bits]                      // StringType, Binary, index to object in pool
 *   }
 *
 * Instances of `UnsafeRow` act as pointers to row data stored in this format.
 */
public final class UnsafeRow extends MutableRow {

  private Object baseObject;
  private long baseOffset;

  /** A pool to hold non-primitive objects */
  private ObjectPool pool;

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }
  public ObjectPool getPool() { return pool; }

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

  public static final long OFFSET_BITS = 31L;

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
   * @param pool the object pool to hold arbitrary objects
   */
  public void pointTo(
      Object baseObject, long baseOffset, int numFields, int sizeInBytes, ObjectPool pool) {
    assert numFields >= 0 : "numFields should >= 0";
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.numFields = numFields;
    this.sizeInBytes = sizeInBytes;
    this.pool = pool;
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

  /**
   * Updates the column `i` as Object `value`, which cannot be primitive types.
   */
  @Override
  public void update(int i, Object value) {
    if (value == null) {
      if (!isNullAt(i)) {
        // remove the old value from pool
        long idx = getLong(i);
        if (idx <= 0) {
          // this is the index of old value in pool, remove it
          pool.replace((int)-idx, null);
        } else {
          // there will be some garbage left (UTF8String or byte[])
        }
        setNullAt(i);
      }
      return;
    }

    if (isNullAt(i)) {
      // there is not an old value, put the new value into pool
      int idx = pool.put(value);
      setLong(i, (long)-idx);
    } else {
      // there is an old value, check the type, then replace it or update it
      long v = getLong(i);
      if (v <= 0) {
        // it's the index in the pool, replace old value with new one
        int idx = (int)-v;
        pool.replace(idx, value);
      } else {
        // old value is UTF8String or byte[], try to reuse the space
        boolean isString;
        byte[] newBytes;
        if (value instanceof UTF8String) {
          newBytes = ((UTF8String) value).getBytes();
          isString = true;
        } else {
          newBytes = (byte[]) value;
          isString = false;
        }
        int offset = (int) ((v >> OFFSET_BITS) & Integer.MAX_VALUE);
        int oldLength = (int) (v & Integer.MAX_VALUE);
        if (newBytes.length <= oldLength) {
          // the new value can fit in the old buffer, re-use it
          PlatformDependent.copyMemory(
            newBytes,
            PlatformDependent.BYTE_ARRAY_OFFSET,
            baseObject,
            baseOffset + offset,
            newBytes.length);
          long flag = isString ? 1L << (OFFSET_BITS * 2) : 0L;
          setLong(i, flag | (((long) offset) << OFFSET_BITS) | (long) newBytes.length);
        } else {
          // Cannot fit in the buffer
          int idx = pool.put(value);
          setLong(i, (long) -idx);
        }
      }
    }
    setNotNullAt(i);
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

  /**
   * Returns the object for column `i`, which should not be primitive type.
   */
  @Override
  public Object get(int i) {
    assertIndexIsValid(i);
    if (isNullAt(i)) {
      return null;
    }
    long v = PlatformDependent.UNSAFE.getLong(baseObject, getFieldOffset(i));
    if (v <= 0) {
      // It's an index to object in the pool.
      int idx = (int)-v;
      return pool.get(idx);
    } else {
      // The column could be StingType or BinaryType
      boolean isString = (v >> (OFFSET_BITS * 2)) > 0;
      int offset = (int) ((v >> OFFSET_BITS) & Integer.MAX_VALUE);
      int size = (int) (v & Integer.MAX_VALUE);
      final byte[] bytes = new byte[size];
      // TODO(davies): Avoid the copy once we can manage the life cycle of Row well.
      PlatformDependent.copyMemory(
        baseObject,
        baseOffset + offset,
        bytes,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        size
      );
      if (isString) {
        return UTF8String.fromBytes(bytes);
      } else {
        return bytes;
      }
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

  /**
   * Copies this row, returning a self-contained UnsafeRow that stores its data in an internal
   * byte array rather than referencing data stored in a data page.
   * <p>
   * This method is only supported on UnsafeRows that do not use ObjectPools.
   */
  @Override
  public InternalRow copy() {
    if (pool != null) {
      throw new UnsupportedOperationException(
        "Copy is not supported for UnsafeRows that use object pools");
    } else {
      UnsafeRow rowCopy = new UnsafeRow();
      final byte[] rowDataCopy = new byte[sizeInBytes];
      PlatformDependent.copyMemory(
        baseObject,
        baseOffset,
        rowDataCopy,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        sizeInBytes
      );
      rowCopy.pointTo(
        rowDataCopy, PlatformDependent.BYTE_ARRAY_OFFSET, numFields, sizeInBytes, null);
      return rowCopy;
    }
  }

  @Override
  public boolean anyNull() {
    return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes);
  }
}
