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
package org.apache.spark.sql.execution.vectorized;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class represents a column of values and provides the main APIs to access the data
 * values. It supports all the types and contains get/put APIs as well as their batched versions.
 * The batched versions are preferable whenever possible.
 *
 * To handle nested schemas, ColumnVector has two types: Arrays and Structs. In both cases these
 * columns have child columns. All of the data is stored in the child columns and the parent column
 * contains nullability, and in the case of Arrays, the lengths and offsets into the child column.
 * Lengths and offsets are encoded identically to INTs.
 * Maps are just a special case of a two field struct.
 * Strings are handled as an Array of ByteType.
 *
 * Capacity: The data stored is dense but the arrays are not fixed capacity. It is the
 * responsibility of the caller to call reserve() to ensure there is enough room before adding
 * elements. This means that the put() APIs do not check as in common cases (i.e. flat schemas),
 * the lengths are known up front.
 *
 * Most of the APIs take the rowId as a parameter. This is the batch local 0-based row id for values
 * in the current RowBatch.
 *
 * A ColumnVector should be considered immutable once originally created. In other words, it is not
 * valid to call put APIs after reads until reset() is called.
 *
 * ColumnVectors are intended to be reused.
 */
public abstract class ColumnVector implements AutoCloseable {
  /**
   * Allocates a column to store elements of `type` on or off heap.
   * Capacity is the initial capacity of the vector and it will grow as necessary. Capacity is
   * in number of elements, not number of bytes.
   */
  public static ColumnVector allocate(int capacity, DataType type, MemoryMode mode) {
    if (mode == MemoryMode.OFF_HEAP) {
      return new OffHeapColumnVector(capacity, type);
    } else {
      return new OnHeapColumnVector(capacity, type);
    }
  }

  /**
   * Holder object to return an array. This object is intended to be reused. Callers should
   * copy the data out if it needs to be stored.
   */
  public static final class Array extends ArrayData {
    // The data for this array. This array contains elements from
    // data[offset] to data[offset + length).
    public final ColumnVector data;
    public int length;
    public int offset;

    // Populate if binary data is required for the Array. This is stored here as an optimization
    // for string data.
    public byte[] byteArray;
    public int byteArrayOffset;

    // Reused staging buffer, used for loading from offheap.
    protected byte[] tmpByteArray = new byte[1];

    protected Array(ColumnVector data) {
      this.data = data;
    }

    @Override
    public int numElements() { return length; }

    @Override
    public ArrayData copy() {
      throw new UnsupportedOperationException();
    }

    // TODO: this is extremely expensive.
    @Override
    public Object[] array() {
      DataType dt = data.dataType();
      Object[] list = new Object[length];

      if (dt instanceof BooleanType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getBoolean(offset + i);
          }
        }
      } else if (dt instanceof ByteType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getByte(offset + i);
          }
        }
      } else if (dt instanceof ShortType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getShort(offset + i);
          }
        }
      } else if (dt instanceof IntegerType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getInt(offset + i);
          }
        }
      } else if (dt instanceof FloatType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getFloat(offset + i);
          }
        }
      } else if (dt instanceof DoubleType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getDouble(offset + i);
          }
        }
      } else if (dt instanceof LongType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getLong(offset + i);
          }
        }
      } else if (dt instanceof DecimalType) {
        DecimalType decType = (DecimalType)dt;
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = getDecimal(i, decType.precision(), decType.scale());
          }
        }
      } else if (dt instanceof StringType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = getUTF8String(i).toString();
          }
        }
      } else if (dt instanceof CalendarIntervalType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = getInterval(i);
          }
        }
      } else {
        throw new UnsupportedOperationException("Type " + dt);
      }
      return list;
    }

    @Override
    public boolean isNullAt(int ordinal) { return data.isNullAt(offset + ordinal); }

    @Override
    public boolean getBoolean(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int ordinal) { return data.getByte(offset + ordinal); }

    @Override
    public short getShort(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int ordinal) { return data.getInt(offset + ordinal); }

    @Override
    public long getLong(int ordinal) { return data.getLong(offset + ordinal); }

    @Override
    public float getFloat(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int ordinal) { return data.getDouble(offset + ordinal); }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      return data.getDecimal(offset + ordinal, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return data.getUTF8String(offset + ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return data.getBinary(offset + ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      int month = data.getChildColumn(0).getInt(offset + ordinal);
      long microseconds = data.getChildColumn(1).getLong(offset + ordinal);
      return new CalendarInterval(month, microseconds);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return data.getStruct(offset + ordinal);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return data.getArray(offset + ordinal);
    }

    @Override
    public MapData getMap(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Returns the data type of this column.
   */
  public final DataType dataType() { return type; }

  /**
   * Resets this column for writing. The currently stored values are no longer accessible.
   */
  public void reset() {
    if (isConstant) return;

    if (childColumns != null) {
      for (ColumnVector c: childColumns) {
        c.reset();
      }
    }
    numNulls = 0;
    elementsAppended = 0;
    if (anyNullsSet) {
      putNotNulls(0, capacity);
      anyNullsSet = false;
    }
  }

  /**
   * Cleans up memory for this column. The column is not usable after this.
   * TODO: this should probably have ref-counted semantics.
   */
  public abstract void close();

  public void reserve(int requiredCapacity) {
    if (requiredCapacity > capacity) {
      int newCapacity = (int) Math.min(MAX_CAPACITY, requiredCapacity * 2L);
      if (requiredCapacity <= newCapacity) {
        try {
          reserveInternal(newCapacity);
        } catch (OutOfMemoryError outOfMemoryError) {
          throwUnsupportedException(newCapacity, requiredCapacity, outOfMemoryError);
        }
      } else {
        throwUnsupportedException(newCapacity, requiredCapacity, null);
      }
    }
  }

  private void throwUnsupportedException(int newCapacity, int requiredCapacity, Throwable cause) {
    String message = "Cannot reserve more than " + newCapacity +
        " bytes in the vectorized reader (requested = " + requiredCapacity + " bytes). As a" +
        " workaround, you can disable the vectorized reader by setting "
        + SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key() + " to false.";

    if (cause != null) {
      throw new RuntimeException(message, cause);
    } else {
      throw new RuntimeException(message);
    }
  }

  /**
   * Ensures that there is enough storage to store capcity elements. That is, the put() APIs
   * must work for all rowIds < capcity.
   */
  protected abstract void reserveInternal(int capacity);

  /**
   * Returns the number of nulls in this column.
   */
  public final int numNulls() { return numNulls; }

  /**
   * Returns true if any of the nulls indicator are set for this column. This can be used
   * as an optimization to prevent setting nulls.
   */
  public final boolean anyNullsSet() { return anyNullsSet; }

  /**
   * Returns the off heap ptr for the arrays backing the NULLs and values buffer. Only valid
   * to call for off heap columns.
   */
  public abstract long nullsNativeAddress();
  public abstract long valuesNativeAddress();

  /**
   * Sets the value at rowId to null/not null.
   */
  public abstract void putNotNull(int rowId);
  public abstract void putNull(int rowId);

  /**
   * Sets the values from [rowId, rowId + count) to null/not null.
   */
  public abstract void putNulls(int rowId, int count);
  public abstract void putNotNulls(int rowId, int count);

  /**
   * Returns whether the value at rowId is NULL.
   */
  public abstract boolean isNullAt(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putBoolean(int rowId, boolean value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putBooleans(int rowId, int count, boolean value);

  /**
   * Returns the value for rowId.
   */
  public abstract boolean getBoolean(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putByte(int rowId, byte value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putBytes(int rowId, int count, byte value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putBytes(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract byte getByte(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putShort(int rowId, short value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putShorts(int rowId, int count, short value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putShorts(int rowId, int count, short[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract short getShort(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putInt(int rowId, int value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putInts(int rowId, int count, int value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putInts(int rowId, int count, int[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be 4-byte little endian ints.
   */
  public abstract void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract int getInt(int rowId);

  /**
   * Returns the dictionary Id for rowId.
   * This should only be called when the ColumnVector is dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  public abstract int getDictId(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putLong(int rowId, long value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putLongs(int rowId, int count, long value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putLongs(int rowId, int count, long[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be 8-byte little endian longs.
   */
  public abstract void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract long getLong(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putFloat(int rowId, float value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putFloats(int rowId, int count, float value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   * src should contain `count` doubles written as ieee format.
   */
  public abstract void putFloats(int rowId, int count, float[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be ieee formatted floats.
   */
  public abstract void putFloats(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract float getFloat(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putDouble(int rowId, double value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putDoubles(int rowId, int count, double value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   * src should contain `count` doubles written as ieee format.
   */
  public abstract void putDoubles(int rowId, int count, double[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be ieee formatted doubles.
   */
  public abstract void putDoubles(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract double getDouble(int rowId);

  /**
   * Puts a byte array that already exists in this column.
   */
  public abstract void putArray(int rowId, int offset, int length);

  /**
   * Returns the length of the array at rowid.
   */
  public abstract int getArrayLength(int rowId);

  /**
   * Returns the offset of the array at rowid.
   */
  public abstract int getArrayOffset(int rowId);

  /**
   * Returns a utility object to get structs.
   */
  public ColumnarBatch.Row getStruct(int rowId) {
    resultStruct.rowId = rowId;
    return resultStruct;
  }

  /**
   * Returns a utility object to get structs.
   * provided to keep API compatibility with InternalRow for code generation
   */
  public ColumnarBatch.Row getStruct(int rowId, int size) {
    resultStruct.rowId = rowId;
    return resultStruct;
  }

  /**
   * Returns the array at rowid.
   */
  public final Array getArray(int rowId) {
    resultArray.length = getArrayLength(rowId);
    resultArray.offset = getArrayOffset(rowId);
    return resultArray;
  }

  /**
   * Loads the data into array.byteArray.
   */
  public abstract void loadBytes(Array array);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract int putByteArray(int rowId, byte[] value, int offset, int count);
  public final int putByteArray(int rowId, byte[] value) {
    return putByteArray(rowId, value, 0, value.length);
  }

  /**
   * Returns the value for rowId.
   */
  private Array getByteArray(int rowId) {
    Array array = getArray(rowId);
    array.data.loadBytes(array);
    return array;
  }

  /**
   * Returns the value for rowId.
   */
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the decimal for rowId.
   */
  public final Decimal getDecimal(int rowId, int precision, int scale) {
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      // TODO: best perf?
      byte[] bytes = getBinary(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }


  public final void putDecimal(int rowId, Decimal value, int precision) {
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      putInt(rowId, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      putLong(rowId, value.toUnscaledLong());
    } else {
      BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();
      putByteArray(rowId, bigInteger.toByteArray());
    }
  }

  /**
   * Returns the UTF8String for rowId.
   */
  public final UTF8String getUTF8String(int rowId) {
    if (dictionary == null) {
      ColumnVector.Array a = getByteArray(rowId);
      return UTF8String.fromBytes(a.byteArray, a.byteArrayOffset, a.length);
    } else {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(rowId));
      return UTF8String.fromBytes(v.getBytes());
    }
  }

  /**
   * Returns the byte array for rowId.
   */
  public final byte[] getBinary(int rowId) {
    if (dictionary == null) {
      ColumnVector.Array array = getByteArray(rowId);
      byte[] bytes = new byte[array.length];
      System.arraycopy(array.byteArray, array.byteArrayOffset, bytes, 0, bytes.length);
      return bytes;
    } else {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(rowId));
      return v.getBytes();
    }
  }

  /**
   * Append APIs. These APIs all behave similarly and will append data to the current vector.  It
   * is not valid to mix the put and append APIs. The append APIs are slower and should only be
   * used if the sizes are not known up front.
   * In all these cases, the return value is the rowId for the first appended element.
   */
  public final int appendNull() {
    assert (!(dataType() instanceof StructType)); // Use appendStruct()
    reserve(elementsAppended + 1);
    putNull(elementsAppended);
    return elementsAppended++;
  }

  public final int appendNotNull() {
    reserve(elementsAppended + 1);
    putNotNull(elementsAppended);
    return elementsAppended++;
  }

  public final int appendNulls(int count) {
    assert (!(dataType() instanceof StructType));
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putNulls(elementsAppended, count);
    elementsAppended += count;
    return result;
  }

  public final int appendNotNulls(int count) {
    assert (!(dataType() instanceof StructType));
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putNotNulls(elementsAppended, count);
    elementsAppended += count;
    return result;
  }

  public final int appendBoolean(boolean v) {
    reserve(elementsAppended + 1);
    putBoolean(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendBooleans(int count, boolean v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putBooleans(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendByte(byte v) {
    reserve(elementsAppended + 1);
    putByte(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendBytes(int count, byte v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putBytes(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendBytes(int length, byte[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putBytes(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendShort(short v) {
    reserve(elementsAppended + 1);
    putShort(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendShorts(int count, short v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putShorts(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendShorts(int length, short[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putShorts(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendInt(int v) {
    reserve(elementsAppended + 1);
    putInt(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendInts(int count, int v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putInts(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendInts(int length, int[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putInts(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendLong(long v) {
    reserve(elementsAppended + 1);
    putLong(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendLongs(int count, long v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putLongs(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendLongs(int length, long[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putLongs(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendFloat(float v) {
    reserve(elementsAppended + 1);
    putFloat(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendFloats(int count, float v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putFloats(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendDouble(double v) {
    reserve(elementsAppended + 1);
    putDouble(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendDoubles(int count, double v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putDoubles(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendDoubles(int length, double[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putDoubles(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendByteArray(byte[] value, int offset, int length) {
    int copiedOffset = arrayData().appendBytes(length, value, offset);
    reserve(elementsAppended + 1);
    putArray(elementsAppended, copiedOffset, length);
    return elementsAppended++;
  }

  public final int appendArray(int length) {
    reserve(elementsAppended + 1);
    putArray(elementsAppended, arrayData().elementsAppended, length);
    return elementsAppended++;
  }

  /**
   * Appends a NULL struct. This *has* to be used for structs instead of appendNull() as this
   * recursively appends a NULL to its children.
   * We don't have this logic as the general appendNull implementation to optimize the more
   * common non-struct case.
   */
  public final int appendStruct(boolean isNull) {
    if (isNull) {
      appendNull();
      for (ColumnVector c: childColumns) {
        if (c.type instanceof StructType) {
          c.appendStruct(true);
        } else {
          c.appendNull();
        }
      }
    } else {
      appendNotNull();
    }
    return elementsAppended;
  }

  /**
   * Returns the data for the underlying array.
   */
  public final ColumnVector arrayData() { return childColumns[0]; }

  /**
   * Returns the ordinal's child data column.
   */
  public final ColumnVector getChildColumn(int ordinal) { return childColumns[ordinal]; }

  /**
   * Returns the elements appended.
   */
  public final int getElementsAppended() { return elementsAppended; }

  /**
   * Returns true if this column is an array.
   */
  public final boolean isArray() { return resultArray != null; }

  /**
   * Marks this column as being constant.
   */
  public final void setIsConstant() { isConstant = true; }

  /**
   * Maximum number of rows that can be stored in this column.
   */
  protected int capacity;

  /**
   * Upper limit for the maximum capacity for this column.
   */
  @VisibleForTesting
  protected int MAX_CAPACITY = Integer.MAX_VALUE;

  /**
   * Data type for this column.
   */
  protected final DataType type;

  /**
   * Number of nulls in this column. This is an optimization for the reader, to skip NULL checks.
   */
  protected int numNulls;

  /**
   * True if there is at least one NULL byte set. This is an optimization for the writer, to skip
   * having to clear NULL bits.
   */
  protected boolean anyNullsSet;

  /**
   * True if this column's values are fixed. This means the column values never change, even
   * across resets.
   */
  protected boolean isConstant;

  /**
   * Default size of each array length value. This grows as necessary.
   */
  protected static final int DEFAULT_ARRAY_LENGTH = 4;

  /**
   * Current write cursor (row index) when appending data.
   */
  protected int elementsAppended;

  /**
   * If this is a nested type (array or struct), the column for the child data.
   */
  protected final ColumnVector[] childColumns;

  /**
   * Reusable Array holder for getArray().
   */
  protected final Array resultArray;

  /**
   * Reusable Struct holder for getStruct().
   */
  protected final ColumnarBatch.Row resultStruct;

  /**
   * The Dictionary for this column.
   *
   * If it's not null, will be used to decode the value in getXXX().
   */
  protected Dictionary dictionary;

  /**
   * Reusable column for ids of dictionary.
   */
  protected ColumnVector dictionaryIds;

  /**
   * Update the dictionary.
   */
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  /**
   * Returns true if this column has a dictionary.
   */
  public boolean hasDictionary() { return this.dictionary != null; }

  /**
   * Reserve a integer column for ids of dictionary.
   */
  public ColumnVector reserveDictionaryIds(int capacity) {
    if (dictionaryIds == null) {
      dictionaryIds = allocate(capacity, DataTypes.IntegerType,
        this instanceof OnHeapColumnVector ? MemoryMode.ON_HEAP : MemoryMode.OFF_HEAP);
    } else {
      dictionaryIds.reset();
      dictionaryIds.reserve(capacity);
    }
    return dictionaryIds;
  }

  /**
   * Returns the underlying integer column for ids of dictionary.
   */
  public ColumnVector getDictionaryIds() {
    return dictionaryIds;
  }

  /**
   * Sets up the common state and also handles creating the child columns if this is a nested
   * type.
   */
  protected ColumnVector(int capacity, DataType type, MemoryMode memMode) {
    this.capacity = capacity;
    this.type = type;

    if (type instanceof ArrayType || type instanceof BinaryType || type instanceof StringType
        || DecimalType.isByteArrayDecimalType(type)) {
      DataType childType;
      int childCapacity = capacity;
      if (type instanceof ArrayType) {
        childType = ((ArrayType)type).elementType();
      } else {
        childType = DataTypes.ByteType;
        childCapacity *= DEFAULT_ARRAY_LENGTH;
      }
      this.childColumns = new ColumnVector[1];
      this.childColumns[0] = ColumnVector.allocate(childCapacity, childType, memMode);
      this.resultArray = new Array(this.childColumns[0]);
      this.resultStruct = null;
    } else if (type instanceof StructType) {
      StructType st = (StructType)type;
      this.childColumns = new ColumnVector[st.fields().length];
      for (int i = 0; i < childColumns.length; ++i) {
        this.childColumns[i] = ColumnVector.allocate(capacity, st.fields()[i].dataType(), memMode);
      }
      this.resultArray = null;
      this.resultStruct = new ColumnarBatch.Row(this.childColumns);
    } else if (type instanceof CalendarIntervalType) {
      // Two columns. Months as int. Microseconds as Long.
      this.childColumns = new ColumnVector[2];
      this.childColumns[0] = ColumnVector.allocate(capacity, DataTypes.IntegerType, memMode);
      this.childColumns[1] = ColumnVector.allocate(capacity, DataTypes.LongType, memMode);
      this.resultArray = null;
      this.resultStruct = new ColumnarBatch.Row(this.childColumns);
    } else {
      this.childColumns = null;
      this.resultArray = null;
      this.resultStruct = null;
    }
  }
}
