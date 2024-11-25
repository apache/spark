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
import java.nio.ByteBuffer;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class adds write APIs to ColumnVector.
 * It supports all the types and contains put APIs as well as their batched versions.
 * The batched versions are preferable whenever possible.
 *
 * Capacity: The data stored is dense but the arrays are not fixed capacity. It is the
 * responsibility of the caller to call reserve() to ensure there is enough room before adding
 * elements. This means that the put() APIs do not check as in common cases (i.e. flat schemas),
 * the lengths are known up front.
 *
 * A WritableColumnVector should be considered immutable once originally created. In other words,
 * it is not valid to call put APIs after reads until reset() is called.
 *
 * WritableColumnVector are intended to be reused.
 */
public abstract class WritableColumnVector extends ColumnVector {
  private final byte[] byte8 = new byte[8];

  protected abstract void releaseMemory();

  /**
   * Resets this column for writing. The currently stored values are no longer accessible.
   */
  public void reset() {
    if (isConstant || isAllNull) return;

    if (childColumns != null) {
      for (WritableColumnVector c: childColumns) {
        c.reset();
      }
    }
    elementsAppended = 0;
    if (numNulls > 0) {
      putNotNulls(0, capacity);
      numNulls = 0;
    }

    if (hugeVectorThreshold > -1 && capacity > hugeVectorThreshold) {
      capacity = defaultCapacity;
      releaseMemory();
      reserveInternal(capacity);
    }
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    if (dictionaryIds != null) {
      dictionaryIds.close();
      dictionaryIds = null;
    }
    dictionary = null;
    releaseMemory();
  }

  @Override
  public void closeIfNotWritable() {
    // no-op
  }

  public void reserveAdditional(int additionalCapacity) {
    reserve(elementsAppended + additionalCapacity);
  }

  public void reserve(int requiredCapacity) {
    if (requiredCapacity < 0) {
      throwUnsupportedException(requiredCapacity, null);
    } else if (requiredCapacity > capacity) {
      int newCapacity =
          hugeVectorThreshold < 0 || requiredCapacity < hugeVectorThreshold ?
              (int) Math.min(MAX_CAPACITY, requiredCapacity * 2L) :
              (int) Math.min(MAX_CAPACITY, requiredCapacity * hugeVectorReserveRatio);
      if (requiredCapacity <= newCapacity) {
        try {
          reserveInternal(newCapacity);
        } catch (OutOfMemoryError outOfMemoryError) {
          throwUnsupportedException(requiredCapacity, outOfMemoryError);
        }
      } else {
        throwUnsupportedException(requiredCapacity, null);
      }
    }
  }

  private void throwUnsupportedException(int requiredCapacity, Throwable cause) {
    String message = "Cannot reserve additional contiguous bytes in the vectorized reader (" +
        (requiredCapacity >= 0 ? "requested " + requiredCapacity + " bytes" : "integer overflow") +
        "). As a workaround, you can reduce the vectorized reader batch size, or disable the " +
        "vectorized reader, or disable " + SQLConf.BUCKETING_ENABLED().key() + " if you read " +
        "from bucket table. For Parquet file format, refer to " +
        SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().key() +
        " (default " + SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().defaultValueString() +
        ") and " + SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key() + "; for ORC file format, " +
        "refer to " + SQLConf.ORC_VECTORIZED_READER_BATCH_SIZE().key() +
        " (default " + SQLConf.ORC_VECTORIZED_READER_BATCH_SIZE().defaultValueString() +
        ") and " + SQLConf.ORC_VECTORIZED_READER_ENABLED().key() + ".";
    throw new RuntimeException(message, cause);
  }

  @Override
  public boolean hasNull() {
    return isAllNull || numNulls > 0;
  }

  @Override
  public int numNulls() { return numNulls; }

  /**
   * Returns the dictionary Id for rowId.
   *
   * This should only be called when this `WritableColumnVector` represents dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  public abstract int getDictId(int rowId);

  /**
   * The Dictionary for this column.
   *
   * If it's not null, will be used to decode the value in getXXX().
   */
  protected Dictionary dictionary;

  /**
   * Reusable column for ids of dictionary.
   */
  protected WritableColumnVector dictionaryIds;

  /**
   * Returns true if this column has a dictionary.
   */
  public boolean hasDictionary() { return this.dictionary != null; }

  /**
   * Returns the underlying integer column for ids of dictionary.
   */
  public WritableColumnVector getDictionaryIds() {
    return dictionaryIds;
  }

  /**
   * Update the dictionary.
   */
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  /**
   * Reserve a integer column for ids of dictionary.
   */
  public WritableColumnVector reserveDictionaryIds(int capacity) {
    if (dictionaryIds == null) {
      dictionaryIds = reserveNewColumn(capacity, DataTypes.IntegerType);
    } else {
      dictionaryIds.reset();
      dictionaryIds.reserve(capacity);
    }
    return dictionaryIds;
  }

  /**
   * Ensures that there is enough storage to store capacity elements. That is, the put() APIs
   * must work for all rowIds < capacity.
   */
  protected abstract void reserveInternal(int capacity);

  /**
   * Sets null/not null to the value at rowId.
   */
  public abstract void putNotNull(int rowId);
  public abstract void putNull(int rowId);

  /**
   * Sets null/not null to the values at [rowId, rowId + count).
   */
  public abstract void putNulls(int rowId, int count);
  public abstract void putNotNulls(int rowId, int count);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putBoolean(int rowId, boolean value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putBooleans(int rowId, int count, boolean value);

  /**
   * Sets bits from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   * src must contain bit-packed 8 booleans in the byte.
   */
  public void putBooleans(int rowId, int count, byte src, int srcIndex) {
    assert ((srcIndex + count) <= 8);
    byte8[0] = (byte)(src & 1);
    byte8[1] = (byte)(src >>> 1 & 1);
    byte8[2] = (byte)(src >>> 2 & 1);
    byte8[3] = (byte)(src >>> 3 & 1);
    byte8[4] = (byte)(src >>> 4 & 1);
    byte8[5] = (byte)(src >>> 5 & 1);
    byte8[6] = (byte)(src >>> 6 & 1);
    byte8[7] = (byte)(src >>> 7 & 1);
    putBytes(rowId, count, byte8, srcIndex);
  }

  /**
   * Sets bits from [src[0], src[7]] to [rowId, rowId + 7]
   * src must contain bit-packed 8 booleans in the byte.
   */
  public abstract void putBooleans(int rowId, byte src);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putByte(int rowId, byte value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putBytes(int rowId, int count, byte value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putBytes(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putShort(int rowId, short value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putShorts(int rowId, int count, short value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putShorts(int rowId, int count, short[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 2]) to [rowId, rowId + count)
   * The data in src must be 2-byte platform native endian shorts.
   */
  public abstract void putShorts(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putInt(int rowId, int value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putInts(int rowId, int count, int value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putInts(int rowId, int count, int[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be 4-byte platform native endian ints.
   */
  public abstract void putInts(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be 4-byte little endian ints.
   */
  public abstract void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putLong(int rowId, long value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putLongs(int rowId, int count, long value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putLongs(int rowId, int count, long[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
   * The data in src must be 8-byte platform native endian longs.
   */
  public abstract void putLongs(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src + srcIndex, src + srcIndex + count * 8) to [rowId, rowId + count)
   * The data in src must be 8-byte little endian longs.
   */
  public abstract void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putFloat(int rowId, float value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putFloats(int rowId, int count, float value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putFloats(int rowId, int count, float[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be ieee formatted floats in platform native endian.
   */
  public abstract void putFloats(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be ieee formatted floats in little endian.
   */
  public abstract void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putDouble(int rowId, double value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putDoubles(int rowId, int count, double value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putDoubles(int rowId, int count, double[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
   * The data in src must be ieee formatted doubles in platform native endian.
   */
  public abstract void putDoubles(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
   * The data in src must be ieee formatted doubles in little endian.
   */
  public abstract void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Puts a byte array that already exists in this column.
   */
  public abstract void putArray(int rowId, int offset, int length);

  /**
   * Sets values from [value + offset, value + offset + count) to the values at rowId.
   */
  public abstract int putByteArray(int rowId, byte[] value, int offset, int count);
  public final int putByteArray(int rowId, byte[] value) {
    return putByteArray(rowId, value, 0, value.length);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
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

  public void putDecimal(int rowId, Decimal value, int precision) {
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      putInt(rowId, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      putLong(rowId, value.toUnscaledLong());
    } else {
      BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();
      putByteArray(rowId, bigInteger.toByteArray());
    }
  }

  public void putInterval(int rowId, CalendarInterval value) {
    getChild(0).putInt(rowId, value.months);
    getChild(1).putInt(rowId, value.days);
    getChild(2).putLong(rowId, value.microseconds);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) return null;
    if (dictionary == null) {
      return arrayData().getBytesAsUTF8String(getArrayOffset(rowId), getArrayLength(rowId));
    } else {
      byte[] bytes = dictionary.decodeToBinary(dictionaryIds.getDictId(rowId));
      return UTF8String.fromBytes(bytes);
    }
  }

  /**
   * Gets the values of bytes from [rowId, rowId + count), as a UTF8String.
   * This method is similar to {@link ColumnVector#getBytes(int, int)}, but can save data copy as
   * UTF8String is used as a pointer.
   */
  protected abstract UTF8String getBytesAsUTF8String(int rowId, int count);

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    if (dictionary == null) {
      return arrayData().getBytes(getArrayOffset(rowId), getArrayLength(rowId));
    } else {
      return dictionary.decodeToBinary(dictionaryIds.getDictId(rowId));
    }
  }

  /**
   * Gets the values of bytes from [rowId, rowId + count), as a ByteBuffer.
   * This method is similar to {@link ColumnVector#getBytes(int, int)}, but avoids making a copy.
   */
  public abstract ByteBuffer getByteBuffer(int rowId, int count);

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

  /**
   * Append bits from [src[offset], src[offset + count])
   * src must contain bit-packed 8 booleans in the byte.
   */
  public final int appendBooleans(int count, byte src, int offset) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putBooleans(elementsAppended, count, src, offset);
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

  public final int appendFloats(int length, float[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putFloats(elementsAppended, length, src, offset);
    elementsAppended += length;
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
    for (WritableColumnVector childColumn : childColumns) {
      childColumn.reserve(childColumn.elementsAppended + length);
    }
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
      // This is the same as appendNull but without the assertion for struct types
      reserve(elementsAppended + 1);
      putNull(elementsAppended);
      elementsAppended++;
      for (WritableColumnVector c: childColumns) {
        if (c.type instanceof StructType || c.type instanceof VariantType) {
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
   * Appends multiple copies of a Java Object to the vector using the corresponding append* method
   * above.
   * @param length: The number of instances to append
   * @param value value to append to the vector
   * @return the number of values appended if the value maps to one of the append* methods above,
   * or Optional.empty() otherwise.
   */
  public Optional<Integer> appendObjects(int length, Object value) {
    if (value instanceof Boolean) {
      return Optional.of(appendBooleans(length, (Boolean) value));
    }
    if (value instanceof Byte) {
      return Optional.of(appendBytes(length, (Byte) value));
    }
    if (value instanceof Decimal decimal) {
      long unscaled = decimal.toUnscaledLong();
      if (decimal.precision() <= Decimal.MAX_INT_DIGITS()) {
        return Optional.of(appendInts(length, (int) unscaled));
      } else if (decimal.precision() <= Decimal.MAX_LONG_DIGITS()) {
        return Optional.of(appendLongs(length, unscaled));
      } else {
        BigInteger integer = decimal.toJavaBigDecimal().unscaledValue();
        byte[] bytes = integer.toByteArray();
        int result = 0;
        for (int i = 0; i < length; ++i) {
          result += appendByteArray(bytes, 0, bytes.length);
        }
        return Optional.of(result);
      }
    }
    if (value instanceof Double) {
      return Optional.of(appendDoubles(length, (Double) value));
    }
    if (value instanceof Float) {
      return Optional.of(appendFloats(length, (Float) value));
    }
    if (value instanceof Integer) {
      return Optional.of(appendInts(length, (Integer) value));
    }
    if (value instanceof Long) {
      return Optional.of(appendLongs(length, (Long) value));
    }
    if (value instanceof Short) {
      return Optional.of(appendShorts(length, (Short) value));
    }
    if (value instanceof UTF8String utf8) {
      byte[] bytes = utf8.getBytes();
      int result = 0;
      for (int i = 0; i < length; ++i) {
        result += appendByteArray(bytes, 0, bytes.length);
      }
      return Optional.of(result);
    }
    if (value instanceof GenericArrayData arrayData) {
      int result = 0;
      for (int i = 0; i < length; ++i) {
        appendArray(arrayData.numElements());
        for (Object element : arrayData.array()) {
          if (arrayData().appendObjects(1, element).isEmpty()) {
            return Optional.empty();
          }
        }
        result += arrayData.numElements();
      }
      return Optional.of(result);
    }
    if (value instanceof GenericInternalRow row) {
      int result = 0;
      for (int i = 0; i < length; ++i) {
        appendStruct(false);
        for (int j = 0; j < row.values().length; ++j) {
          Object element = row.values()[j];
          if (childColumns[j].appendObjects(1, element).isEmpty()) {
            return Optional.empty();
          }
        }
        result += row.values().length;
      }
      return Optional.of(result);
    }
    if (value instanceof ArrayBasedMapData data) {
      appendArray(length);
      int result = 0;
      for (int i = 0; i < length; ++i) {
        for (Object key : data.keyArray().array()) {
          if (childColumns[0].appendObjects(1, key).isEmpty()) {
            return Optional.empty();
          }
        }
        for (Object val: data.valueArray().array()) {
          if (childColumns[1].appendObjects(1, val).isEmpty()) {
            return Optional.empty();
          }
        }
        result += data.keyArray().numElements();
      }
      return Optional.of(result);
    }
    return Optional.empty();
  }

  // `WritableColumnVector` puts the data of array in the first child column vector, and puts the
  // array offsets and lengths in the current column vector.
  @Override
  public final ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) return null;
    return new ColumnarArray(arrayData(), getArrayOffset(rowId), getArrayLength(rowId));
  }

  // `WritableColumnVector` puts the key array in the first child column vector, value array in the
  // second child column vector, and puts the offsets and lengths in the current column vector.
  @Override
  public final ColumnarMap getMap(int rowId) {
    if (isNullAt(rowId)) return null;
    return new ColumnarMap(getChild(0), getChild(1), getArrayOffset(rowId), getArrayLength(rowId));
  }

  public WritableColumnVector arrayData() {
    return childColumns[0];
  }

  public abstract int getArrayLength(int rowId);

  public abstract int getArrayOffset(int rowId);

  @Override
  public WritableColumnVector getChild(int ordinal) { return childColumns[ordinal]; }

  /**
   * Returns the number of child vectors.
   */
  public int getNumChildren() {
    return childColumns.length;
  }

  /**
   * Returns the elements appended. This is useful
   */
  public final int getElementsAppended() { return elementsAppended; }

  /**
   * Increment number of elements appended by 'num'.
   *
   * This is useful when one wants to use the 'putXXX' API to add new elements to the vector, but
   * still want to keep count of how many elements have been added (since the 'putXXX' APIs don't
   * increment count).
   */
  public final void addElementsAppended(int num) {
    elementsAppended += num;
  }

  /**
   * Marks this column as being constant.
   */
  public final void setIsConstant() {
    if (childColumns != null) {
      for (WritableColumnVector c : childColumns) {
        c.setIsConstant();
      }
    }
    isConstant = true;
  }

  /**
   * Marks this column only contains null values.
   */
  public final void setAllNull() {
    isAllNull = true;
  }

  /**
   * Whether this column only contains null values.
   */
  public final boolean isAllNull() {
    return isAllNull;
  }

  /**
   * Maximum number of rows that can be stored in this column.
   */
  protected int capacity;

  /**
   * The default number of rows that can be stored in this column.
   */
  protected final int defaultCapacity;

  /**
   * Upper limit for the maximum capacity for this column.
   */
  @VisibleForTesting
  protected int MAX_CAPACITY = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

  protected int hugeVectorThreshold;

  protected double hugeVectorReserveRatio;

  /**
   * Number of nulls in this column. This is an optimization for the reader, to skip NULL checks.
   */
  protected int numNulls;

  /**
   * True if this column's values are fixed. This means the column values never change, even
   * across resets.
   */
  protected boolean isConstant;

  /**
   * True if this column only contains nulls. This means the column values never change, even
   * across resets. Comparing to 'isConstant' above, this doesn't require any allocation of space.
   */
  protected boolean isAllNull;

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
  protected WritableColumnVector[] childColumns;

  /**
   * Reserve a new column.
   */
  protected abstract WritableColumnVector reserveNewColumn(int capacity, DataType type);

  protected boolean isArray() {
    return type instanceof ArrayType || type instanceof BinaryType || type instanceof StringType ||
      DecimalType.isByteArrayDecimalType(type);
  }

  /**
   * Sets up the common state and also handles creating the child columns if this is a nested
   * type.
   */
  protected WritableColumnVector(int capacity, DataType dataType) {
    super(dataType);
    this.capacity = capacity;
    this.defaultCapacity = capacity;
    this.hugeVectorThreshold = SQLConf.get().vectorizedHugeVectorThreshold();
    this.hugeVectorReserveRatio = SQLConf.get().vectorizedHugeVectorReserveRatio();

    if (isArray()) {
      DataType childType;
      int childCapacity = capacity;
      if (type instanceof ArrayType) {
        childType = ((ArrayType)type).elementType();
      } else {
        childType = DataTypes.ByteType;
        childCapacity *= DEFAULT_ARRAY_LENGTH;
      }
      this.childColumns = new WritableColumnVector[1];
      this.childColumns[0] = reserveNewColumn(childCapacity, childType);
    } else if (type instanceof StructType st) {
      this.childColumns = new WritableColumnVector[st.fields().length];
      for (int i = 0; i < childColumns.length; ++i) {
        this.childColumns[i] = reserveNewColumn(capacity, st.fields()[i].dataType());
      }
    } else if (type instanceof MapType mapType) {
      this.childColumns = new WritableColumnVector[2];
      this.childColumns[0] = reserveNewColumn(capacity, mapType.keyType());
      this.childColumns[1] = reserveNewColumn(capacity, mapType.valueType());
    } else if (type instanceof CalendarIntervalType) {
      // Three columns. Months as int. Days as Int. Microseconds as Long.
      this.childColumns = new WritableColumnVector[3];
      this.childColumns[0] = reserveNewColumn(capacity, DataTypes.IntegerType);
      this.childColumns[1] = reserveNewColumn(capacity, DataTypes.IntegerType);
      this.childColumns[2] = reserveNewColumn(capacity, DataTypes.LongType);
    } else if (type instanceof VariantType) {
      this.childColumns = new WritableColumnVector[2];
      this.childColumns[0] = reserveNewColumn(capacity, DataTypes.BinaryType);
      this.childColumns[1] = reserveNewColumn(capacity, DataTypes.BinaryType);
    } else {
      this.childColumns = null;
    }
  }
}
