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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column backed by an in memory JVM array. This stores the NULLs as a byte per value
 * and a java array for the values.
 */
public final class OnHeapColumnVector extends WritableColumnVector {

  private static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  /**
   * Allocates columns to store elements of each field of the schema on heap.
   * Capacity is the initial capacity of the vector and it will grow as necessary. Capacity is
   * in number of elements, not number of bytes.
   */
  public static OnHeapColumnVector[] allocateColumns(int capacity, StructType schema) {
    return allocateColumns(capacity, schema.fields());
  }

  /**
   * Allocates columns to store elements of each field on heap.
   * Capacity is the initial capacity of the vector and it will grow as necessary. Capacity is
   * in number of elements, not number of bytes.
   */
  public static OnHeapColumnVector[] allocateColumns(int capacity, StructField[] fields) {
    OnHeapColumnVector[] vectors = new OnHeapColumnVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      vectors[i] = new OnHeapColumnVector(capacity, fields[i].dataType());
    }
    return vectors;
  }

  // The data stored in these arrays need to maintain binary compatible. We can
  // directly pass this buffer to external components.

  private BitSet nulls;
  // Lazily allocated array for all data types.
  // For Map & Array type, the offset & length pair is packed together in a single long (length << 32 | offset).
  private byte[] data = null;

  // The number of bytes each element takes in the byte array used to compute the array size when we pack multi-byte values.
  // The size is always a power of 2, we store the shift amount to avoid slow multiplication.
  private final int elementSizeShift;

  private byte[] ensureData() {
    if (data == null) data = new byte[(int) getDataSize(capacity)];
    return data;
  }

  private long getDataSize(long N) { return N << elementSizeShift; }
  private long getDataOffset(int index) { return Platform.BYTE_ARRAY_OFFSET + getDataSize(index); }

  public OnHeapColumnVector(int capacity, DataType dataType) {
    super(capacity, dataType);

    if (type instanceof BooleanType || type instanceof ByteType) {
      elementSizeShift = 0;
    } else if (type instanceof ShortType) {
      elementSizeShift = 1;
    } else if (DecimalType.is32BitDecimalType(type) ||
            type instanceof IntegerType ||
            type instanceof FloatType ||
            type instanceof DateType ||
            type instanceof YearMonthIntervalType) {
      elementSizeShift = 2;
    } else if (isArray() ||
            DecimalType.is64BitDecimalType(type) ||
            type instanceof DoubleType ||
            type instanceof MapType ||
            type instanceof LongType ||
            type instanceof TimestampType ||
            type instanceof TimestampNTZType ||
            type instanceof DayTimeIntervalType) {
      elementSizeShift = 3;
    } else if (childColumns != null) {
      // Nothing to store.
      elementSizeShift = 0;
    } else {
      throw new RuntimeException("Unhandled " + type);
    }

    reserveInternal(capacity);
    reset();
  }

  @Override
  public void close() {
    super.close();
    nulls = null;
    data = null;
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    nulls.clear(rowId);
  }

  @Override
  public void putNull(int rowId) {
    nulls.set(rowId);
    ++numNulls;
  }

  @Override
  public void putNulls(int rowId, int count) {
    nulls.set(rowId, rowId + count);
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!hasNull()) return;
    nulls.clear(rowId, rowId + count);
  }

  @Override
  public boolean isNullAt(int rowId) {
    return isAllNull || nulls.get(rowId);
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    putByte(rowId, (byte)((value) ? 1 : 0));
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    for (int i = 0; i < count; ++i) {
      putByte(rowId + i, v);
    }
  }

  @Override
  public void putBooleans(int rowId, byte src) {
    putByte(rowId, (byte)(src & 1));
    putByte(rowId + 1, (byte)(src >>> 1 & 1));
    putByte(rowId + 2, (byte)(src >>> 2 & 1));
    putByte(rowId + 3, (byte)(src >>> 3 & 1));
    putByte(rowId + 4, (byte)(src >>> 4 & 1));
    putByte(rowId + 5, (byte)(src >>> 5 & 1));
    putByte(rowId + 6, (byte)(src >>> 6 & 1));
    putByte(rowId + 7, (byte)(src >>> 7 & 1));
  }

  @Override
  public boolean getBoolean(int rowId) {
    return getByte(rowId) == 1;
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    assert(dictionary == null);
    boolean[] array = new boolean[count];
    for (int i = 0; i < count; ++i) {
      array[i] = (getByte(rowId + i) == 1);
    }
   return array;
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    Platform.putByte(ensureData(), getDataOffset(rowId), value);
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    Arrays.fill(ensureData(), rowId, rowId + count, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, ensureData(), getDataOffset(rowId), getDataSize(count));
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      return data == null ? 0 : Platform.getByte(data, getDataOffset(rowId));
    } else {
      return (byte) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    assert(dictionary == null);
    byte[] array = new byte[count];
    if (data == null) return array;
    Platform.copyMemory(data, getDataOffset(rowId), array, Platform.BYTE_ARRAY_OFFSET, getDataSize(count));
    return array;
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    return UTF8String.fromBytes(ensureData(), rowId, count);
  }

  @Override
  public ByteBuffer getByteBuffer(int rowId, int count) {
    return ByteBuffer.wrap(ensureData(), rowId, count);
  }


  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    Platform.putShort(ensureData(), getDataOffset(rowId), value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    ensureData();
    for (int i = 0; i < count; ++i) {
      Platform.putShort(data, getDataOffset(rowId + i), value);
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + getDataSize(srcIndex), ensureData(), getDataOffset(rowId), getDataSize(count));
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    assert(elementSizeShift == 1);
    putBytes(rowId, count, src, srcIndex);
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      return data == null ? 0 : Platform.getShort(data, getDataOffset(rowId));
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    assert(dictionary == null);
    short[] array = new short[count];
    if (data == null) return array;
    Platform.copyMemory(data, getDataOffset(rowId), array, Platform.SHORT_ARRAY_OFFSET, getDataSize(count));
    return array;
  }


  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    Platform.putInt(ensureData(), getDataOffset(rowId), value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    ensureData();
    for (int i = 0; i < count; ++i) {
      Platform.putInt(data, getDataOffset(rowId + i), value);
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    assert(elementSizeShift == 2);
    Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + getDataSize(srcIndex), ensureData(), getDataOffset(rowId), getDataSize(count));
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    putBytes(rowId, count, src, srcIndex);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i, srcOffset += 4) {
      putInt(i + rowId, Platform.getInt(src, srcOffset));
      if (bigEndianPlatform) {
        putInt(i + rowId, java.lang.Integer.reverseBytes(getInt(i + rowId)));
      }
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      return data == null ? 0 : Platform.getInt(data, getDataOffset(rowId));
    } else {
      return dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public int[] getInts(int rowId, int count) {
    assert(dictionary == null);
    int[] array = new int[count];
    if (data == null) return array;
    Platform.copyMemory(data, getDataOffset(rowId), array, Platform.INT_ARRAY_OFFSET, getDataSize(count));
    return array;
  }

  /**
   * Returns the dictionary Id for rowId.
   * This should only be called when the ColumnVector is dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  @Override
  public int getDictId(int rowId) {
    assert(dictionary == null)
            : "A ColumnVector dictionary should not have a dictionary for itself.";
    return getInt(rowId);
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(ensureData(), getDataOffset(rowId), value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    ensureData();
    for (int i = 0; i < count; ++i) {
      Platform.putLong(data, getDataOffset(rowId + i), value);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    assert(elementSizeShift == 3);
    Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + getDataSize(srcIndex), ensureData(), getDataOffset(rowId), getDataSize(count));
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    putBytes(rowId, count, src, srcIndex);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i, srcOffset += 8) {
      putLong(i + rowId, Platform.getLong(src, srcOffset));
      if (bigEndianPlatform) {
        putLong(i + rowId, java.lang.Long.reverseBytes(getLong(i + rowId)));
      }
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      return data == null ? 0 : Platform.getLong(data, getDataOffset(rowId));
    } else {
      return dictionary.decodeToLong(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    assert(dictionary == null);
    long[] array = new long[count];
    if (data == null) return array;
    Platform.copyMemory(data, getDataOffset(rowId), array, Platform.LONG_ARRAY_OFFSET, getDataSize(count));
    return array;
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    Platform.putFloat(ensureData(), getDataOffset(rowId), value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    ensureData();
    for (int i = 0; i < count; ++i) {
      Platform.putFloat(data, getDataOffset(rowId + i), value);
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    assert(elementSizeShift == 2);
    Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + getDataSize(srcIndex), ensureData(), getDataOffset(rowId), getDataSize(count));
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    putBytes(rowId, count, src, srcIndex);
  }

  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      putFloats(rowId, count, src, srcIndex);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < count; ++i) {
        putFloat(i + rowId, bb.getFloat(srcIndex + (4 * i)));
      }
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      return data == null ? 0 : Platform.getFloat(data, getDataOffset(rowId));
    } else {
      return dictionary.decodeToFloat(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    assert(dictionary == null);
    float[] array = new float[count];
    if (data == null) return array;
    Platform.copyMemory(data, getDataOffset(rowId), array, Platform.FLOAT_ARRAY_OFFSET, getDataSize(count));
    return array;
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    Platform.putDouble(ensureData(), getDataOffset(rowId), value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    ensureData();
    for (int i = 0; i < count; ++i) {
      Platform.putDouble(data, getDataOffset(rowId + i), value);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    assert(elementSizeShift == 3);
    Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + getDataSize(srcIndex), ensureData(), getDataOffset(rowId), getDataSize(count));
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    putBytes(rowId, count, src, srcIndex);
  }

  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      putDoubles(rowId, count, src, srcIndex);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < count; ++i) {
        putDouble(i + rowId, bb.getDouble(srcIndex + (8 * i)));
      }
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      return data == null ? 0 : Platform.getDouble(data, getDataOffset(rowId));
    } else {
      return dictionary.decodeToDouble(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    assert(dictionary == null);
    double[] array = new double[count];
    if (data == null) return array;
    Platform.copyMemory(data, getDataOffset(rowId), array, Platform.DOUBLE_ARRAY_OFFSET, getDataSize(count));
    return array;
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    long pair = getLong(rowId);
    return (int) (pair >>> 32);
  }

  @Override
  public int getArrayOffset(int rowId) {
    long pair = getLong(rowId);
    return (int) (pair & 0xFFFFFFFFL);
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    long pair = ((long) length << 32) | (offset & 0xFFFFFFFFL);
    putLong(rowId, pair);
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    putArray(rowId, result, length);
    return result;
  }

  // Spilt this function out since it is the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    assert(newCapacity > capacity || data == null);

    if (data != null) {
      byte[] newData = new byte[(int) getDataSize(newCapacity)];
      System.arraycopy(data, 0, newData, 0, (int) getDataSize(capacity));
      data = newData;
    }

    BitSet newNulls = new BitSet(newCapacity);
    if (nulls != null) newNulls.or(nulls);

    capacity = newCapacity;
  }

  @Override
  protected OnHeapColumnVector reserveNewColumn(int capacity, DataType type) {
    return new OnHeapColumnVector(capacity, type);
  }
}
