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

  // This is faster than a boolean array and we optimize this over memory footprint.
  private byte[] nulls;

  // Array for each type. Only 1 is populated for any type.
  private byte[] byteData;
  private short[] shortData;
  private int[] intData;
  private long[] longData;
  private float[] floatData;
  private double[] doubleData;

  // Only set if type is Array or Map.
  private int[] arrayLengths;
  private int[] arrayOffsets;

  public OnHeapColumnVector(int capacity, DataType type) {
    super(capacity, type);

    reserveInternal(capacity);
    reset();
  }

  @Override
  public void close() {
    super.close();
    nulls = null;
    byteData = null;
    shortData = null;
    intData = null;
    longData = null;
    floatData = null;
    doubleData = null;
    arrayLengths = null;
    arrayOffsets = null;
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    nulls[rowId] = (byte)0;
  }

  @Override
  public void putNull(int rowId) {
    nulls[rowId] = (byte)1;
    ++numNulls;
  }

  @Override
  public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; ++i) {
      nulls[rowId + i] = (byte)1;
    }
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!hasNull()) return;
    for (int i = 0; i < count; ++i) {
      nulls[rowId + i] = (byte)0;
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    return isAllNull || nulls[rowId] == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    byteData[rowId] = (byte)((value) ? 1 : 0);
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    for (int i = 0; i < count; ++i) {
      byteData[i + rowId] = v;
    }
  }

  @Override
  public void putBooleans(int rowId, byte src) {
    byteData[rowId] = (byte)(src & 1);
    byteData[rowId + 1] = (byte)(src >>> 1 & 1);
    byteData[rowId + 2] = (byte)(src >>> 2 & 1);
    byteData[rowId + 3] = (byte)(src >>> 3 & 1);
    byteData[rowId + 4] = (byte)(src >>> 4 & 1);
    byteData[rowId + 5] = (byte)(src >>> 5 & 1);
    byteData[rowId + 6] = (byte)(src >>> 6 & 1);
    byteData[rowId + 7] = (byte)(src >>> 7 & 1);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return byteData[rowId] == 1;
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    assert(dictionary == null);
    boolean[] array = new boolean[count];
    for (int i = 0; i < count; ++i) {
      array[i] = (byteData[rowId + i] == 1);
    }
   return array;
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    byteData[rowId] = value;
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    for (int i = 0; i < count; ++i) {
      byteData[i + rowId] = value;
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, byteData, rowId, count);
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      return byteData[rowId];
    } else {
      return (byte) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    assert(dictionary == null);
    byte[] array = new byte[count];
    System.arraycopy(byteData, rowId, array, 0, count);
    return array;
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    return UTF8String.fromBytes(byteData, rowId, count);
  }

  @Override
  public ByteBuffer getByteBuffer(int rowId, int count) {
    return ByteBuffer.wrap(byteData, rowId, count);
  }


  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    shortData[rowId] = value;
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    for (int i = 0; i < count; ++i) {
      shortData[i + rowId] = value;
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, shortData, rowId, count);
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, shortData,
      Platform.SHORT_ARRAY_OFFSET + rowId * 2L, count * 2L);
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      return shortData[rowId];
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    assert(dictionary == null);
    short[] array = new short[count];
    System.arraycopy(shortData, rowId, array, 0, count);
    return array;
  }


  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    intData[rowId] = value;
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    for (int i = 0; i < count; ++i) {
      intData[i + rowId] = value;
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, intData, rowId, count);
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, intData,
      Platform.INT_ARRAY_OFFSET + rowId * 4L, count * 4L);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i, srcOffset += 4) {
      intData[i + rowId] = Platform.getInt(src, srcOffset);
      if (bigEndianPlatform) {
        intData[i + rowId] = java.lang.Integer.reverseBytes(intData[i + rowId]);
      }
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      return intData[rowId];
    } else {
      return dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public int[] getInts(int rowId, int count) {
    assert(dictionary == null);
    int[] array = new int[count];
    System.arraycopy(intData, rowId, array, 0, count);
    return array;
  }

  /**
   * Returns the dictionary Id for rowId.
   * This should only be called when the ColumnVector is dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  public int getDictId(int rowId) {
    assert(dictionary == null)
            : "A ColumnVector dictionary should not have a dictionary for itself.";
    return intData[rowId];
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    longData[rowId] = value;
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    for (int i = 0; i < count; ++i) {
      longData[i + rowId] = value;
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, longData, rowId, count);
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, longData,
      Platform.LONG_ARRAY_OFFSET + rowId * 8L, count * 8L);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i, srcOffset += 8) {
      longData[i + rowId] = Platform.getLong(src, srcOffset);
      if (bigEndianPlatform) {
        longData[i + rowId] = java.lang.Long.reverseBytes(longData[i + rowId]);
      }
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      return longData[rowId];
    } else {
      return dictionary.decodeToLong(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    assert(dictionary == null);
    long[] array = new long[count];
    System.arraycopy(longData, rowId, array, 0, count);
    return array;
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) { floatData[rowId] = value; }

  @Override
  public void putFloats(int rowId, int count, float value) {
    Arrays.fill(floatData, rowId, rowId + count, value);
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, floatData, rowId, count);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, floatData,
        Platform.FLOAT_ARRAY_OFFSET + rowId * 4L, count * 4L);
  }

  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      putFloats(rowId, count, src, srcIndex);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < count; ++i) {
        floatData[i + rowId] = bb.getFloat(srcIndex + (4 * i));
      }
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      return floatData[rowId];
    } else {
      return dictionary.decodeToFloat(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    assert(dictionary == null);
    float[] array = new float[count];
    System.arraycopy(floatData, rowId, array, 0, count);
    return array;
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    doubleData[rowId] = value;
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    Arrays.fill(doubleData, rowId, rowId + count, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, doubleData, rowId, count);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, doubleData,
        Platform.DOUBLE_ARRAY_OFFSET + rowId * 8L, count * 8L);
  }

  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      putDoubles(rowId, count, src, srcIndex);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < count; ++i) {
        doubleData[i + rowId] = bb.getDouble(srcIndex + (8 * i));
      }
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      return doubleData[rowId];
    } else {
      return dictionary.decodeToDouble(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    assert(dictionary == null);
    double[] array = new double[count];
    System.arraycopy(doubleData, rowId, array, 0, count);
    return array;
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    return arrayLengths[rowId];
  }
  @Override
  public int getArrayOffset(int rowId) {
    return arrayOffsets[rowId];
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    arrayOffsets[rowId] = offset;
    arrayLengths[rowId] = length;
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    arrayOffsets[rowId] = result;
    arrayLengths[rowId] = length;
    return result;
  }

  // Spilt this function out since it is the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    if (isArray() || type instanceof MapType) {
      int[] newLengths = new int[newCapacity];
      int[] newOffsets = new int[newCapacity];
      if (this.arrayLengths != null) {
        System.arraycopy(this.arrayLengths, 0, newLengths, 0, capacity);
        System.arraycopy(this.arrayOffsets, 0, newOffsets, 0, capacity);
      }
      arrayLengths = newLengths;
      arrayOffsets = newOffsets;
    } else if (type instanceof BooleanType) {
      if (byteData == null || byteData.length < newCapacity) {
        byte[] newData = new byte[newCapacity];
        if (byteData != null) System.arraycopy(byteData, 0, newData, 0, capacity);
        byteData = newData;
      }
    } else if (type instanceof ByteType) {
      if (byteData == null || byteData.length < newCapacity) {
        byte[] newData = new byte[newCapacity];
        if (byteData != null) System.arraycopy(byteData, 0, newData, 0, capacity);
        byteData = newData;
      }
    } else if (type instanceof ShortType) {
      if (shortData == null || shortData.length < newCapacity) {
        short[] newData = new short[newCapacity];
        if (shortData != null) System.arraycopy(shortData, 0, newData, 0, capacity);
        shortData = newData;
      }
    } else if (type instanceof IntegerType || type instanceof DateType ||
      DecimalType.is32BitDecimalType(type) || type instanceof YearMonthIntervalType) {
      if (intData == null || intData.length < newCapacity) {
        int[] newData = new int[newCapacity];
        if (intData != null) System.arraycopy(intData, 0, newData, 0, capacity);
        intData = newData;
      }
    } else if (type instanceof LongType ||
        type instanceof TimestampType ||type instanceof TimestampNTZType ||
        DecimalType.is64BitDecimalType(type) || type instanceof DayTimeIntervalType) {
      if (longData == null || longData.length < newCapacity) {
        long[] newData = new long[newCapacity];
        if (longData != null) System.arraycopy(longData, 0, newData, 0, capacity);
        longData = newData;
      }
    } else if (type instanceof FloatType) {
      if (floatData == null || floatData.length < newCapacity) {
        float[] newData = new float[newCapacity];
        if (floatData != null) System.arraycopy(floatData, 0, newData, 0, capacity);
        floatData = newData;
      }
    } else if (type instanceof DoubleType) {
      if (doubleData == null || doubleData.length < newCapacity) {
        double[] newData = new double[newCapacity];
        if (doubleData != null) System.arraycopy(doubleData, 0, newData, 0, capacity);
        doubleData = newData;
      }
    } else if (childColumns != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }

    byte[] newNulls = new byte[newCapacity];
    if (nulls != null) System.arraycopy(nulls, 0, newNulls, 0, capacity);
    nulls = newNulls;

    capacity = newCapacity;
  }

  @Override
  protected OnHeapColumnVector reserveNewColumn(int capacity, DataType type) {
    return new OnHeapColumnVector(capacity, type);
  }
}
