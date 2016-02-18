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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector.Array;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

import java.util.Arrays;

/**
 * A column backed by an in memory JVM array. This stores the NULLs as a byte per value
 * and a java array for the values.
 */
public final class OnHeapColumnVector extends ColumnVector {
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

  // Only set if type is Array.
  private int[] arrayLengths;
  private int[] arrayOffsets;

  protected OnHeapColumnVector(int capacity, DataType type) {
    super(capacity, type, MemoryMode.ON_HEAP);
    reserveInternal(capacity);
    reset();
  }

  @Override
  public final long valuesNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }
  @Override
  public final long nullsNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }

  @Override
  public final void close() {
    nulls = null;
    intData = null;
    doubleData = null;
  }


  //
  // APIs dealing with nulls
  //

  @Override
  public final void putNotNull(int rowId) {
    nulls[rowId] = (byte)0;
  }

  @Override
  public final void putNull(int rowId) {
    nulls[rowId] = (byte)1;
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public final void putNulls(int rowId, int count) {
    for (int i = 0; i < count; ++i) {
      nulls[rowId + i] = (byte)1;
    }
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public final void putNotNulls(int rowId, int count) {
    if (!anyNullsSet) return;
    for (int i = 0; i < count; ++i) {
      nulls[rowId + i] = (byte)0;
    }
  }

  @Override
  public final boolean getIsNull(int rowId) {
    return nulls[rowId] == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public final void putBoolean(int rowId, boolean value) {
    byteData[rowId] = (byte)((value) ? 1 : 0);
  }

  @Override
  public final void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    for (int i = 0; i < count; ++i) {
      byteData[i + rowId] = v;
    }
  }

  @Override
  public final boolean getBoolean(int rowId) {
    return byteData[rowId] == 1;
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public final void putByte(int rowId, byte value) {
    byteData[rowId] = value;
  }

  @Override
  public final void putBytes(int rowId, int count, byte value) {
    for (int i = 0; i < count; ++i) {
      byteData[i + rowId] = value;
    }
  }

  @Override
  public final void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, byteData, rowId, count);
  }

  @Override
  public final byte getByte(int rowId) {
    return byteData[rowId];
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public final void putShort(int rowId, short value) {
    shortData[rowId] = value;
  }

  @Override
  public final void putShorts(int rowId, int count, short value) {
    for (int i = 0; i < count; ++i) {
      shortData[i + rowId] = value;
    }
  }

  @Override
  public final void putShorts(int rowId, int count, short[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, shortData, rowId, count);
  }

  @Override
  public final short getShort(int rowId) {
    return shortData[rowId];
  }


  //
  // APIs dealing with Ints
  //

  @Override
  public final void putInt(int rowId, int value) {
    intData[rowId] = value;
  }

  @Override
  public final void putInts(int rowId, int count, int value) {
    for (int i = 0; i < count; ++i) {
      intData[i + rowId] = value;
    }
  }

  @Override
  public final void putInts(int rowId, int count, int[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, intData, rowId, count);
  }

  @Override
  public final void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i) {
      intData[i + rowId] = Platform.getInt(src, srcOffset);;
      srcIndex += 4;
      srcOffset += 4;
    }
  }

  @Override
  public final int getInt(int rowId) {
    return intData[rowId];
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public final void putLong(int rowId, long value) {
    longData[rowId] = value;
  }

  @Override
  public final void putLongs(int rowId, int count, long value) {
    for (int i = 0; i < count; ++i) {
      longData[i + rowId] = value;
    }
  }

  @Override
  public final void putLongs(int rowId, int count, long[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, longData, rowId, count);
  }

  @Override
  public final void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i) {
      longData[i + rowId] = Platform.getLong(src, srcOffset);
      srcIndex += 8;
      srcOffset += 8;
    }
  }

  @Override
  public final long getLong(int rowId) {
    return longData[rowId];
  }

  //
  // APIs dealing with floats
  //

  @Override
  public final void putFloat(int rowId, float value) { floatData[rowId] = value; }

  @Override
  public final void putFloats(int rowId, int count, float value) {
    Arrays.fill(floatData, rowId, rowId + count, value);
  }

  @Override
  public final void putFloats(int rowId, int count, float[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, floatData, rowId, count);
  }

  @Override
  public final void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
        floatData, Platform.DOUBLE_ARRAY_OFFSET + rowId * 4, count * 4);
  }

  @Override
  public final float getFloat(int rowId) { return floatData[rowId]; }

  //
  // APIs dealing with doubles
  //

  @Override
  public final void putDouble(int rowId, double value) {
    doubleData[rowId] = value;
  }

  @Override
  public final void putDoubles(int rowId, int count, double value) {
    Arrays.fill(doubleData, rowId, rowId + count, value);
  }

  @Override
  public final void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, doubleData, rowId, count);
  }

  @Override
  public final void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, doubleData,
        Platform.DOUBLE_ARRAY_OFFSET + rowId * 8, count * 8);
  }

  @Override
  public final double getDouble(int rowId) {
    return doubleData[rowId];
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public final int getArrayLength(int rowId) {
    return arrayLengths[rowId];
  }
  @Override
  public final int getArrayOffset(int rowId) {
    return arrayOffsets[rowId];
  }

  @Override
  public final void putArray(int rowId, int offset, int length) {
    arrayOffsets[rowId] = offset;
    arrayLengths[rowId] = length;
  }

  @Override
  public final void loadBytes(ColumnVector.Array array) {
    array.byteArray = byteData;
    array.byteArrayOffset = array.offset;
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public final int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    arrayOffsets[rowId] = result;
    arrayLengths[rowId] = length;
    return result;
  }

  @Override
  public final void reserve(int requiredCapacity) {
    if (requiredCapacity > capacity) reserveInternal(requiredCapacity * 2);
  }

  // Spilt this function out since it is the slow path.
  private final void reserveInternal(int newCapacity) {
    if (this.resultArray != null || DecimalType.isByteArrayDecimalType(type)) {
      int[] newLengths = new int[newCapacity];
      int[] newOffsets = new int[newCapacity];
      if (this.arrayLengths != null) {
        System.arraycopy(this.arrayLengths, 0, newLengths, 0, elementsAppended);
        System.arraycopy(this.arrayOffsets, 0, newOffsets, 0, elementsAppended);
      }
      arrayLengths = newLengths;
      arrayOffsets = newOffsets;
    } else if (type instanceof BooleanType) {
      byte[] newData = new byte[newCapacity];
      if (byteData != null) System.arraycopy(byteData, 0, newData, 0, elementsAppended);
      byteData = newData;
    } else if (type instanceof ByteType) {
      byte[] newData = new byte[newCapacity];
      if (byteData != null) System.arraycopy(byteData, 0, newData, 0, elementsAppended);
      byteData = newData;
    } else if (type instanceof ShortType) {
      short[] newData = new short[newCapacity];
      if (shortData != null) System.arraycopy(shortData, 0, newData, 0, elementsAppended);
      shortData = newData;
    } else if (type instanceof IntegerType || type instanceof DateType) {
      int[] newData = new int[newCapacity];
      if (intData != null) System.arraycopy(intData, 0, newData, 0, elementsAppended);
      intData = newData;
    } else if (type instanceof LongType || DecimalType.is64BitDecimalType(type)) {
      long[] newData = new long[newCapacity];
      if (longData != null) System.arraycopy(longData, 0, newData, 0, elementsAppended);
      longData = newData;
    } else if (type instanceof FloatType) {
      float[] newData = new float[newCapacity];
      if (floatData != null) System.arraycopy(floatData, 0, newData, 0, elementsAppended);
      floatData = newData;
    } else if (type instanceof DoubleType) {
      double[] newData = new double[newCapacity];
      if (doubleData != null) System.arraycopy(doubleData, 0, newData, 0, elementsAppended);
      doubleData = newData;
    } else if (resultStruct != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }

    byte[] newNulls = new byte[newCapacity];
    if (nulls != null) System.arraycopy(nulls, 0, newNulls, 0, elementsAppended);
    nulls = newNulls;

    capacity = newCapacity;
  }
}
