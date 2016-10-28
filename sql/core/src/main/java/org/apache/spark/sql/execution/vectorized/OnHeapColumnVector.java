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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * A column backed by an in memory JVM array. This stores the NULLs as a byte per value
 * and a java array for the values.
 */
public final class OnHeapColumnVector extends ColumnVector {

  private static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

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
  public long valuesNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }
  @Override
  public long nullsNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }

  @Override
  public void close() {
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
    anyNullsSet = true;
  }

  @Override
  public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; ++i) {
      nulls[rowId + i] = (byte)1;
    }
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!anyNullsSet) return;
    for (int i = 0; i < count; ++i) {
      nulls[rowId + i] = (byte)0;
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nulls[rowId] == 1;
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
  public boolean getBoolean(int rowId) {
    return byteData[rowId] == 1;
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
  public short getShort(int rowId) {
    if (dictionary == null) {
      return shortData[rowId];
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
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
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, floatData,
          Platform.DOUBLE_ARRAY_OFFSET + rowId * 4, count * 4);
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
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, doubleData,
          Platform.DOUBLE_ARRAY_OFFSET + rowId * 8, count * 8);
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

  @Override
  public void loadBytes(ColumnVector.Array array) {
    array.byteArray = byteData;
    array.byteArrayOffset = array.offset;
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
      if (byteData == null || byteData.length < newCapacity) {
        byte[] newData = new byte[newCapacity];
        if (byteData != null) System.arraycopy(byteData, 0, newData, 0, elementsAppended);
        byteData = newData;
      }
    } else if (type instanceof ByteType) {
      if (byteData == null || byteData.length < newCapacity) {
        byte[] newData = new byte[newCapacity];
        if (byteData != null) System.arraycopy(byteData, 0, newData, 0, elementsAppended);
        byteData = newData;
      }
    } else if (type instanceof ShortType) {
      if (shortData == null || shortData.length < newCapacity) {
        short[] newData = new short[newCapacity];
        if (shortData != null) System.arraycopy(shortData, 0, newData, 0, elementsAppended);
        shortData = newData;
      }
    } else if (type instanceof IntegerType || type instanceof DateType ||
      DecimalType.is32BitDecimalType(type)) {
      if (intData == null || intData.length < newCapacity) {
        int[] newData = new int[newCapacity];
        if (intData != null) System.arraycopy(intData, 0, newData, 0, elementsAppended);
        intData = newData;
      }
    } else if (type instanceof LongType || type instanceof TimestampType ||
        DecimalType.is64BitDecimalType(type)) {
      if (longData == null || longData.length < newCapacity) {
        long[] newData = new long[newCapacity];
        if (longData != null) System.arraycopy(longData, 0, newData, 0, elementsAppended);
        longData = newData;
      }
    } else if (type instanceof FloatType) {
      if (floatData == null || floatData.length < newCapacity) {
        float[] newData = new float[newCapacity];
        if (floatData != null) System.arraycopy(floatData, 0, newData, 0, elementsAppended);
        floatData = newData;
      }
    } else if (type instanceof DoubleType) {
      if (doubleData == null || doubleData.length < newCapacity) {
        double[] newData = new double[newCapacity];
        if (doubleData != null) System.arraycopy(doubleData, 0, newData, 0, elementsAppended);
        doubleData = newData;
      }
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
