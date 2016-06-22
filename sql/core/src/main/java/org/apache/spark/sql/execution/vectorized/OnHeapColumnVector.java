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
 * A column backed by an in memory JVM array.
 */
public final class OnHeapColumnVector extends ColumnVector {
  protected OnHeapColumnVector(int capacity, int childCapacity, DataType type, boolean isConstant) {
    super(capacity, childCapacity, type, MemoryMode.ON_HEAP, isConstant);

    this.isConstant = isConstant;
    reserveInternal(capacity);
    reset();
  }

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

  // Spilt this function out since it is the slow path.
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

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    if (isConstant) {
      nulls[0] = (byte)0;
    } else {
      nulls[rowId] = (byte)0;
    }
  }

  @Override
  public void putNull(int rowId) {
    if (isConstant) {
      nulls[0] = (byte)1;
    } else {
      nulls[rowId] = (byte)1;
    }
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public void putNulls(int rowId, int count) {
    if (isConstant) {
      nulls[0] = (byte)1;
    } else {
      for (int i = 0; i < count; ++i) {
        nulls[rowId + i] = (byte)1;
      }
    }
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!anyNullsSet) return;
    if (isConstant) {
      nulls[0] = (byte)0;
    } else {
      for (int i = 0; i < count; ++i) {
        nulls[rowId + i] = (byte)0;
      }
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (isConstant) {
      return nulls[0] == 1;
    } else {
      return nulls[rowId] == 1;
    }
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    if (isConstant) {
      byteData[0] = (byte)((value) ? 1 : 0);
    } else {
      byteData[rowId] = (byte)((value) ? 1 : 0);
    }
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    if (isConstant) {
      byteData[0] = v;
    } else {
      for (int i = 0; i < count; ++i) {
        byteData[i + rowId] = v;
      }
    }
  }

  @Override
  public boolean getBoolean(int rowId) {
    if (isConstant) {
      return byteData[0] == 1;
    } else {
      return byteData[rowId] == 1;
    }
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    if (isConstant) {
      byteData[0] = value;
    } else {
      byteData[rowId] = value;
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    if (isConstant) {
      byteData[0] = value;
    } else {
      for (int i = 0; i < count; ++i) {
        byteData[i + rowId] = value;
      }
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    if (isConstant) {
      System.arraycopy(src, srcIndex, byteData, 0, 1);
    } else {
      System.arraycopy(src, srcIndex, byteData, rowId, count);
    }
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return byteData[0];
      } else {
        return byteData[rowId];
      }
    } else {
      if (isConstant) {
        return (byte) dictionary.decodeToInt(dictionaryIds.getInt(0));
      } else {
        return (byte) dictionary.decodeToInt(dictionaryIds.getInt(rowId));
      }
    }
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    if (isConstant) {
      shortData[0] = value;
    } else {
      shortData[rowId] = value;
    }
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    if (isConstant) {
      shortData[0] = value;
    } else {
      for (int i = 0; i < count; ++i) {
        shortData[i + rowId] = value;
      }
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    if (isConstant) {
      System.arraycopy(src, srcIndex, shortData, 0, 1);
    } else {
      System.arraycopy(src, srcIndex, shortData, rowId, count);
    }
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return shortData[0];
      } else {
        return shortData[rowId];
      }
    } else {
      if (isConstant) {
        return (short) dictionary.decodeToInt(dictionaryIds.getInt(0));
      } else {
        return (short) dictionary.decodeToInt(dictionaryIds.getInt(rowId));
      }
    }
  }


  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    if (isConstant) {
      intData[0] = value;
    } else {
      intData[rowId] = value;
    }
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    if (isConstant) {
      intData[0] = value;
    } else {
      for (int i = 0; i < count; ++i) {
        intData[i + rowId] = value;
      }
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    if (isConstant) {
      System.arraycopy(src, srcIndex, intData, 0, 1);
    } else {
      System.arraycopy(src, srcIndex, intData, rowId, count);
    }
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    if (isConstant) {
      intData[0] = Platform.getInt(src, srcOffset);
      if (bigEndianPlatform) {
        intData[0] = java.lang.Integer.reverseBytes(intData[0]);
      }
    } else {
      for (int i = 0; i < count; ++i, srcOffset += 4) {
        intData[i + rowId] = Platform.getInt(src, srcOffset);
        if (bigEndianPlatform) {
          intData[i + rowId] = java.lang.Integer.reverseBytes(intData[i + rowId]);
        }
      }
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return intData[0];
      } else {
        return intData[rowId];
      }
    } else {
      if (isConstant) {
        return dictionary.decodeToInt(dictionaryIds.getInt(0));
      } else {
        return dictionary.decodeToInt(dictionaryIds.getInt(rowId));
      }
    }
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    if (isConstant) {
      longData[0] = value;
    } else {
      longData[rowId] = value;
    }
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    if (isConstant) {
      longData[0] = value;
    } else {
      for (int i = 0; i < count; ++i) {
        longData[i + rowId] = value;
      }
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    if (isConstant) {
      System.arraycopy(src, srcIndex, longData, 0, 1);
    } else {
      System.arraycopy(src, srcIndex, longData, rowId, count);
    }
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    if (isConstant) {
      longData[0] = Platform.getLong(src, srcOffset);
      if (bigEndianPlatform) {
        longData[0] = java.lang.Long.reverseBytes(longData[0]);
      }
    } else {
      for (int i = 0; i < count; ++i, srcOffset += 8) {
        longData[i + rowId] = Platform.getLong(src, srcOffset);
        if (bigEndianPlatform) {
          longData[i + rowId] = java.lang.Long.reverseBytes(longData[i + rowId]);
        }
      }
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return longData[0];
      } else {
        return longData[rowId];
      }
    } else {
      if (isConstant) {
        return dictionary.decodeToLong(dictionaryIds.getInt(0));
      } else {
        return dictionary.decodeToLong(dictionaryIds.getInt(rowId));
      }
    }
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    if (isConstant) {
      floatData[0] = value;
    } else {
      floatData[rowId] = value;
    }
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    if (isConstant) {
      Arrays.fill(floatData, 0, 1, value);
    } else {
      Arrays.fill(floatData, rowId, rowId + count, value);
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    if (isConstant) {
      System.arraycopy(src, srcIndex, floatData, 0, 1);
    } else {
      System.arraycopy(src, srcIndex, floatData, rowId, count);
    }
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      if (isConstant) {
        Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, floatData,
          Platform.DOUBLE_ARRAY_OFFSET, 4);
      } else {
        Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, floatData,
          Platform.DOUBLE_ARRAY_OFFSET + rowId * 4, count * 4);
      }
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      if (isConstant) {
        floatData[0] = bb.getFloat(srcIndex);
      } else {
        for (int i = 0; i < count; ++i) {
          floatData[i + rowId] = bb.getFloat(srcIndex + (4 * i));
        }
      }
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return floatData[0];
      } else {
        return floatData[rowId];
      }
    } else {
      if (isConstant) {
        return dictionary.decodeToFloat(dictionaryIds.getInt(0));
      } else {
        return dictionary.decodeToFloat(dictionaryIds.getInt(rowId));
      }
    }
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    if (isConstant) {
      doubleData[0] = value;
    } else {
      doubleData[rowId] = value;
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    if (isConstant) {
      Arrays.fill(doubleData, 0, 1, value);
    } else {
      Arrays.fill(doubleData, rowId, rowId + count, value);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    if (isConstant) {
      System.arraycopy(src, srcIndex, doubleData, 0, 1);
    } else {
      System.arraycopy(src, srcIndex, doubleData, rowId, count);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      if (isConstant) {
        Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, doubleData,
          Platform.DOUBLE_ARRAY_OFFSET, 8);
      } else {
        Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, doubleData,
          Platform.DOUBLE_ARRAY_OFFSET + rowId * 8, count * 8);
      }
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      if (isConstant) {
        doubleData[0] = bb.getDouble(srcIndex);
      } else { 
        for (int i = 0; i < count; ++i) {
          doubleData[i + rowId] = bb.getDouble(srcIndex + (8 * i));
        }
      }
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return doubleData[0];
      } else {
        return doubleData[rowId];
      }
    } else {
      if (isConstant) {
        return dictionary.decodeToDouble(dictionaryIds.getInt(0));
      } else {
        return dictionary.decodeToDouble(dictionaryIds.getInt(rowId));
      }
    }
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    if (isConstant) {
      return arrayLengths[0];
    } else {
      return arrayLengths[rowId];
    }
  }
  @Override
  public int getArrayOffset(int rowId) {
    if (isConstant) {
      return arrayOffsets[0];
    } else {
      return arrayOffsets[rowId];
    }
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    if (isConstant) {
      arrayOffsets[0] = offset;
      arrayLengths[0] = length;
    } else {
      arrayOffsets[rowId] = offset;
      arrayLengths[rowId] = length;
    }
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
    if (isConstant) {
      arrayOffsets[0] = result;
      arrayLengths[0] = length;
    } else {
      arrayOffsets[rowId] = result;
      arrayLengths[rowId] = length;
    }
    return result;
  }

  @Override
  public void reserve(int requiredCapacity) {
    if (requiredCapacity > capacity) reserveInternal(requiredCapacity * 2);
  }
}
