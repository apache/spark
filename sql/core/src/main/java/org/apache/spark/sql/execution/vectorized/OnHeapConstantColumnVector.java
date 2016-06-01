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
 * A constant column backed by an in memory JVM array.
 */
public final class OnHeapConstantColumnVector extends OnHeapColumnVectorBase {
  public OnHeapConstantColumnVector(int capacity, DataType type) {
    super(1, capacity, type, true);
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    nulls[0] = (byte)0;
  }

  @Override
  public void putNull(int rowId) {
    nulls[0] = (byte)1;
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public void putNulls(int rowId, int count) {
    nulls[0] = (byte)1;
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!anyNullsSet) return;
    nulls[0] = (byte)0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nulls[0] == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    byteData[0] = (byte)((value) ? 1 : 0);
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    byteData[0] = v;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return byteData[0] == 1;
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    byteData[0] = value;
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    byteData[0] = value;
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, byteData, 0, 1);
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      return byteData[0];
    } else {
      return (byte) dictionary.decodeToInt(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    shortData[0] = value;
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    shortData[0] = value;
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, shortData, 0, 1);
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      return shortData[0];
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getInt(0));
    }
  }


  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    intData[0] = value;
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    intData[0] = value;
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, intData, 0, 1);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    intData[0] = Platform.getInt(src, srcOffset);
    if (bigEndianPlatform) {
      intData[0] = java.lang.Integer.reverseBytes(intData[0]);
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      return intData[0];
    } else {
      return dictionary.decodeToInt(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    longData[0] = value;
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    longData[0] = value;
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, longData, 0, 1);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    longData[0] = Platform.getLong(src, srcOffset);
    if (bigEndianPlatform) {
      longData[0] = java.lang.Long.reverseBytes(longData[0]);
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      return longData[0];
    } else {
      return dictionary.decodeToLong(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    floatData[0] = value;
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    Arrays.fill(floatData, 0, 1, value);
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, floatData, 0, 1);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, floatData,
          Platform.DOUBLE_ARRAY_OFFSET, 4);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      floatData[0] = bb.getFloat(srcIndex);
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      return floatData[0];
    } else {
      return dictionary.decodeToFloat(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    doubleData[0] = value;
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    Arrays.fill(doubleData, 0, 1, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, doubleData, 0, 1);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, doubleData,
          Platform.DOUBLE_ARRAY_OFFSET, 8);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      doubleData[0] = bb.getDouble(srcIndex);
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      return doubleData[0];
    } else {
      return dictionary.decodeToDouble(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    return arrayLengths[0];
  }
  @Override
  public int getArrayOffset(int rowId) {
    return arrayOffsets[0];
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    arrayOffsets[0] = offset;
    arrayLengths[0] = length;
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
    arrayOffsets[0] = result;
    arrayLengths[0] = length;
    return result;
  }

  @Override
  public void reserve(int requiredCapacity) {
    // no-op
  }
}
 
