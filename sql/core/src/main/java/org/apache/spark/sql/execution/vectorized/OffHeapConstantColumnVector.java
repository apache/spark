
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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * Constant column data backed using offheap memory.
 */
public final class OffHeapConstantColumnVector extends OffHeapColumnVectorBase {
  public OffHeapConstantColumnVector(int capacity, DataType type) {
    super(1, capacity, type, true);
  }

  @Override
  public void reserve(int requiredCapacity) {
    // no-op
  }

  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    Platform.putByte(null, nulls, (byte) 0);
  }

  @Override
  public void putNull(int rowId) {
    Platform.putByte(null, nulls, (byte) 1);
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public void putNulls(int rowId, int count) {
    long offset = nulls;
    Platform.putByte(null, offset, (byte) 1);
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!anyNullsSet) return;
    long offset = nulls;
    Platform.putByte(null, offset, (byte) 0);
  }

  @Override
  public boolean isNullAt(int rowId) {
    return Platform.getByte(null, nulls) == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    Platform.putByte(null, data, (byte)((value) ? 1 : 0));
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    if (isConstant) {
      rowId = 0;
      count = 1;
    }
    byte v = (byte)((value) ? 1 : 0);
    Platform.putByte(null, data, v);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return Platform.getByte(null, data) == 1;
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    Platform.putByte(null, data, value);

  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    Platform.putByte(null, data, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, data, 1);
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      return Platform.getByte(null, data);
    } else {
      return (byte) dictionary.decodeToInt(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    Platform.putShort(null, data, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    long offset = data;
    Platform.putShort(null, offset, value);
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2, null, data, 2);
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      return Platform.getShort(null, data);
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with ints
  //

  @Override
  public void putInt(int rowId, int value) {
    Platform.putInt(null, data, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    long offset = data;
    Platform.putInt(null, offset, value);
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4, null, data, 4);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
        null, data, 4);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = data;
      Platform.putInt(null, offset,
        java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset)));
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      return Platform.getInt(null, data);
    } else {
      return dictionary.decodeToInt(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(null, data, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    long offset = data;
    Platform.putLong(null, offset, value);
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8,
      null, data, 8);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          null, data, 8);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = data;
      Platform.putLong(null, offset,
        java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset)));
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      return Platform.getLong(null, data);
    } else {
      return dictionary.decodeToLong(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    Platform.putFloat(null, data, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    long offset = data;
    Platform.putFloat(null, offset, value);
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4, null, data, 4);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
          null, data, 4);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = data;
      Platform.putFloat(null, offset, bb.getFloat(srcIndex));
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      return Platform.getFloat(null, data);
    } else {
      return dictionary.decodeToFloat(dictionaryIds.getInt(0));
    }
  }


  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    Platform.putDouble(null, data, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    long offset = data;
    Platform.putDouble(null, offset, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8, null, data, 8);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
        null, data, 8);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = data;
      Platform.putDouble(null, offset, bb.getDouble(srcIndex));
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      return Platform.getDouble(null, data);
    } else {
      return dictionary.decodeToDouble(dictionaryIds.getInt(0));
    }
  }

  //
  // APIs dealing with Arrays.
  //
  @Override
  public void putArray(int rowId, int offset, int length) {
    assert(offset >= 0 && offset + length <= childColumns[0].capacity);
    Platform.putInt(null, lengthData, length);
    Platform.putInt(null, offsetData, offset);
  }

  @Override
  public int getArrayLength(int rowId) {
    return Platform.getInt(null, lengthData);
  }

  @Override
  public int getArrayOffset(int rowId) {
    return Platform.getInt(null, offsetData);
  }

  // APIs dealing with ByteArrays
  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    Platform.putInt(null, lengthData, length);
    Platform.putInt(null, offsetData, result);
    return result;
  }
}
