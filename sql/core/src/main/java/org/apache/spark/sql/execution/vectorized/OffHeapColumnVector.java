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
 * Column data backed using offheap memory.
 */
public final class OffHeapColumnVector extends ColumnVector {
  protected OffHeapColumnVector(int capacity, int childCapacity, DataType type, boolean isConstant) {
    super(capacity, childCapacity, type, MemoryMode.OFF_HEAP, isConstant);

    this.isConstant = isConstant;

    nulls = 0;
    data = 0;
    lengthData = 0;
    offsetData = 0;

    reserveInternal(capacity);
    reset();
  }

  private static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  // The data stored in these two allocations need to maintain binary compatible. We can
  // directly pass this buffer to external components.
  protected long nulls;
  protected long data;

  // Set iff the type is array.
  protected long lengthData;
  protected long offsetData;

  @Override
  public long valuesNativeAddress() {
    return data;
  }

  @Override
  public long nullsNativeAddress() {
    return nulls;
  }

  @Override
  public void close() {
    Platform.freeMemory(nulls);
    Platform.freeMemory(data);
    Platform.freeMemory(lengthData);
    Platform.freeMemory(offsetData);
    nulls = 0;
    data = 0;
    lengthData = 0;
    offsetData = 0;
  }

  @Override
  public void loadBytes(ColumnVector.Array array) {
    if (array.tmpByteArray.length < array.length) array.tmpByteArray = new byte[array.length];
    Platform.copyMemory(
        null, data + array.offset, array.tmpByteArray, Platform.BYTE_ARRAY_OFFSET, array.length);
    array.byteArray = array.tmpByteArray;
    array.byteArrayOffset = 0;
  }

  // Split out the slow path.
  protected void reserveInternal(int newCapacity) {
    if (this.resultArray != null) {
      this.lengthData =
          Platform.reallocateMemory(lengthData, elementsAppended * 4, newCapacity * 4);
      this.offsetData =
          Platform.reallocateMemory(offsetData, elementsAppended * 4, newCapacity * 4);
    } else if (type instanceof ByteType || type instanceof BooleanType) {
      this.data = Platform.reallocateMemory(data, elementsAppended, newCapacity);
    } else if (type instanceof ShortType) {
      this.data = Platform.reallocateMemory(data, elementsAppended * 2, newCapacity * 2);
    } else if (type instanceof IntegerType || type instanceof FloatType ||
        type instanceof DateType || DecimalType.is32BitDecimalType(type)) {
      this.data = Platform.reallocateMemory(data, elementsAppended * 4, newCapacity * 4);
    } else if (type instanceof LongType || type instanceof DoubleType ||
        DecimalType.is64BitDecimalType(type) || type instanceof TimestampType) {
      this.data = Platform.reallocateMemory(data, elementsAppended * 8, newCapacity * 8);
    } else if (resultStruct != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    this.nulls = Platform.reallocateMemory(nulls, elementsAppended, newCapacity);
    Platform.setMemory(nulls + elementsAppended, (byte)0, newCapacity - elementsAppended);
    capacity = newCapacity;
  }

  @Override
  public void reserve(int requiredCapacity) {
    if (requiredCapacity > capacity) reserveInternal(requiredCapacity * 2);
  }

  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    if (isConstant) {
      Platform.putByte(null, nulls, (byte) 0);
    } else {
      Platform.putByte(null, nulls + rowId, (byte) 0);
    }
  }

  @Override
  public void putNull(int rowId) {
    if (isConstant) {
      Platform.putByte(null, nulls, (byte) 1);
    } else {
      Platform.putByte(null, nulls + rowId, (byte) 1);
    }
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public void putNulls(int rowId, int count) {
    if (isConstant) {
      Platform.putByte(null, nulls, (byte) 1);
    } else {
      long offset = nulls + rowId;
      for (int i = 0; i < count; ++i, ++offset) {
        Platform.putByte(null, offset, (byte) 1);
      }
    }
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!anyNullsSet) return;
    if (isConstant) {
      Platform.putByte(null, nulls, (byte) 0);
    } else {
      long offset = nulls + rowId;
      for (int i = 0; i < count; ++i, ++offset) {
        Platform.putByte(null, offset, (byte) 0);
      }
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (isConstant) {
      return Platform.getByte(null, nulls) == 1;
    } else {
      return Platform.getByte(null, nulls + rowId) == 1;
    }
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    if (isConstant) {
      Platform.putByte(null, data, (byte)((value) ? 1 : 0));
    } else {
      Platform.putByte(null, data + rowId, (byte)((value) ? 1 : 0));
    }
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    if (isConstant) {
      Platform.putByte(null, data, v);
    } else {
      for (int i = 0; i < count; ++i) {
        Platform.putByte(null, data + rowId + i, v);
      }
    }
  }

  @Override
  public boolean getBoolean(int rowId) {
    if (isConstant) {
      return Platform.getByte(null, data) == 1;
    } else {
      return Platform.getByte(null, data + rowId) == 1;
    }
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    if (isConstant) {
      Platform.putByte(null, data, value);
    } else {
      Platform.putByte(null, data + rowId, value);
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    if (isConstant) {
      Platform.putByte(null, data, value);
    } else {
      for (int i = 0; i < count; ++i) {
        Platform.putByte(null, data + rowId + i, value);
      }
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    if (isConstant) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, data, 1);
    } else {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, data + rowId, count);
    }
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return Platform.getByte(null, data);
      } else {
        return Platform.getByte(null, data + rowId);
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
  // APIs dealing with shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    if (isConstant) {
      Platform.putShort(null, data, value);
    } else {
      Platform.putShort(null, data + 2 * rowId, value);
    }
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    if (isConstant) {
      Platform.putShort(null, data, value);
    } else {
      long offset = data + 2 * rowId;
      for (int i = 0; i < count; ++i, offset += 4) {
        Platform.putShort(null, offset, value);
      }
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    if (isConstant) {
      Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2,
        null, data, 2);
    } else {
      Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2,
        null, data + 2 * rowId, count * 2);
    }
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return Platform.getShort(null, data);
      } else {
        return Platform.getShort(null, data + 2 * rowId);
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
  // APIs dealing with ints
  //

  @Override
  public void putInt(int rowId, int value) {
    if (isConstant) {
      Platform.putInt(null, data, value);
    } else {
      Platform.putInt(null, data + 4 * rowId, value);
    }
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    if (isConstant) {
      Platform.putInt(null, data, value);
    } else {
      long offset = data + 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4) {
        Platform.putInt(null, offset, value);
      }
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    if (isConstant) {
      Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4,
        null, data, 4);
    } else {
      Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4,
        null, data + 4 * rowId, count * 4);
    }
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      if (isConstant) {
        Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          null, data, 4);
      } else {
        Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          null, data + 4 * rowId, count * 4);
      }
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      if (isConstant) {
        Platform.putInt(null, data,
          java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset)));
      } else {
        long offset = data + 4 * rowId;
        for (int i = 0; i < count; ++i, offset += 4, srcOffset += 4) {
          Platform.putInt(null, offset,
            java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset)));
        }
      }
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return Platform.getInt(null, data);
      } else {
        return Platform.getInt(null, data + 4 * rowId);
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
      Platform.putLong(null, data, value);
    } else {
      Platform.putLong(null, data + 8 * rowId, value);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    if (isConstant) {
      Platform.putLong(null, data, value);
    } else {
      long offset = data + 8 * rowId;
      for (int i = 0; i < count; ++i, offset += 8) {
        Platform.putLong(null, offset, value);
      }
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    if (isConstant) {
      Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8,
        null, data, 8);
    } else {
      Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8,
        null, data + 8 * rowId, count * 8);
    }
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      if (isConstant) {
        Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          null, data, 8);
      } else {
        Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          null, data + 8 * rowId, count * 8);
      }
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      if (isConstant) {
        Platform.putLong(null, data,
          java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset)));
      } else {
        long offset = data + 8 * rowId;
        for (int i = 0; i < count; ++i, offset += 8, srcOffset += 8) {
          Platform.putLong(null, offset,
            java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset)));
        }
      }
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return Platform.getLong(null, data);
      } else {
        return Platform.getLong(null, data + 8 * rowId);
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
      Platform.putFloat(null, data, value);
    } else {
      Platform.putFloat(null, data + rowId * 4, value);
    }
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    if (isConstant) {
      Platform.putFloat(null, data, value);
    } else {
      long offset = data + 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4) {
        Platform.putFloat(null, offset, value);
      }
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    if (isConstant) {
      Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4,
        null, data, 4);
    } else {
      Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4,
        null, data + 4 * rowId, count * 4);
    }
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      if (isConstant) {
        Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
          null, data, 4);
      } else {
        Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
          null, data + rowId * 4, count * 4);
      }
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      if (isConstant) {
        Platform.putFloat(null, data, bb.getFloat(srcIndex));
      } else {
        long offset = data + 4 * rowId;
        for (int i = 0; i < count; ++i, offset += 4) {
          Platform.putFloat(null, offset, bb.getFloat(srcIndex + (4 * i)));
        }
      }
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return Platform.getFloat(null, data);
      } else {
        return Platform.getFloat(null, data + rowId * 4);
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
      Platform.putDouble(null, data, value);
    } else {
      Platform.putDouble(null, data + rowId * 8, value);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    if (isConstant) {
      Platform.putDouble(null, data, value);
    } else {
      long offset = data + 8 * rowId;
      for (int i = 0; i < count; ++i, offset += 8) {
        Platform.putDouble(null, offset, value);
      }
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    if (isConstant) {
      Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8,
        null, data, 8);
    } else {
      Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8,
        null, data + 8 * rowId, count * 8);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      if (isConstant) {
        Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
          null, data, 8);
      } else {
        Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
          null, data + rowId * 8, count * 8);
      }
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      if (isConstant) {
        Platform.putDouble(null, data, bb.getDouble(srcIndex));
      } else {
        long offset = data + 8 * rowId;
        for (int i = 0; i < count; ++i, offset += 8) {
          Platform.putDouble(null, offset, bb.getDouble(srcIndex + (8 * i)));
        }
      }
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      if (isConstant) {
        return Platform.getDouble(null, data);
      } else {
        return Platform.getDouble(null, data + rowId * 8);
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
  // APIs dealing with Arrays.
  //
  @Override
  public void putArray(int rowId, int offset, int length) {
    assert(offset >= 0 && offset + length <= childColumns[0].capacity);
    if (isConstant) {
      Platform.putInt(null, lengthData, length);
      Platform.putInt(null, offsetData, offset);
    } else {
      Platform.putInt(null, lengthData + 4 * rowId, length);
      Platform.putInt(null, offsetData + 4 * rowId, offset);
    }
  }

  @Override
  public int getArrayLength(int rowId) {
    if (isConstant) {
      return Platform.getInt(null, lengthData);
    } else {
      return Platform.getInt(null, lengthData + 4 * rowId);
    }
  }

  @Override
  public int getArrayOffset(int rowId) {
    if (isConstant) {
      return Platform.getInt(null, offsetData);
    } else {
      return Platform.getInt(null, offsetData + 4 * rowId);
    }
  }

  // APIs dealing with ByteArrays
  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    if (isConstant) {
      Platform.putInt(null, lengthData, length);
      Platform.putInt(null, offsetData, result);
    } else {
      Platform.putInt(null, lengthData + 4 * rowId, length);
      Platform.putInt(null, offsetData + 4 * rowId, result);
    }
    return result;
  }
}
