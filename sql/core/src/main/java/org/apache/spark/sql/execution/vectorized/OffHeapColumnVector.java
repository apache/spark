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

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.OffHeapMemoryBlock;

/**
 * Column data backed using offheap memory.
 */
public final class OffHeapColumnVector extends WritableColumnVector {

  private static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  /**
   * Allocates columns to store elements of each field of the schema off heap.
   * Capacity is the initial capacity of the vector and it will grow as necessary. Capacity is
   * in number of elements, not number of bytes.
   */
  public static OffHeapColumnVector[] allocateColumns(int capacity, StructType schema) {
    return allocateColumns(capacity, schema.fields());
  }

  /**
   * Allocates columns to store elements of each field off heap.
   * Capacity is the initial capacity of the vector and it will grow as necessary. Capacity is
   * in number of elements, not number of bytes.
   */
  public static OffHeapColumnVector[] allocateColumns(int capacity, StructField[] fields) {
    OffHeapColumnVector[] vectors = new OffHeapColumnVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      vectors[i] = new OffHeapColumnVector(capacity, fields[i].dataType());
    }
    return vectors;
  }

  // The data stored in these two allocations need to maintain binary compatible. We can
  // directly pass this buffer to external components.
  private OffHeapMemoryBlock nulls;
  private OffHeapMemoryBlock data;

  // Only set if type is Array or Map.
  private OffHeapMemoryBlock lengthData;
  private OffHeapMemoryBlock offsetData;

  public OffHeapColumnVector(int capacity, DataType type) {
    super(capacity, type);

    nulls = OffHeapMemoryBlock.NULL;
    data = OffHeapMemoryBlock.NULL;
    lengthData = OffHeapMemoryBlock.NULL;
    offsetData = OffHeapMemoryBlock.NULL;

    reserveInternal(capacity);
    reset();
  }

  /**
   * Returns the off heap pointer for the values buffer.
   */
  @VisibleForTesting
  public long valuesNativeAddress() {
    return data.getBaseOffset();
  }

  @Override
  public void close() {
    super.close();
    MemoryAllocator.UNSAFE.free(nulls);
    MemoryAllocator.UNSAFE.free(data);
    MemoryAllocator.UNSAFE.free(lengthData);
    MemoryAllocator.UNSAFE.free(offsetData);
    nulls = OffHeapMemoryBlock.NULL;
    data = OffHeapMemoryBlock.NULL;
    lengthData = OffHeapMemoryBlock.NULL;
    offsetData = OffHeapMemoryBlock.NULL;
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    Platform.putByte(nulls, nulls.getBaseOffset() + rowId, (byte) 0);
  }

  @Override
  public void putNull(int rowId) {
    Platform.putByte(nulls, nulls.getBaseOffset() + rowId, (byte) 1);
    ++numNulls;
  }

  @Override
  public void putNulls(int rowId, int count) {
    long offset = nulls.getBaseOffset() + rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      Platform.putByte(nulls, offset, (byte) 1);
    }
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!hasNull()) return;
    long offset = nulls.getBaseOffset() + rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      Platform.putByte(nulls, offset, (byte) 0);
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    return Platform.getByte(nulls, nulls.getBaseOffset() + rowId) == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    Platform.putByte(data, data.getBaseOffset() + rowId, (byte)((value) ? 1 : 0));
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    for (int i = 0; i < count; ++i) {
      Platform.putByte(data, data.getBaseOffset() + rowId + i, v);
    }
  }

  @Override
  public boolean getBoolean(int rowId) { return Platform.getByte(data, data.getBaseOffset() + rowId) == 1; }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    assert(dictionary == null);
    boolean[] array = new boolean[count];
    for (int i = 0; i < count; ++i) {
      array[i] = (Platform.getByte(data, data.getBaseOffset() + rowId + i) == 1);
    }
    return array;
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    Platform.putByte(data, data.getBaseOffset() + rowId, value);

  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    for (int i = 0; i < count; ++i) {
      Platform.putByte(data, data.getBaseOffset() + rowId + i, value);
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, data, data.getBaseOffset() + rowId, count);
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      return Platform.getByte(data, data.getBaseOffset() + rowId);
    } else {
      return (byte) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    assert(dictionary == null);
    byte[] array = new byte[count];
    Platform.copyMemory(data, data.getBaseOffset() + rowId, array, Platform.BYTE_ARRAY_OFFSET, count);
    return array;
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    return UTF8String.fromAddress(null, data.getBaseOffset() + rowId, count);
  }

  //
  // APIs dealing with shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    Platform.putShort(data, data.getBaseOffset() + 2 * rowId, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    long offset = data.getBaseOffset() + 2 * rowId;
    for (int i = 0; i < count; ++i, offset += 2) {
      Platform.putShort(data, offset, value);
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2,
        data, data.getBaseOffset() + 2 * rowId, count * 2);
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
      null, data.getBaseOffset() + rowId * 2, count * 2);
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      return Platform.getShort(data, data.getBaseOffset() + 2 * rowId);
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    assert(dictionary == null);
    short[] array = new short[count];
    Platform.copyMemory(data, data.getBaseOffset() + rowId * 2, array, Platform.SHORT_ARRAY_OFFSET, count * 2);
    return array;
  }

  //
  // APIs dealing with ints
  //

  @Override
  public void putInt(int rowId, int value) {
    Platform.putInt(data, data.getBaseOffset() + 4 * rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    long offset = data.getBaseOffset() + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putInt(data, offset, value);
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4,
        data, data.getBaseOffset() + 4 * rowId, count * 4);
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
      null, data.getBaseOffset() + rowId * 4, count * 4);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          data, data.getBaseOffset() + 4 * rowId, count * 4);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = data.getBaseOffset() + 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4, srcOffset += 4) {
        Platform.putInt(data, offset,
            java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset)));
      }
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      return Platform.getInt(data, data.getBaseOffset() + 4 * rowId);
    } else {
      return dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public int[] getInts(int rowId, int count) {
    assert(dictionary == null);
    int[] array = new int[count];
    Platform.copyMemory(data, data.getBaseOffset() + rowId * 4, array, Platform.INT_ARRAY_OFFSET, count * 4);
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
    return Platform.getInt(data, data.getBaseOffset() + 4 * rowId);
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(data, data.getBaseOffset() + 8 * rowId, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    long offset = data.getBaseOffset() + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putLong(data, offset, value);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8,
        data, data.getBaseOffset() + 8 * rowId, count * 8);
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
      null, data.getBaseOffset() + rowId * 8, count * 8);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          data, data.getBaseOffset() + 8 * rowId, count * 8);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = data.getBaseOffset() + 8 * rowId;
      for (int i = 0; i < count; ++i, offset += 8, srcOffset += 8) {
        Platform.putLong(data, offset,
            java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset)));
      }
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      return Platform.getLong(data, data.getBaseOffset() + 8 * rowId);
    } else {
      return dictionary.decodeToLong(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    assert(dictionary == null);
    long[] array = new long[count];
    Platform.copyMemory(data, data.getBaseOffset() + rowId * 8, array, Platform.LONG_ARRAY_OFFSET, count * 8);
    return array;
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    Platform.putFloat(data, data.getBaseOffset() + rowId * 4, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    long offset = data.getBaseOffset() + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putFloat(data, offset, value);
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4,
        data, data.getBaseOffset() + 4 * rowId, count * 4);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
          data, data.getBaseOffset() + rowId * 4, count * 4);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = data.getBaseOffset() + 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4) {
        Platform.putFloat(data, offset, bb.getFloat(srcIndex + (4 * i)));
      }
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      return Platform.getFloat(data, data.getBaseOffset() + rowId * 4);
    } else {
      return dictionary.decodeToFloat(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    assert(dictionary == null);
    float[] array = new float[count];
    Platform.copyMemory(data, data.getBaseOffset() + rowId * 4, array, Platform.FLOAT_ARRAY_OFFSET, count * 4);
    return array;
  }


  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    Platform.putDouble(data, data.getBaseOffset() + rowId * 8, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    long offset = data.getBaseOffset() + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putDouble(data, offset, value);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8,
      data, data.getBaseOffset() + 8 * rowId, count * 8);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
        data, data.getBaseOffset() + rowId * 8, count * 8);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = data.getBaseOffset() + 8 * rowId;
      for (int i = 0; i < count; ++i, offset += 8) {
        Platform.putDouble(data, offset, bb.getDouble(srcIndex + (8 * i)));
      }
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      return Platform.getDouble(data, data.getBaseOffset() + rowId * 8);
    } else {
      return dictionary.decodeToDouble(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    assert(dictionary == null);
    double[] array = new double[count];
    Platform.copyMemory(data, data.getBaseOffset() + rowId * 8, array, Platform.DOUBLE_ARRAY_OFFSET, count * 8);
    return array;
  }

  //
  // APIs dealing with Arrays.
  //
  @Override
  public void putArray(int rowId, int offset, int length) {
    assert(offset >= 0 && offset + length <= childColumns[0].capacity);
    Platform.putInt(lengthData, lengthData.getBaseOffset() + 4 * rowId, length);
    Platform.putInt(offsetData, offsetData.getBaseOffset() + 4 * rowId, offset);
  }

  @Override
  public int getArrayLength(int rowId) {
    return Platform.getInt(lengthData, lengthData.getBaseOffset() + 4 * rowId);
  }

  @Override
  public int getArrayOffset(int rowId) {
    return Platform.getInt(offsetData, offsetData.getBaseOffset() + 4 * rowId);
  }

  // APIs dealing with ByteArrays
  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    Platform.putInt(lengthData, lengthData.getBaseOffset() + 4 * rowId, length);
    Platform.putInt(offsetData, offsetData.getBaseOffset() + 4 * rowId, result);
    return result;
  }

  // Split out the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    int oldCapacity = (nulls == OffHeapMemoryBlock.NULL) ? 0 : capacity;
    if (isArray() || type instanceof MapType) {
      this.lengthData =
        MemoryAllocator.UNSAFE.reallocate(lengthData, oldCapacity * 4, newCapacity * 4);
      this.offsetData =
        MemoryAllocator.UNSAFE.reallocate(offsetData, oldCapacity * 4, newCapacity * 4);
    } else if (type instanceof ByteType || type instanceof BooleanType) {
      this.data = MemoryAllocator.UNSAFE.reallocate(data, oldCapacity, newCapacity);
    } else if (type instanceof ShortType) {
      this.data = MemoryAllocator.UNSAFE.reallocate(data, oldCapacity * 2, newCapacity * 2);
    } else if (type instanceof IntegerType || type instanceof FloatType ||
        type instanceof DateType || DecimalType.is32BitDecimalType(type)) {
      this.data = MemoryAllocator.UNSAFE.reallocate(data, oldCapacity * 4, newCapacity * 4);
    } else if (type instanceof LongType || type instanceof DoubleType ||
        DecimalType.is64BitDecimalType(type) || type instanceof TimestampType) {
      this.data = MemoryAllocator.UNSAFE.reallocate(data, oldCapacity * 8, newCapacity * 8);
    } else if (childColumns != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    this.nulls = MemoryAllocator.UNSAFE.reallocate(nulls, oldCapacity, newCapacity);
    Platform.setMemory(nulls.getBaseOffset() + oldCapacity, (byte)0, newCapacity - oldCapacity);
    capacity = newCapacity;
  }

  @Override
  protected OffHeapColumnVector reserveNewColumn(int capacity, DataType type) {
    return new OffHeapColumnVector(capacity, type);
  }
}
