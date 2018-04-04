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
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.OffHeapMemoryBlock;
import org.apache.spark.unsafe.types.UTF8String;

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
    nulls.putByte(rowId, (byte) 0);
  }

  @Override
  public void putNull(int rowId) {
    nulls.putByte(rowId, (byte) 1);
    ++numNulls;
  }

  @Override
  public void putNulls(int rowId, int count) {
    long offset = rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      nulls.putByte(offset, (byte) 1);
    }
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!hasNull()) return;
    long offset = rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      nulls.putByte(offset, (byte) 0);
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nulls.getByte(rowId) == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    data.putByte(rowId, (byte)((value) ? 1 : 0));
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    for (int i = 0; i < count; ++i) {
      data.putByte(rowId + i, v);
    }
  }

  @Override
  public boolean getBoolean(int rowId) { return data.getByte(rowId) == 1; }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    assert(dictionary == null);
    boolean[] array = new boolean[count];
    for (int i = 0; i < count; ++i) {
      array[i] = (data.getByte(rowId + i) == 1);
    }
    return array;
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    data.putByte( rowId, value);

  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    for (int i = 0; i < count; ++i) {
      data.putByte(rowId + i, value);
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    data.copyFrom(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, rowId, count);
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      return data.getByte(rowId);
    } else {
      return (byte) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    assert(dictionary == null);
    byte[] array = new byte[count];
    data.writeTo(rowId, array, Platform.BYTE_ARRAY_OFFSET, count);
    return array;
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    return new UTF8String(data.subBlock(rowId, count));
  }

  //
  // APIs dealing with shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    data.putShort(2 * rowId, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    long offset = 2 * rowId;
    for (int i = 0; i < count; ++i, offset += 2) {
      data.putShort(offset, value);
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    data.copyFrom(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2, rowId * 2, count * 2);
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    data.copyFrom(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,rowId * 2, count * 2);
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      return data.getShort(2 * rowId);
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    assert(dictionary == null);
    short[] array = new short[count];
    data.writeTo(rowId * 2, array, Platform.SHORT_ARRAY_OFFSET, count * 2);
    return array;
  }

  //
  // APIs dealing with ints
  //

  @Override
  public void putInt(int rowId, int value) {
    data.putInt(4 * rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    long offset = 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      data.putInt(offset, value);
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    data.copyFrom(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4, rowId * 4, count * 4);
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    data.copyFrom(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, rowId * 4, count * 4);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      data.copyFrom(src, srcIndex + Platform.BYTE_ARRAY_OFFSET, 4 * rowId, count * 4);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4, srcOffset += 4) {
        data.putInt(offset, java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset)));
      }
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      return data.getInt(4 * rowId);
    } else {
      return dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public int[] getInts(int rowId, int count) {
    assert(dictionary == null);
    int[] array = new int[count];
    data.writeTo(rowId * 4, array, Platform.INT_ARRAY_OFFSET, count * 4);
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
    return data.getInt(4 * rowId);
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    data.putLong(8 * rowId, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    long offset = 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      data.putLong(offset, value);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    data.copyFrom(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8, rowId * 8, count * 8);
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    data.copyFrom(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, rowId * 8, count * 8);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      data.copyFrom(src, srcIndex + Platform.BYTE_ARRAY_OFFSET, rowId * 8, count * 8);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = 8 * rowId;
      for (int i = 0; i < count; ++i, offset += 8, srcOffset += 8) {
        data.putLong(offset, java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset)));
      }
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      return data.getLong(8 * rowId);
    } else {
      return dictionary.decodeToLong(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    assert(dictionary == null);
    long[] array = new long[count];
    data.writeTo(rowId * 8, array, Platform.LONG_ARRAY_OFFSET, count * 8);
    return array;
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    data.putFloat(rowId * 4, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    long offset = 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      data.putFloat(offset, value);
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    data.copyFrom(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4, rowId * 4, count * 4);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      data.copyFrom(src, srcIndex + Platform.BYTE_ARRAY_OFFSET, rowId * 4, count * 4);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4) {
        data.putFloat(offset, bb.getFloat(srcIndex + (4 * i)));
      }
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      return data.getFloat(rowId * 4);
    } else {
      return dictionary.decodeToFloat(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    assert(dictionary == null);
    float[] array = new float[count];
    data.writeTo(rowId * 4, array, Platform.FLOAT_ARRAY_OFFSET, count * 4);
    return array;
  }


  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    data.putDouble(rowId * 8, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    long offset = 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      data.putDouble(offset, value);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    data.copyFrom(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8, rowId * 8, count * 8);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      data.copyFrom(src, srcIndex + Platform.BYTE_ARRAY_OFFSET, rowId * 8, count * 8);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = 8 * rowId;
      for (int i = 0; i < count; ++i, offset += 8) {
        data.putDouble(offset, bb.getDouble(srcIndex + (8 * i)));
      }
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      return data.getDouble(rowId * 8);
    } else {
      return dictionary.decodeToDouble(dictionaryIds.getDictId(rowId));
    }
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    assert(dictionary == null);
    double[] array = new double[count];
    data.writeTo(rowId * 8, array, Platform.DOUBLE_ARRAY_OFFSET, count * 8);
    return array;
  }

  //
  // APIs dealing with Arrays.
  //
  @Override
  public void putArray(int rowId, int offset, int length) {
    assert(offset >= 0 && offset + length <= childColumns[0].capacity);
    lengthData.putInt(4 * rowId, length);
    offsetData.putInt(4 * rowId, offset);
  }

  @Override
  public int getArrayLength(int rowId) {
    return lengthData.getInt(4 * rowId);
  }

  @Override
  public int getArrayOffset(int rowId) {
    return offsetData.getInt(4 * rowId);
  }

  // APIs dealing with ByteArrays
  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    lengthData.putInt(4 * rowId, length);
    offsetData.putInt(4 * rowId, result);
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
