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

import java.nio.ByteOrder;

import org.apache.commons.lang.NotImplementedException;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.OffHeapMemoryBlock;

/**
 * Column data backed using offheap memory.
 */
public final class OffHeapColumnVector extends ColumnVector {
  // The data stored in these two allocations need to maintain binary compatible. We can
  // directly pass this buffer to external components.
  private OffHeapMemoryBlock nulls;
  private OffHeapMemoryBlock data;

  // Set iff the type is array.
  private OffHeapMemoryBlock lengthData;
  private OffHeapMemoryBlock offsetData;

  protected OffHeapColumnVector(int capacity, DataType type) {
    super(capacity, type, MemoryMode.OFF_HEAP);
    if (!ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
      throw new NotImplementedException("Only little endian is supported.");
    }
    nulls = new OffHeapMemoryBlock(null, 0, 0);
    data = new OffHeapMemoryBlock(null, 0, 0);
    lengthData = new OffHeapMemoryBlock(null, 0, 0);
    offsetData = new OffHeapMemoryBlock(null, 0, 0);

    reserveInternal(capacity);
    reset();
  }

  @Override
  public final long valuesNativeAddress() {
    return data.getBaseOffset();
  }

  @Override
  public long nullsNativeAddress() {
    return nulls.getBaseOffset();
  }

  @Override
  public final void close() {
    nulls = new OffHeapMemoryBlock(null, 0, 0);
    data = new OffHeapMemoryBlock(null, 0, 0);
    lengthData = new OffHeapMemoryBlock(null, 0, 0);
    offsetData = new OffHeapMemoryBlock(null, 0, 0);
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public final void putNotNull(int rowId) {
    Platform.putByte(nulls, nulls.getBaseOffset() + rowId, (byte) 0);
  }

  @Override
  public final void putNull(int rowId) {
    Platform.putByte(nulls, nulls.getBaseOffset() + rowId, (byte) 1);
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public final void putNulls(int rowId, int count) {
    long offset = nulls.getBaseOffset() + rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      Platform.putByte(nulls, offset, (byte) 1);
    }
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public final void putNotNulls(int rowId, int count) {
    if (!anyNullsSet) return;
    long offset = nulls.getBaseOffset() + rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      Platform.putByte(nulls, offset, (byte) 0);
    }
  }

  @Override
  public final boolean getIsNull(int rowId) {
    return Platform.getByte(nulls, nulls.getBaseOffset() + rowId) == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public final void putBoolean(int rowId, boolean value) {
    Platform.putByte(data, data.getBaseOffset() + rowId, (byte)((value) ? 1 : 0));
  }

  @Override
  public final void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    for (int i = 0; i < count; ++i) {
      Platform.putByte(data, data.getBaseOffset() + rowId + i, v);
    }
  }

  @Override
  public final boolean getBoolean(int rowId) {
    return Platform.getByte(data, data.getBaseOffset() + rowId) == 1;
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public final void putByte(int rowId, byte value) {
    Platform.putByte(data, data.getBaseOffset() + rowId, value);

  }

  @Override
  public final void putBytes(int rowId, int count, byte value) {
    for (int i = 0; i < count; ++i) {
      Platform.putByte(data, data.getBaseOffset() + rowId + i, value);
    }
  }

  @Override
  public final void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, data, data.getBaseOffset() + rowId, count);
  }

  @Override
  public final byte getByte(int rowId) {
    if (dictionary == null) {
      return Platform.getByte(data, data.getBaseOffset() + rowId);
    } else {
      return (byte) dictionary.decodeToInt(dictionaryIds.getInt(rowId));
    }
  }

  //
  // APIs dealing with shorts
  //

  @Override
  public final void putShort(int rowId, short value) {
    Platform.putShort(data, data.getBaseOffset() + 2 * rowId, value);
  }

  @Override
  public final void putShorts(int rowId, int count, short value) {
    long offset = data.getBaseOffset() + 2 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putShort(data, offset, value);
    }
  }

  @Override
  public final void putShorts(int rowId, int count, short[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2,
        data, data.getBaseOffset() + 2 * rowId, count * 2);
  }

  @Override
  public final short getShort(int rowId) {
    if (dictionary == null) {
      return Platform.getShort(data, data.getBaseOffset() + 2 * rowId);
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getInt(rowId));
    }
  }

  //
  // APIs dealing with ints
  //

  @Override
  public final void putInt(int rowId, int value) {
    Platform.putInt(data, data.getBaseOffset() + 4 * rowId, value);
  }

  @Override
  public final void putInts(int rowId, int count, int value) {
    long offset = data.getBaseOffset() + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putInt(data, offset, value);
    }
  }

  @Override
  public final void putInts(int rowId, int count, int[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4,
        data, data.getBaseOffset() + 4 * rowId, count * 4);
  }

  @Override
  public final void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
        data, data.getBaseOffset() + 4 * rowId, count * 4);
  }

  @Override
  public final int getInt(int rowId) {
    if (dictionary == null) {
      return Platform.getInt(data, data.getBaseOffset() + 4 * rowId);
    } else {
      return dictionary.decodeToInt(dictionaryIds.getInt(rowId));
    }
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public final void putLong(int rowId, long value) {
    Platform.putLong(data, data.getBaseOffset() + 8 * rowId, value);
  }

  @Override
  public final void putLongs(int rowId, int count, long value) {
    long offset = data.getBaseOffset() + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putLong(data, offset, value);
    }
  }

  @Override
  public final void putLongs(int rowId, int count, long[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8,
        data, data.getBaseOffset() + 8 * rowId, count * 8);
  }

  @Override
  public final void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
        data, data.getBaseOffset() + 8 * rowId, count * 8);
  }

  @Override
  public final long getLong(int rowId) {
    if (dictionary == null) {
      return Platform.getLong(data, data.getBaseOffset() + 8 * rowId);
    } else {
      return dictionary.decodeToLong(dictionaryIds.getInt(rowId));
    }
  }

  //
  // APIs dealing with floats
  //

  @Override
  public final void putFloat(int rowId, float value) {
    Platform.putFloat(data, data.getBaseOffset() + rowId * 4, value);
  }

  @Override
  public final void putFloats(int rowId, int count, float value) {
    long offset = data.getBaseOffset() + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putFloat(data, offset, value);
    }
  }

  @Override
  public final void putFloats(int rowId, int count, float[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4,
        data, data.getBaseOffset() + 4 * rowId, count * 4);
  }

  @Override
  public final void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
        data, data.getBaseOffset() + rowId * 4, count * 4);
  }

  @Override
  public final float getFloat(int rowId) {
    if (dictionary == null) {
      return Platform.getFloat(data, data.getBaseOffset() + rowId * 4);
    } else {
      return dictionary.decodeToFloat(dictionaryIds.getInt(rowId));
    }
  }


  //
  // APIs dealing with doubles
  //

  @Override
  public final void putDouble(int rowId, double value) {
    Platform.putDouble(data, data.getBaseOffset() + rowId * 8, value);
  }

  @Override
  public final void putDoubles(int rowId, int count, double value) {
    long offset = data.getBaseOffset() + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putDouble(data, offset, value);
    }
  }

  @Override
  public final void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8,
        data, data.getBaseOffset() + 8 * rowId, count * 8);
  }

  @Override
  public final void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
        data, data.getBaseOffset() + rowId * 8, count * 8);
  }

  @Override
  public final double getDouble(int rowId) {
    if (dictionary == null) {
      return Platform.getDouble(data, data.getBaseOffset() + rowId * 8);
    } else {
      return dictionary.decodeToDouble(dictionaryIds.getInt(rowId));
    }
  }

  //
  // APIs dealing with Arrays.
  //
  @Override
  public final void putArray(int rowId, int offset, int length) {
    assert(offset >= 0 && offset + length <= childColumns[0].capacity);
    Platform.putInt(lengthData, lengthData.getBaseOffset() + 4 * rowId, length);
    Platform.putInt(offsetData, offsetData.getBaseOffset() + 4 * rowId, offset);
  }

  @Override
  public final int getArrayLength(int rowId) {
    return Platform.getInt(lengthData, lengthData.getBaseOffset() + 4 * rowId);
  }

  @Override
  public final int getArrayOffset(int rowId) {
    return Platform.getInt(offsetData, offsetData.getBaseOffset() + 4 * rowId);
  }

  // APIs dealing with ByteArrays
  @Override
  public final int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    Platform.putInt(lengthData, lengthData.getBaseOffset() + 4 * rowId, length);
    Platform.putInt(offsetData, offsetData.getBaseOffset() + 4 * rowId, result);
    return result;
  }

  @Override
  public final void loadBytes(ColumnVector.Array array) {
    if (array.tmpByteArray.length < array.length) array.tmpByteArray = new byte[array.length];
    Platform.copyMemory(
        data, data.getBaseOffset() + array.offset, array.tmpByteArray, Platform.BYTE_ARRAY_OFFSET, array.length);
    array.byteArray = array.tmpByteArray;
    array.byteArrayOffset = 0;
  }

  @Override
  public final void reserve(int requiredCapacity) {
    if (requiredCapacity > capacity) reserveInternal(requiredCapacity * 2);
  }

  // Split out the slow path.
  private final void reserveInternal(int newCapacity) {
    if (this.resultArray != null) {
      this.lengthData = MemoryAllocator.UNSAFE.reallocate(this.lengthData, elementsAppended * 4, newCapacity * 4);
      this.offsetData = MemoryAllocator.UNSAFE.reallocate(this.offsetData, elementsAppended * 4, newCapacity * 4);
    } else if (type instanceof ByteType || type instanceof BooleanType) {
      this.data = MemoryAllocator.UNSAFE.reallocate(this.data, elementsAppended, newCapacity);
    } else if (type instanceof ShortType) {
      this.data = MemoryAllocator.UNSAFE.reallocate(this.data, elementsAppended * 2, newCapacity * 2);
    } else if (type instanceof IntegerType || type instanceof FloatType ||
        type instanceof DateType || DecimalType.is32BitDecimalType(type)) {
      this.data = MemoryAllocator.UNSAFE.reallocate(this.data, elementsAppended * 4, newCapacity * 4);
    } else if (type instanceof LongType || type instanceof DoubleType ||
        DecimalType.is64BitDecimalType(type)) {
      this.data = MemoryAllocator.UNSAFE.reallocate(this.data, elementsAppended * 8, newCapacity * 8);
    } else if (resultStruct != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    this.nulls = MemoryAllocator.UNSAFE.reallocate(this.nulls, elementsAppended, newCapacity);
    Platform.setMemory(nulls.getBaseOffset() + elementsAppended, (byte)0, newCapacity - elementsAppended);
    capacity = newCapacity;
  }
}
