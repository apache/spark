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
import java.util.Arrays;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow;
import org.apache.spark.sql.catalyst.expressions.MutableRow;
import org.apache.spark.sql.execution.columnar.BasicColumnAccessor;
import org.apache.spark.sql.execution.columnar.ByteBufferHelper;
import org.apache.spark.sql.execution.columnar.NativeColumnAccessor;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * A column backed by an in memory JVM array. This stores the NULLs as a byte per value
 * and a java array for the values.
 */
public final class ByteBufferColumnVector extends ColumnVector {
  // The data stored in these arrays need to maintain binary compatible. We can
  // directly pass this buffer to external components.

  // This is faster than a boolean array and we optimize this over memory footprint.
  private byte[] nulls;

  // Array for each type. Only 1 is populated for any type.
  private byte[] data;
  private long offset;

  protected ByteBufferColumnVector(int capacity, DataType type,
    boolean isConstant, ByteBuffer buffer, ByteBuffer nullsBuffer) {
    super(capacity, type, MemoryMode.ON_HEAP);
    if (this.resultArray != null || DecimalType.isByteArrayDecimalType(type)) {
      throw new NotImplementedException();
    } else if (type instanceof BooleanType || type instanceof ByteType ||
               type instanceof ShortType ||
               type instanceof IntegerType || type instanceof DateType ||
               DecimalType.is32BitDecimalType(type) ||
               type instanceof LongType || DecimalType.is64BitDecimalType(type) ||
               (type instanceof FloatType) || (type instanceof DoubleType)) {
      data = buffer.array();
      offset = Platform.BYTE_ARRAY_OFFSET + buffer.position();
    } else if (resultStruct != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    nulls = new byte[capacity];
    reset();

    int numNulls = ByteBufferHelper.getInt(nullsBuffer);
    for (int i = 0; i < numNulls; i++) {
      int cordinal = ByteBufferHelper.getInt(nullsBuffer);
      putNull(cordinal);
    }
    if (isConstant) {
      setIsConstant();
    }
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
  public final boolean isNullAt(int rowId) {
    return nulls[rowId] == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public final void putBoolean(int rowId, boolean value) {
    Platform.putByte(data, offset  + rowId, (byte)((value) ? 1 : 0));
  }

  @Override
  public final void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    for (int i = 0; i < count; ++i) {
      Platform.putByte(data, offset  + i + rowId, v);
    }
  }

  @Override
  public final boolean getBoolean(int rowId) {
    return Platform.getByte(data, offset + rowId) == 1;
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public final void putByte(int rowId, byte value) {
    Platform.putByte(data, offset  + rowId, value);
  }

  @Override
  public final void putBytes(int rowId, int count, byte value) {
    for (int i = 0; i < count; ++i) {
      Platform.putByte(data, offset + i + rowId, value);
    }
  }

  @Override
  public final void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, data, rowId, count);
  }

  @Override
  public final byte getByte(int rowId) {
    assert(dictionary == null);
    return Platform.getByte(data, offset + rowId);
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public final void putShort(int rowId, short value) {
    Platform.putShort(data, offset + rowId * 2, value);
  }

  @Override
  public final void putShorts(int rowId, int count, short value) {
    for (int i = 0; i < count; ++i) {
      Platform.putShort(data, offset + (i + rowId) * 2, value);
    }
  }

  @Override
  public final void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public final short getShort(int rowId) {
    assert(dictionary == null);
    return Platform.getShort(data, offset + rowId * 2);
  }


  //
  // APIs dealing with Ints
  //

  @Override
  public final void putInt(int rowId, int value) {
    Platform.putInt(data, offset + rowId * 4, value);
  }

  @Override
  public final void putInts(int rowId, int count, int value) {
    for (int i = 0; i < count; ++i) {
      Platform.putInt(data, offset + (i + rowId) * 4, value);
    }
  }

  @Override
  public final void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public final void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public final int getInt(int rowId) {
    assert(dictionary == null);
    return Platform.getInt(data, offset + rowId * 4);
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public final void putLong(int rowId, long value) {
    Platform.putLong(data, offset + rowId * 8, value);
  }

  @Override
  public final void putLongs(int rowId, int count, long value) {
    for (int i = 0; i < count; ++i) {
      Platform.putLong(data, offset + (i + rowId) * 8, value);
    }
  }

  @Override
  public final void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public final void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public final long getLong(int rowId) {
    assert(dictionary == null);
    return Platform.getLong(data, offset + rowId * 8);
  }

  //
  // APIs dealing with floats
  //

  @Override
  public final void putFloat(int rowId, float value) { Platform.putFloat(data, offset + rowId * 4, value); }

  @Override
  public final void putFloats(int rowId, int count, float value) {
    throw new NotImplementedException();
  }

  @Override
  public final void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public final void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
            data, Platform.DOUBLE_ARRAY_OFFSET + rowId * 4, count * 4);
  }

  @Override
  public final float getFloat(int rowId) {
    assert(dictionary == null);
    return Platform.getFloat(data, offset + rowId * 4);
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public final void putDouble(int rowId, double value) { Platform.putDouble(data, offset + rowId * 8, value); }

  @Override
  public final void putDoubles(int rowId, int count, double value) {
    throw new NotImplementedException();
  }

  @Override
  public final void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public final void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public final double getDouble(int rowId) {
    assert(dictionary == null);
    return Platform.getDouble(data, offset + rowId * 8);
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public final int getArrayLength(int rowId) { throw new NotImplementedException(); }
  @Override
  public final int getArrayOffset(int rowId) { throw new NotImplementedException(); }

  @Override
  public final void putArray(int rowId, int offset, int length) {
    throw new NotImplementedException();
  }

  @Override
  public final void loadBytes(ColumnVector.Array array) {
    throw new NotImplementedException();
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public final int putByteArray(int rowId, byte[] value, int offset, int length) {
    throw new NotImplementedException();
  }

  @Override
  public final void reserve(int requiredCapacity) {
    if (requiredCapacity > capacity) reserveInternal(requiredCapacity * 2);
  }

  // Spilt this function out since it is the slow path.
  private final void reserveInternal(int newCapacity) {
    throw new NotImplementedException();
  }
}
