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

import org.apache.commons.lang.NotImplementedException;

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.execution.columnar.ByteBufferHelper;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * A column backed by an in memory JVM byte array.
 * This stores the NULLs as a byte per value and a java array for the values.
 * Currently, this column vector is read-only
 */
public final class ByteBufferColumnVector extends ColumnVector {
  // The data stored in these arrays need to maintain binary compatible. We can
  // directly pass this buffer to external components.

  // This is faster than a boolean array and we optimize this over memory footprint.
  private byte[] nulls;

  // Array stored in byte array
  private byte[] data;
  private long offset;

  // Only set if type is Array.
  private int lastArrayRow;
  private int lastArrayPos;
  private UnsafeArrayData unsafeArray;

  protected ByteBufferColumnVector(int capacity, DataType type,
    boolean isConstant, ByteBuffer buffer, ByteBuffer nullsBuffer) {
    super(capacity, type);
    boolean containsNull = true;
    if (this.resultArray != null) {
      containsNull = ((ArrayType)type).containsNull();
      data = buffer.array();
      offset = Platform.BYTE_ARRAY_OFFSET + buffer.position();

      unsafeArray = new UnsafeArrayData();
      lastArrayPos = 0;
      lastArrayRow = 0;
    } else if (DecimalType.isByteArrayDecimalType(type)) {
      throw new NotImplementedException();
    } else if ((type instanceof FloatType) || (type instanceof DoubleType)) {
      data = buffer.array();
      offset = Platform.BYTE_ARRAY_OFFSET + buffer.position();
    } else if (resultStruct != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    if (containsNull) {
      nulls = new byte[capacity];
    }
    reset();

    if (containsNull) {
      int numNulls = ByteBufferHelper.getInt(nullsBuffer);
      for (int i = 0; i < numNulls; i++) {
        int cordinal = ByteBufferHelper.getInt(nullsBuffer);
        putNull(cordinal);
      }
    }
    if (isConstant) {
      setIsConstant();
    }
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
  public  void putNull(int rowId) {
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
    if (nulls == null) return false;
    return nulls[rowId] == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    throw new NotImplementedException();
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    throw new NotImplementedException();
  }

  @Override
  public boolean getBoolean(int rowId) {
    assert(dictionary == null);
    return Platform.getBoolean(data, offset + rowId);
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    throw new NotImplementedException();
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    throw new NotImplementedException();
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public byte getByte(int rowId) {
    assert(dictionary == null);
    return Platform.getByte(data, offset + rowId);
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    throw new NotImplementedException();
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    throw new NotImplementedException();
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public short getShort(int rowId) {
    assert(dictionary == null);
    return Platform.getShort(data, offset + rowId * 2);
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    throw new NotImplementedException();
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    throw new NotImplementedException();
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public int getInt(int rowId) {
    assert(dictionary == null);
    return Platform.getInt(data, offset + rowId * 4);
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    throw new NotImplementedException();
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    throw new NotImplementedException();
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public long getLong(int rowId) {
    throw new NotImplementedException();
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    throw new NotImplementedException();
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    throw new NotImplementedException();
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public float getFloat(int rowId) {
    assert(dictionary == null);
    return Platform.getFloat(data, offset + rowId * 4);
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    throw new NotImplementedException();
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    throw new NotImplementedException();
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public double getDouble(int rowId) {
    assert(dictionary == null);
    return Platform.getDouble(data, offset + rowId * 8);
  }

  //
  // APIs dealing with Arrays
  //

  private void updateLastArrayPos(int rowId) {
    int relative = rowId - lastArrayRow;
    lastArrayRow = rowId;

    if (relative == 1) {
      int totalBytesLastArray = Platform.getInt(data, offset + lastArrayPos);
      lastArrayPos += totalBytesLastArray + 4;  // 4 for totalbytes in UnsafeArrayData
    } else if (relative == 0) {
      // return the same position
      return;
    } else if (relative > 0) {
      for (int i = 0; i < relative; i++) {
        int totalBytesLastArray = Platform.getInt(data, offset + lastArrayPos);
        lastArrayPos += totalBytesLastArray + 4;  // 4 for totalbytes in UnsafeArrayData
      }
    } else {
      // recalculate pos from the first Array entry
      lastArrayPos = 0;
      for (int i = 0; i < rowId; i++) {
        int totalBytesLastArray = Platform.getInt(data, offset + lastArrayPos);
        lastArrayPos += totalBytesLastArray + 4;  // 4 for totalbytes in UnsafeArrayData
      }
    }
  }

  @Override
  public ArrayData getArray(int rowId) {
    if (rowId - lastArrayRow == 1) {
      lastArrayRow = rowId;
      long localOffset = offset;
      int localLastArrayPos = lastArrayPos;
      int totalBytesLastArray = Platform.getInt(data, localOffset + localLastArrayPos);
      localLastArrayPos += totalBytesLastArray + 4;  // 4 for totalbytes in UnsafeArrayData
      int length = Platform.getInt(data, localOffset + localLastArrayPos);
      unsafeArray.pointTo(data, localOffset + localLastArrayPos + 4, length);
      lastArrayPos = localLastArrayPos;
      return unsafeArray;
    } else {
      updateLastArrayPos(rowId);
      int length = Platform.getInt(data, offset + lastArrayPos);  // inline getArrayLength()
      unsafeArray.pointTo(data, offset + lastArrayPos + 4, length);
      return unsafeArray;
    }
  }

  @Override
  public int getArrayLength(int rowId) {
    updateLastArrayPos(rowId);
    return Platform.getInt(data, offset + lastArrayPos);
  }

  @Override
  public int getArrayOffset(int rowId) {
    updateLastArrayPos(rowId);
    return lastArrayPos;
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    throw new NotImplementedException();
  }

  @Override
  public void loadBytes(ColumnVector.Array array) {
    throw new NotImplementedException();
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    throw new NotImplementedException();
  }

  @Override
  public void reserve(int requiredCapacity) {
    if (requiredCapacity > capacity) reserveInternal(requiredCapacity * 2);
  }

  // Spilt this function out since it is the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    throw new NotImplementedException();
  }
}
