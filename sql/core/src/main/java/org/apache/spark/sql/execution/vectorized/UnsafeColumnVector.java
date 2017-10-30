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
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * A column backed by UnsafeArrayData on byte[].
 */
public final class UnsafeColumnVector extends WritableColumnVector {
  // This is faster than a boolean array and we optimize this over memory footprint.
  private byte[] nulls;

  // Array stored in byte array
  private byte[] data;
  private long offset;

  // Only set if type is Array.
  private int lastArrayRow;
  private int lastArrayPos;
  private UnsafeArrayData unsafeArray = new UnsafeArrayData();

  public UnsafeColumnVector(int capacity, DataType type) {
    super(capacity, type);

    reserveInternal(capacity);
    reset();
    nulls = new byte[capacity];
  }

  @Override
  public void putUnsafeData(ByteBuffer buffer) {
    assert(this.resultArray != null);
    data = buffer.array();
    offset = Platform.BYTE_ARRAY_OFFSET + buffer.position();

    lastArrayRow = Integer.MAX_VALUE;
    lastArrayPos = 0;

    setIsConstant();
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
    if (nulls == null) return false;
    // If @@@ existins in @@@@, data is null.
    if (data != null) {
      return nulls[rowId] == 1;
    } else {
      return unsafeArray.isNullAt(rowId);
    }
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
    return unsafeArray.getBoolean(rowId);
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    assert(dictionary == null);
    boolean[] array = unsafeArray.toBooleanArray();
    if (array.length == count) {
      return array;
    } else {
      assert(count < array.length);
      boolean[] newArray = new boolean[count];
      System.arraycopy(array, 0, newArray, 0, count);
      return newArray;
    }
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
    return unsafeArray.getByte(rowId);
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    assert(dictionary == null);
    byte[] array = unsafeArray.toByteArray();
    if (array.length == count) {
      return array;
    } else {
      assert(count < array.length);
      byte[] newArray = new byte[count];
      System.arraycopy(array, 0, newArray, 0, count);
      return newArray;
    }
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
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public short getShort(int rowId) {
    assert(dictionary == null);
    return unsafeArray.getShort(rowId);
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    assert(dictionary == null);
    short[] array = unsafeArray.toShortArray();
    if (array.length == count) {
      return array;
    } else {
      assert(count < array.length);
      short[] newArray = new short[count];
      System.arraycopy(array, 0, newArray, 0, count);
      return newArray;
    }
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
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public int getInt(int rowId) {
    assert(dictionary == null);
    return unsafeArray.getInt(rowId);
  }

  @Override
  public int[] getInts(int rowId, int count) {
    assert(dictionary == null);
    int[] array = unsafeArray.toIntArray();
    if (array.length == count) {
      return array;
    } else {
      assert(count < array.length);
      int[] newArray = new int[count];
      System.arraycopy(array, 0, newArray, 0, count);
      return newArray;
    }
  }

  public int getDictId(int rowId) {
    throw new NotImplementedException();
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
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public long getLong(int rowId) {
    assert(dictionary == null);
    return unsafeArray.getLong(rowId);
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    assert(dictionary == null);
    long[] array = unsafeArray.toLongArray();
    if (array.length == count) {
      return array;
    } else {
      assert(count < array.length);
      long[] newArray = new long[count];
      System.arraycopy(array, 0, newArray, 0, count);
      return newArray;
    }
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
    return unsafeArray.getFloat(rowId);
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    assert(dictionary == null);
    float[] array = unsafeArray.toFloatArray();
    if (array.length == count) {
      return array;
    } else {
      assert(count < array.length);
      float[] newArray = new float[count];
      System.arraycopy(array, 0, newArray, 0, count);
      return newArray;
    }
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
    return unsafeArray.getDouble(rowId);
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    assert(dictionary == null);
    double[] array = unsafeArray.toDoubleArray();
    if (array.length == count) {
      return array;
    } else {
      assert(count < array.length);
      double[] newArray = new double[count];
      System.arraycopy(array, 0, newArray, 0, count);
      return newArray;
    }
  }

  //
  // APIs dealing with Arrays
  //

  private void updateLastArrayPos(int rowId) {
    int relative = rowId - lastArrayRow;
    if (relative == 1) {
      int totalBytesLastArray = Platform.getInt(data, offset + lastArrayPos);
      lastArrayPos += totalBytesLastArray + 4;  // 4 for totalbytes in UnsafeArrayData
    } else if (relative == 0) {
      // return the same position
      return;
    } else if (relative > 0) {
      for (int i = 0; i < relative; i++) {
        if (isNullAt(lastArrayRow + i)) continue;
        int totalBytesLastArray = Platform.getInt(data, offset + lastArrayPos);
        lastArrayPos += totalBytesLastArray + 4;  // 4 for totalbytes in UnsafeArrayData
      }
    } else {
      // recalculate pos from the first entry
      lastArrayPos = 0;
      for (int i = 0; i < rowId; i++) {
        if (isNullAt(i)) continue;
        int totalBytesLastArray = Platform.getInt(data, offset + lastArrayPos);
        lastArrayPos += totalBytesLastArray + 4;  // 4 for totalbytes in UnsafeArrayData
      }
    }
    lastArrayRow = rowId;
  }

  private int setUnsafeArray(int rowId) {
    assert(data != null);
    int length;
    if (rowId - lastArrayRow == 1 && !anyNullsSet()) {
      // inlined frequently-executed path (access an array in the next row)
      lastArrayRow = rowId;
      long localOffset = offset;
      int localLastArrayPos = lastArrayPos;
      int totalBytesLastArray = Platform.getInt(data, localOffset + localLastArrayPos);
      localLastArrayPos += totalBytesLastArray + 4;  // 4 for totalbytes in UnsafeArrayData
      length = Platform.getInt(data, localOffset + localLastArrayPos);
      ((UnsafeColumnVector)(this.resultArray.data))
        .unsafeArray.pointTo(data,  localOffset + localLastArrayPos + 4, length);
      lastArrayPos = localLastArrayPos;
    } else {
      updateLastArrayPos(rowId);
      length = Platform.getInt(data, offset + lastArrayPos);  // inline getArrayLength()
      ((UnsafeColumnVector)(this.resultArray.data))
        .unsafeArray.pointTo(data,  offset + lastArrayPos + 4, length);
    }
    return ((UnsafeColumnVector)(this.resultArray.data)).unsafeArray.numElements();
  }

  @Override
  public ColumnVector.Array getArray(int rowId) {
    int elements = setUnsafeArray(rowId);

    resultArray.length = elements;
    resultArray.offset = 0;
    return resultArray;
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

  // Spilt this function out since it is the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    assert(this.resultArray != null);
    byte[] newNulls = new byte[newCapacity];
    if (nulls != null) System.arraycopy(nulls, 0, newNulls, 0, capacity);
    nulls = newNulls;
    capacity = newCapacity;
  }

  @Override
  protected UnsafeColumnVector reserveNewColumn(int capacity, DataType type) {
    return new UnsafeColumnVector(capacity, type);
  }
}