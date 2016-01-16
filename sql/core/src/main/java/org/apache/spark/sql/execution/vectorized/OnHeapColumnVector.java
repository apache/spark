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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.unsafe.Platform;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.Arrays;

/**
 * A column backed by an in memory JVM array. This stores the NULLs as a byte per value
 * and a java array for the values.
 */
public final class OnHeapColumnVector extends ColumnVector {
  // The data stored in these arrays need to maintain binary compatible. We can
  // directly pass this buffer to external components.

  // This is faster than a boolean array and we optimize this over memory footprint.
  private byte[] nulls;

  // Array for each type. Only 1 is populated for any type.
  private int[] intData;
  private double[] doubleData;

  protected OnHeapColumnVector(int capacity, DataType type) {
    super(capacity, type);
    if (type instanceof IntegerType) {
      this.intData = new int[capacity];
    } else if (type instanceof DoubleType) {
      this.doubleData = new double[capacity];
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    this.nulls = new byte[capacity];
    reset();
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
    nulls = null;
    intData = null;
    doubleData = null;
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
  public final boolean getIsNull(int rowId) {
    return nulls[rowId] == 1;
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public final void putInt(int rowId, int value) {
    intData[rowId] = value;
  }

  @Override
  public final void putInts(int rowId, int count, int value) {
    for (int i = 0; i < count; ++i) {
      intData[i + rowId] = value;
    }
  }

  @Override
  public final void putInts(int rowId, int count, int[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, intData, rowId, count);
  }

  @Override
  public final void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i) {
      intData[i + rowId] = Platform.getInt(src, srcOffset);;
      srcIndex += 4;
      srcOffset += 4;
    }
  }

  @Override
  public final int getInt(int rowId) {
    return intData[rowId];
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public final void putDouble(int rowId, double value) {
    doubleData[rowId] = value;
  }

  @Override
  public final void putDoubles(int rowId, int count, double value) {
    Arrays.fill(doubleData, rowId, rowId + count, value);
  }

  @Override
  public final void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, doubleData, rowId, count);
  }

  @Override
  public final void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, doubleData,
        Platform.DOUBLE_ARRAY_OFFSET + rowId * 8, count * 8);
  }

  @Override
  public final double getDouble(int rowId) {
    return doubleData[rowId];
  }
}
