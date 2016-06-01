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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.*;

/**
 * A column backed by an in memory JVM array. This stores the NULLs as a byte per value
 * and a java array for the values.
 */
public abstract class OnHeapColumnVectorBase extends ColumnVector {

  protected static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  // The data stored in these arrays need to maintain binary compatible. We can
  // directly pass this buffer to external components.

  // This is faster than a boolean array and we optimize this over memory footprint.
  protected byte[] nulls;

  // Array for each type. Only 1 is populated for any type.
  protected byte[] byteData;
  protected short[] shortData;
  protected int[] intData;
  protected long[] longData;
  protected float[] floatData;
  protected double[] doubleData;

  // Only set if type is Array.
  protected int[] arrayLengths;
  protected int[] arrayOffsets;

  public OnHeapColumnVectorBase(
      int capacity,
      int childCapacity,
      DataType type,
      boolean isConstant) {
    super(capacity, childCapacity, type, MemoryMode.ON_HEAP, isConstant);
    reserveInternal(capacity);
    reset();
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

  // Spilt this function out since it is the slow path.
  protected void reserveInternal(int newCapacity) {
    if (this.resultArray != null || DecimalType.isByteArrayDecimalType(type)) {
      int[] newLengths = new int[newCapacity];
      int[] newOffsets = new int[newCapacity];
      if (this.arrayLengths != null) {
        System.arraycopy(this.arrayLengths, 0, newLengths, 0, elementsAppended);
        System.arraycopy(this.arrayOffsets, 0, newOffsets, 0, elementsAppended);
      }
      arrayLengths = newLengths;
      arrayOffsets = newOffsets;
    } else if (type instanceof BooleanType) {
      if (byteData == null || byteData.length < newCapacity) {
        byte[] newData = new byte[newCapacity];
        if (byteData != null) System.arraycopy(byteData, 0, newData, 0, elementsAppended);
        byteData = newData;
      }
    } else if (type instanceof ByteType) {
      if (byteData == null || byteData.length < newCapacity) {
        byte[] newData = new byte[newCapacity];
        if (byteData != null) System.arraycopy(byteData, 0, newData, 0, elementsAppended);
        byteData = newData;
      }
    } else if (type instanceof ShortType) {
      if (shortData == null || shortData.length < newCapacity) {
        short[] newData = new short[newCapacity];
        if (shortData != null) System.arraycopy(shortData, 0, newData, 0, elementsAppended);
        shortData = newData;
      }
    } else if (type instanceof IntegerType || type instanceof DateType ||
      DecimalType.is32BitDecimalType(type)) {
      if (intData == null || intData.length < newCapacity) {
        int[] newData = new int[newCapacity];
        if (intData != null) System.arraycopy(intData, 0, newData, 0, elementsAppended);
        intData = newData;
      }
    } else if (type instanceof LongType || type instanceof TimestampType ||
        DecimalType.is64BitDecimalType(type)) {
      if (longData == null || longData.length < newCapacity) {
        long[] newData = new long[newCapacity];
        if (longData != null) System.arraycopy(longData, 0, newData, 0, elementsAppended);
        longData = newData;
      }
    } else if (type instanceof FloatType) {
      if (floatData == null || floatData.length < newCapacity) {
        float[] newData = new float[newCapacity];
        if (floatData != null) System.arraycopy(floatData, 0, newData, 0, elementsAppended);
        floatData = newData;
      }
    } else if (type instanceof DoubleType) {
      if (doubleData == null || doubleData.length < newCapacity) {
        double[] newData = new double[newCapacity];
        if (doubleData != null) System.arraycopy(doubleData, 0, newData, 0, elementsAppended);
        doubleData = newData;
      }
    } else if (resultStruct != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }

    byte[] newNulls = new byte[newCapacity];
    if (nulls != null) System.arraycopy(nulls, 0, newNulls, 0, elementsAppended);
    nulls = newNulls;

    capacity = newCapacity;
  }
}
