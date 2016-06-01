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
import org.apache.spark.unsafe.Platform;

/**
 * Column data backed using offheap memory.
 */
public abstract class OffHeapColumnVectorBase extends ColumnVector {

  protected static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  // The data stored in these two allocations need to maintain binary compatible. We can
  // directly pass this buffer to external components.
  protected long nulls;
  protected long data;

  // Set iff the type is array.
  protected long lengthData;
  protected long offsetData;

  public OffHeapColumnVectorBase(
      int capacity,
      int childCapacity,
      DataType type,
      boolean isConstant) {
    super(capacity, childCapacity, type, MemoryMode.OFF_HEAP, isConstant);

    nulls = 0;
    data = 0;
    lengthData = 0;
    offsetData = 0;

    reserveInternal(capacity);
    reset();
  }

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
}
