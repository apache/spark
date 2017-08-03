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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.*;

/**
 * An abstract class for read-only column vector.
 */
public abstract class ReadOnlyColumnVector extends ColumnVector {

  protected ReadOnlyColumnVector(int capacity, DataType type, MemoryMode memMode) {
    super(capacity, DataTypes.NullType, memMode);
    this.type = type;
    isConstant = true;
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public final void putNotNull(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putNull(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putNulls(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putNotNulls(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public final void putBoolean(int rowId, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putBooleans(int rowId, int count, boolean value) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public final void putByte(int rowId, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putBytes(int rowId, int count, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public final void putShort(int rowId, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putShorts(int rowId, int count, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public final void putInt(int rowId, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putInts(int rowId, int count, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public final void putLong(int rowId, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putLongs(int rowId, int count, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with floats
  //

  @Override
  public final void putFloat(int rowId, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putFloats(int rowId, int count, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public final void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putDoubles(int rowId, int count, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public final void putArray(int rowId, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public final int putByteArray(int rowId, byte[] value, int offset, int count) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Decimals
  //

  @Override
  public final void putDecimal(int rowId, Decimal value, int precision) {
    throw new UnsupportedOperationException();
  }

  //
  // Other APIs
  //

  @Override
  public final void setDictionary(Dictionary dictionary) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final ColumnVector reserveDictionaryIds(int capacity) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected final void reserveInternal(int newCapacity) {
    throw new UnsupportedOperationException();
  }
}
