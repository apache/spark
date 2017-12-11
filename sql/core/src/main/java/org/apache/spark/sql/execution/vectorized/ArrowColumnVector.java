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

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector backed by Apache Arrow.
 */
public final class ArrowColumnVector extends ColumnVector {

  private final ArrowVectorAccessor accessor;
  private ArrowColumnVector[] childColumns;

  private void ensureAccessible(int index) {
    int valueCount = accessor.getValueCount();
    if (index < 0 || index >= valueCount) {
      throw new IndexOutOfBoundsException(
        String.format("index: %d, valueCount: %d", index, valueCount));
    }
  }

  private void ensureAccessible(int index, int count) {
    int valueCount = accessor.getValueCount();
    if (index < 0 || index + count > valueCount) {
      throw new IndexOutOfBoundsException(
        String.format("index range: [%d, %d), valueCount: %d", index, index + count, valueCount));
    }
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  @Override
  public boolean anyNullsSet() {
    return numNulls() > 0;
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
      }
    }
    accessor.close();
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public boolean isNullAt(int rowId) {
    ensureAccessible(rowId);
    return accessor.isNullAt(rowId);
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public boolean getBoolean(int rowId) {
    ensureAccessible(rowId);
    return accessor.getBoolean(rowId);
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    ensureAccessible(rowId, count);
    boolean[] array = new boolean[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getBoolean(rowId + i);
    }
    return array;
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public byte getByte(int rowId) {
    ensureAccessible(rowId);
    return accessor.getByte(rowId);
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    ensureAccessible(rowId, count);
    byte[] array = new byte[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getByte(rowId + i);
    }
    return array;
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public short getShort(int rowId) {
    ensureAccessible(rowId);
    return accessor.getShort(rowId);
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    ensureAccessible(rowId, count);
    short[] array = new short[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getShort(rowId + i);
    }
    return array;
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public int getInt(int rowId) {
    ensureAccessible(rowId);
    return accessor.getInt(rowId);
  }

  @Override
  public int[] getInts(int rowId, int count) {
    ensureAccessible(rowId, count);
    int[] array = new int[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getInt(rowId + i);
    }
    return array;
  }

  @Override
  public int getDictId(int rowId) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public long getLong(int rowId) {
    ensureAccessible(rowId);
    return accessor.getLong(rowId);
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    ensureAccessible(rowId, count);
    long[] array = new long[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getLong(rowId + i);
    }
    return array;
  }

  //
  // APIs dealing with floats
  //

  @Override
  public float getFloat(int rowId) {
    ensureAccessible(rowId);
    return accessor.getFloat(rowId);
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    ensureAccessible(rowId, count);
    float[] array = new float[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getFloat(rowId + i);
    }
    return array;
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public double getDouble(int rowId) {
    ensureAccessible(rowId);
    return accessor.getDouble(rowId);
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    ensureAccessible(rowId, count);
    double[] array = new double[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getDouble(rowId + i);
    }
    return array;
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    ensureAccessible(rowId);
    return accessor.getArrayLength(rowId);
  }

  @Override
  public int getArrayOffset(int rowId) {
    ensureAccessible(rowId);
    return accessor.getArrayOffset(rowId);
  }

  //
  // APIs dealing with Decimals
  //

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    ensureAccessible(rowId);
    return accessor.getDecimal(rowId, precision, scale);
  }

  //
  // APIs dealing with UTF8Strings
  //

  @Override
  public UTF8String getUTF8String(int rowId) {
    ensureAccessible(rowId);
    return accessor.getUTF8String(rowId);
  }

  //
  // APIs dealing with Binaries
  //

  @Override
  public byte[] getBinary(int rowId) {
    ensureAccessible(rowId);
    return accessor.getBinary(rowId);
  }

  /**
   * Returns the data for the underlying array.
   */
  @Override
  public ArrowColumnVector arrayData() { return childColumns[0]; }

  /**
   * Returns the ordinal's child data column.
   */
  @Override
  public ArrowColumnVector getChildColumn(int ordinal) { return childColumns[ordinal]; }

  public ArrowColumnVector(ValueVector vector) {
    super(ArrowUtils.fromArrowField(vector.getField()));

    if (vector instanceof NullableBitVector) {
      accessor = new BooleanAccessor((NullableBitVector) vector);
    } else if (vector instanceof NullableTinyIntVector) {
      accessor = new ByteAccessor((NullableTinyIntVector) vector);
    } else if (vector instanceof NullableSmallIntVector) {
      accessor = new ShortAccessor((NullableSmallIntVector) vector);
    } else if (vector instanceof NullableIntVector) {
      accessor = new IntAccessor((NullableIntVector) vector);
    } else if (vector instanceof NullableBigIntVector) {
      accessor = new LongAccessor((NullableBigIntVector) vector);
    } else if (vector instanceof NullableFloat4Vector) {
      accessor = new FloatAccessor((NullableFloat4Vector) vector);
    } else if (vector instanceof NullableFloat8Vector) {
      accessor = new DoubleAccessor((NullableFloat8Vector) vector);
    } else if (vector instanceof NullableDecimalVector) {
      accessor = new DecimalAccessor((NullableDecimalVector) vector);
    } else if (vector instanceof NullableVarCharVector) {
      accessor = new StringAccessor((NullableVarCharVector) vector);
    } else if (vector instanceof NullableVarBinaryVector) {
      accessor = new BinaryAccessor((NullableVarBinaryVector) vector);
    } else if (vector instanceof NullableDateDayVector) {
      accessor = new DateAccessor((NullableDateDayVector) vector);
    } else if (vector instanceof NullableTimeStampMicroTZVector) {
      accessor = new TimestampAccessor((NullableTimeStampMicroTZVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new ArrayAccessor(listVector);

      childColumns = new ArrowColumnVector[1];
      childColumns[0] = new ArrowColumnVector(listVector.getDataVector());
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      accessor = new StructAccessor(mapVector);

      childColumns = new ArrowColumnVector[mapVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new ArrowColumnVector(mapVector.getVectorById(i));
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private abstract static class ArrowVectorAccessor {

    private final ValueVector vector;
    private final ValueVector.Accessor nulls;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
      this.nulls = vector.getAccessor();
    }

    final boolean isNullAt(int rowId) {
      return nulls.isNull(rowId);
    }

    final int getValueCount() {
      return nulls.getValueCount();
    }

    final int getNullCount() {
      return nulls.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getArrayLength(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getArrayOffset(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowVectorAccessor {

    private final NullableBitVector.Accessor accessor;

    BooleanAccessor(NullableBitVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  private static class ByteAccessor extends ArrowVectorAccessor {

    private final NullableTinyIntVector.Accessor accessor;

    ByteAccessor(NullableTinyIntVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class ShortAccessor extends ArrowVectorAccessor {

    private final NullableSmallIntVector.Accessor accessor;

    ShortAccessor(NullableSmallIntVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class IntAccessor extends ArrowVectorAccessor {

    private final NullableIntVector.Accessor accessor;

    IntAccessor(NullableIntVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class LongAccessor extends ArrowVectorAccessor {

    private final NullableBigIntVector.Accessor accessor;

    LongAccessor(NullableBigIntVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class FloatAccessor extends ArrowVectorAccessor {

    private final NullableFloat4Vector.Accessor accessor;

    FloatAccessor(NullableFloat4Vector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DoubleAccessor extends ArrowVectorAccessor {

    private final NullableFloat8Vector.Accessor accessor;

    DoubleAccessor(NullableFloat8Vector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DecimalAccessor extends ArrowVectorAccessor {

    private final NullableDecimalVector.Accessor accessor;

    DecimalAccessor(NullableDecimalVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {

    private final NullableVarCharVector.Accessor accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(NullableVarCharVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
          stringResult.buffer.memoryAddress() + stringResult.start,
          stringResult.end - stringResult.start);
      }
    }
  }

  private static class BinaryAccessor extends ArrowVectorAccessor {

    private final NullableVarBinaryVector.Accessor accessor;

    BinaryAccessor(NullableVarBinaryVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static class DateAccessor extends ArrowVectorAccessor {

    private final NullableDateDayVector.Accessor accessor;

    DateAccessor(NullableDateDayVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class TimestampAccessor extends ArrowVectorAccessor {

    private final NullableTimeStampMicroTZVector.Accessor accessor;

    TimestampAccessor(NullableTimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector.getAccessor();
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class ArrayAccessor extends ArrowVectorAccessor {

    private final UInt4Vector.Accessor accessor;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector.getOffsetVector().getAccessor();
    }

    @Override
    final int getArrayLength(int rowId) {
      return accessor.get(rowId + 1) - accessor.get(rowId);
    }

    @Override
    final int getArrayOffset(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class StructAccessor extends ArrowVectorAccessor {

    StructAccessor(MapVector vector) {
      super(vector);
    }
  }
}
