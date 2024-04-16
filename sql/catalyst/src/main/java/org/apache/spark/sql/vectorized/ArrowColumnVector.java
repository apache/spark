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

package org.apache.spark.sql.vectorized;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.NullableIntervalMonthDayNanoHolder;
import org.apache.arrow.vector.holders.NullableLargeVarCharHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector backed by Apache Arrow.
 */
@DeveloperApi
public class ArrowColumnVector extends ColumnVector {

  ArrowVectorAccessor accessor;
  ArrowColumnVector[] childColumns;

  public ValueVector getValueVector() { return accessor.vector; }

  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    accessor.close();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getUTF8String(rowId);
  }

  @Override
  public CalendarInterval getInterval(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getInterval(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getBinary(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getMap(rowId);
  }

  @Override
  public ArrowColumnVector getChild(int ordinal) { return childColumns[ordinal]; }

  ArrowColumnVector(DataType type) {
     super(type);
  }

  public ArrowColumnVector(ValueVector vector) {
    this(ArrowUtils.fromArrowField(vector.getField()));
    initAccessor(vector);
  }

  void initAccessor(ValueVector vector) {
    if (vector instanceof BitVector bitVector) {
      accessor = new BooleanAccessor(bitVector);
    } else if (vector instanceof TinyIntVector tinyIntVector) {
      accessor = new ByteAccessor(tinyIntVector);
    } else if (vector instanceof SmallIntVector smallIntVector) {
      accessor = new ShortAccessor(smallIntVector);
    } else if (vector instanceof IntVector intVector) {
      accessor = new IntAccessor(intVector);
    } else if (vector instanceof BigIntVector bigIntVector) {
      accessor = new LongAccessor(bigIntVector);
    } else if (vector instanceof Float4Vector float4Vector) {
      accessor = new FloatAccessor(float4Vector);
    } else if (vector instanceof Float8Vector float8Vector) {
      accessor = new DoubleAccessor(float8Vector);
    } else if (vector instanceof DecimalVector decimalVector) {
      accessor = new DecimalAccessor(decimalVector);
    } else if (vector instanceof VarCharVector varCharVector) {
      accessor = new StringAccessor(varCharVector);
    } else if (vector instanceof LargeVarCharVector largeVarCharVector) {
      accessor = new LargeStringAccessor(largeVarCharVector);
    } else if (vector instanceof VarBinaryVector varBinaryVector) {
      accessor = new BinaryAccessor(varBinaryVector);
    } else if (vector instanceof LargeVarBinaryVector largeVarBinaryVector) {
      accessor = new LargeBinaryAccessor(largeVarBinaryVector);
    } else if (vector instanceof DateDayVector dateDayVector) {
      accessor = new DateAccessor(dateDayVector);
    } else if (vector instanceof TimeStampMicroTZVector timeStampMicroTZVector) {
      accessor = new TimestampAccessor(timeStampMicroTZVector);
    } else if (vector instanceof TimeStampMicroVector timeStampMicroVector) {
      accessor = new TimestampNTZAccessor(timeStampMicroVector);
    } else if (vector instanceof MapVector mapVector) {
      accessor = new MapAccessor(mapVector);
    } else if (vector instanceof ListVector listVector) {
      accessor = new ArrayAccessor(listVector);
    } else if (vector instanceof StructVector structVector) {
      accessor = new StructAccessor(structVector);

      childColumns = new ArrowColumnVector[structVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new ArrowColumnVector(structVector.getVectorById(i));
      }
    } else if (vector instanceof NullVector nullVector) {
      accessor = new NullAccessor(nullVector);
    } else if (vector instanceof IntervalYearVector intervalYearVector) {
      accessor = new IntervalYearAccessor(intervalYearVector);
    } else if (vector instanceof DurationVector durationVector) {
      accessor = new DurationAccessor(durationVector);
    } else if (vector instanceof IntervalMonthDayNanoVector intervalMonthDayNanoVector) {
      accessor = new IntervalMonthDayNanoAccessor(intervalMonthDayNanoVector);
    } else {
      throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3160");
    }
  }

  abstract static class ArrowVectorAccessor {

    final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    byte getByte(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    short getShort(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    int getInt(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    long getLong(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    float getFloat(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    double getDouble(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    CalendarInterval getInterval(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw SparkUnsupportedOperationException.apply();
    }

    UTF8String getUTF8String(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    byte[] getBinary(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    ColumnarArray getArray(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }

    ColumnarMap getMap(int rowId) {
      throw SparkUnsupportedOperationException.apply();
    }
  }

  static class BooleanAccessor extends ArrowVectorAccessor {

    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  static class ByteAccessor extends ArrowVectorAccessor {

    private final TinyIntVector accessor;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class ShortAccessor extends ArrowVectorAccessor {

    private final SmallIntVector accessor;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class IntAccessor extends ArrowVectorAccessor {

    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class LongAccessor extends ArrowVectorAccessor {

    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class FloatAccessor extends ArrowVectorAccessor {

    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class DoubleAccessor extends ArrowVectorAccessor {

    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class DecimalAccessor extends ArrowVectorAccessor {

    private final DecimalVector accessor;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  static class StringAccessor extends ArrowVectorAccessor {

    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
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

  static class LargeStringAccessor extends ArrowVectorAccessor {

    private final LargeVarCharVector accessor;
    private final NullableLargeVarCharHolder stringResult = new NullableLargeVarCharHolder();

    LargeStringAccessor(LargeVarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
          stringResult.buffer.memoryAddress() + stringResult.start,
          // A single string cannot be larger than the max integer size, so the conversion is safe
          (int)(stringResult.end - stringResult.start));
      }
    }
  }

  static class BinaryAccessor extends ArrowVectorAccessor {

    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  static class LargeBinaryAccessor extends ArrowVectorAccessor {

    private final LargeVarBinaryVector accessor;

    LargeBinaryAccessor(LargeVarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  static class DateAccessor extends ArrowVectorAccessor {

    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class TimestampAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroTZVector accessor;

    TimestampAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class TimestampNTZAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroVector accessor;

    TimestampNTZAccessor(TimeStampMicroVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class ArrayAccessor extends ArrowVectorAccessor {

    private final ListVector accessor;
    private final ArrowColumnVector arrayData;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
      this.arrayData = new ArrowColumnVector(vector.getDataVector());
    }

    @Override
    final ColumnarArray getArray(int rowId) {
      int start = accessor.getElementStartIndex(rowId);
      int end = accessor.getElementEndIndex(rowId);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * Access struct values in a ArrowColumnVector doesn't use this accessor. Instead, it uses
   * getStruct() method defined in the parent class. Any call to "get" method in this class is a
   * bug in the code.
   *
   */
  static class StructAccessor extends ArrowVectorAccessor {

    StructAccessor(StructVector vector) {
      super(vector);
    }
  }

  static class MapAccessor extends ArrowVectorAccessor {
    private final MapVector accessor;
    private final ArrowColumnVector keys;
    private final ArrowColumnVector values;

    MapAccessor(MapVector vector) {
      super(vector);
      this.accessor = vector;
      StructVector entries = (StructVector) vector.getDataVector();
      this.keys = new ArrowColumnVector(entries.getChild(MapVector.KEY_NAME));
      this.values = new ArrowColumnVector(entries.getChild(MapVector.VALUE_NAME));
    }

    @Override
    final ColumnarMap getMap(int rowId) {
      int index = rowId * MapVector.OFFSET_WIDTH;
      int offset = accessor.getOffsetBuffer().getInt(index);
      int length = accessor.getInnerValueCountAt(rowId);
      return new ColumnarMap(keys, values, offset, length);
    }
  }

  static class NullAccessor extends ArrowVectorAccessor {

    NullAccessor(NullVector vector) {
      super(vector);
    }
  }

  static class IntervalYearAccessor extends ArrowVectorAccessor {

    private final IntervalYearVector accessor;

    IntervalYearAccessor(IntervalYearVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  static class DurationAccessor extends ArrowVectorAccessor {

    private final DurationVector accessor;

    DurationAccessor(DurationVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return DurationVector.get(accessor.getDataBuffer(), rowId);
    }
  }

  static class IntervalMonthDayNanoAccessor extends ArrowVectorAccessor {

    private final IntervalMonthDayNanoVector accessor;

    private final NullableIntervalMonthDayNanoHolder result =
      new NullableIntervalMonthDayNanoHolder();

    IntervalMonthDayNanoAccessor(IntervalMonthDayNanoVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    CalendarInterval getInterval(int rowId) {
      accessor.get(rowId, result);
      if (result.isSet == 0) {
        return null;
      } else {
        return new CalendarInterval(result.months, result.days, result.nanoseconds / 1000);
      }
    }
  }
}
