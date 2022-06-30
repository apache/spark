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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Utilities to help manipulate data associate with ColumnVectors. These should be used mostly
 * for debugging or other non-performance critical paths.
 * These utilities are mostly used to convert ColumnVectors into other formats.
 */
public class ColumnVectorUtils {
  /**
   * Populates the entire `col` with `row[fieldIdx]`
   */
  public static void populate(WritableColumnVector col, InternalRow row, int fieldIdx) {
    int capacity = col.capacity;
    DataType t = col.dataType();

    if (row.isNullAt(fieldIdx)) {
      col.putNulls(0, capacity);
    } else {
      if (t == DataTypes.BooleanType) {
        col.putBooleans(0, capacity, row.getBoolean(fieldIdx));
      } else if (t == DataTypes.BinaryType) {
        col.putByteArray(0, row.getBinary(fieldIdx));
      } else if (t == DataTypes.ByteType) {
        col.putBytes(0, capacity, row.getByte(fieldIdx));
      } else if (t == DataTypes.ShortType) {
        col.putShorts(0, capacity, row.getShort(fieldIdx));
      } else if (t == DataTypes.IntegerType) {
        col.putInts(0, capacity, row.getInt(fieldIdx));
      } else if (t == DataTypes.LongType) {
        col.putLongs(0, capacity, row.getLong(fieldIdx));
      } else if (t == DataTypes.FloatType) {
        col.putFloats(0, capacity, row.getFloat(fieldIdx));
      } else if (t == DataTypes.DoubleType) {
        col.putDoubles(0, capacity, row.getDouble(fieldIdx));
      } else if (t == DataTypes.StringType) {
        UTF8String v = row.getUTF8String(fieldIdx);
        byte[] bytes = v.getBytes();
        for (int i = 0; i < capacity; i++) {
          col.putByteArray(i, bytes);
        }
      } else if (t instanceof DecimalType) {
        DecimalType dt = (DecimalType)t;
        Decimal d = row.getDecimal(fieldIdx, dt.precision(), dt.scale());
        if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
          col.putInts(0, capacity, (int)d.toUnscaledLong());
        } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
          col.putLongs(0, capacity, d.toUnscaledLong());
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          byte[] bytes = integer.toByteArray();
          for (int i = 0; i < capacity; i++) {
            col.putByteArray(i, bytes, 0, bytes.length);
          }
        }
      } else if (t instanceof CalendarIntervalType) {
        CalendarInterval c = (CalendarInterval)row.get(fieldIdx, t);
        col.getChild(0).putInts(0, capacity, c.months);
        col.getChild(1).putInts(0, capacity, c.days);
        col.getChild(2).putLongs(0, capacity, c.microseconds);
      } else if (t instanceof DateType || t instanceof YearMonthIntervalType) {
        col.putInts(0, capacity, row.getInt(fieldIdx));
      } else if (t instanceof TimestampType || t instanceof TimestampNTZType ||
        t instanceof DayTimeIntervalType) {
        col.putLongs(0, capacity, row.getLong(fieldIdx));
      } else {
        throw new RuntimeException(String.format("DataType %s is not supported" +
            " in column vectorized reader.", t.sql()));
      }
    }
  }

  /**
   * Populates the value of `row[fieldIdx]` into `ConstantColumnVector`.
   */
  public static void populate(ConstantColumnVector col, InternalRow row, int fieldIdx) {
    DataType t = col.dataType();

    if (row.isNullAt(fieldIdx)) {
      col.setNull();
    } else {
      if (t == DataTypes.BooleanType) {
        col.setBoolean(row.getBoolean(fieldIdx));
      } else if (t == DataTypes.BinaryType) {
        col.setBinary(row.getBinary(fieldIdx));
      } else if (t == DataTypes.ByteType) {
        col.setByte(row.getByte(fieldIdx));
      } else if (t == DataTypes.ShortType) {
        col.setShort(row.getShort(fieldIdx));
      } else if (t == DataTypes.IntegerType) {
        col.setInt(row.getInt(fieldIdx));
      } else if (t == DataTypes.LongType) {
        col.setLong(row.getLong(fieldIdx));
      } else if (t == DataTypes.FloatType) {
        col.setFloat(row.getFloat(fieldIdx));
      } else if (t == DataTypes.DoubleType) {
        col.setDouble(row.getDouble(fieldIdx));
      } else if (t == DataTypes.StringType) {
        UTF8String v = row.getUTF8String(fieldIdx);
        col.setUtf8String(v);
      } else if (t instanceof DecimalType) {
        DecimalType dt = (DecimalType) t;
        Decimal d = row.getDecimal(fieldIdx, dt.precision(), dt.scale());
        if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
          col.setInt((int)d.toUnscaledLong());
        } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
          col.setLong(d.toUnscaledLong());
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          byte[] bytes = integer.toByteArray();
          col.setBinary(bytes);
        }
      } else if (t instanceof CalendarIntervalType) {
        // The value of `numRows` is irrelevant.
        col.setCalendarInterval((CalendarInterval) row.get(fieldIdx, t));
      } else if (t instanceof DateType || t instanceof YearMonthIntervalType) {
        col.setInt(row.getInt(fieldIdx));
      } else if (t instanceof TimestampType || t instanceof TimestampNTZType ||
        t instanceof DayTimeIntervalType) {
        col.setLong(row.getLong(fieldIdx));
      } else {
        throw new RuntimeException(String.format("DataType %s is not supported" +
            " in column vectorized reader.", t.sql()));
      }
    }
  }

  /**
   * Returns the array data as the java primitive array.
   * For example, an array of IntegerType will return an int[].
   * Throws exceptions for unhandled schemas.
   */
  public static int[] toJavaIntArray(ColumnarArray array) {
    for (int i = 0; i < array.numElements(); i++) {
      if (array.isNullAt(i)) {
        throw new RuntimeException("Cannot handle NULL values.");
      }
    }
    return array.toIntArray();
  }

  public static Map<Integer, Integer> toJavaIntMap(ColumnarMap map) {
    int[] keys = toJavaIntArray(map.keyArray());
    int[] values = toJavaIntArray(map.valueArray());
    assert keys.length == values.length;

    Map<Integer, Integer> result = new HashMap<>();
    for (int i = 0; i < keys.length; i++) {
      result.put(keys[i], values[i]);
    }
    return result;
  }

  private static void appendValue(WritableColumnVector dst, DataType t, Object o) {
    if (o == null) {
      if (t instanceof CalendarIntervalType) {
        dst.appendStruct(true);
      } else {
        dst.appendNull();
      }
    } else {
      if (t == DataTypes.BooleanType) {
        dst.appendBoolean((Boolean) o);
      } else if (t == DataTypes.ByteType) {
        dst.appendByte((Byte) o);
      } else if (t == DataTypes.ShortType) {
        dst.appendShort((Short) o);
      } else if (t == DataTypes.IntegerType) {
        dst.appendInt((Integer) o);
      } else if (t == DataTypes.LongType) {
        dst.appendLong((Long) o);
      } else if (t == DataTypes.FloatType) {
        dst.appendFloat((Float) o);
      } else if (t == DataTypes.DoubleType) {
        dst.appendDouble((Double) o);
      } else if (t == DataTypes.StringType) {
        byte[] b =((String)o).getBytes(StandardCharsets.UTF_8);
        dst.appendByteArray(b, 0, b.length);
      } else if (t instanceof DecimalType) {
        DecimalType dt = (DecimalType) t;
        Decimal d = Decimal.apply((BigDecimal) o, dt.precision(), dt.scale());
        if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
          dst.appendInt((int) d.toUnscaledLong());
        } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
          dst.appendLong(d.toUnscaledLong());
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          byte[] bytes = integer.toByteArray();
          dst.appendByteArray(bytes, 0, bytes.length);
        }
      } else if (t instanceof CalendarIntervalType) {
        CalendarInterval c = (CalendarInterval)o;
        dst.appendStruct(false);
        dst.getChild(0).appendInt(c.months);
        dst.getChild(1).appendInt(c.days);
        dst.getChild(2).appendLong(c.microseconds);
      } else if (t instanceof DateType) {
        dst.appendInt(DateTimeUtils.fromJavaDate((Date)o));
      } else {
        throw new UnsupportedOperationException("Type " + t);
      }
    }
  }

  private static void appendValue(WritableColumnVector dst, DataType t, Row src, int fieldIdx) {
    if (t instanceof ArrayType) {
      ArrayType at = (ArrayType)t;
      if (src.isNullAt(fieldIdx)) {
        dst.appendNull();
      } else {
        List<Object> values = src.getList(fieldIdx);
        dst.appendArray(values.size());
        for (Object o : values) {
          appendValue(dst.arrayData(), at.elementType(), o);
        }
      }
    } else if (t instanceof StructType) {
      StructType st = (StructType)t;
      if (src.isNullAt(fieldIdx)) {
        dst.appendStruct(true);
      } else {
        dst.appendStruct(false);
        Row c = src.getStruct(fieldIdx);
        for (int i = 0; i < st.fields().length; i++) {
          appendValue(dst.getChild(i), st.fields()[i].dataType(), c, i);
        }
      }
    } else {
      appendValue(dst, t, src.get(fieldIdx));
    }
  }

  /**
   * Converts an iterator of rows into a single ColumnBatch.
   */
  public static ColumnarBatch toBatch(
      StructType schema, MemoryMode memMode, Iterator<Row> row) {
    int capacity = 4 * 1024;
    WritableColumnVector[] columnVectors;
    if (memMode == MemoryMode.OFF_HEAP) {
      columnVectors = OffHeapColumnVector.allocateColumns(capacity, schema);
    } else {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, schema);
    }

    int n = 0;
    while (row.hasNext()) {
      Row r = row.next();
      for (int i = 0; i < schema.fields().length; i++) {
        appendValue(columnVectors[i], schema.fields()[i].dataType(), r, i);
      }
      n++;
    }
    ColumnarBatch batch = new ColumnarBatch(columnVectors);
    batch.setNumRows(n);
    return batch;
  }
}
