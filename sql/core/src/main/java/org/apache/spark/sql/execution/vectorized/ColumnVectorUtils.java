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
import java.util.Iterator;
import java.util.List;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.*;
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
  public static void populate(ColumnVector col, InternalRow row, int fieldIdx) {
    int capacity = col.capacity;
    DataType t = col.dataType();

    if (row.isNullAt(fieldIdx)) {
      col.putNulls(0, capacity);
    } else {
      if (t == DataTypes.BooleanType) {
        col.putBooleans(0, capacity, row.getBoolean(fieldIdx));
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
        col.getChildColumn(0).putInts(0, capacity, c.months);
        col.getChildColumn(1).putLongs(0, capacity, c.microseconds);
      } else if (t instanceof DateType) {
        Date date = (Date)row.get(fieldIdx, t);
        col.putInts(0, capacity, DateTimeUtils.fromJavaDate(date));
      }
    }
  }

  /**
   * Returns the array data as the java primitive array.
   * For example, an array of IntegerType will return an int[].
   * Throws exceptions for unhandled schemas.
   */
  public static Object toPrimitiveJavaArray(ColumnVector.Array array) {
    DataType dt = array.data.dataType();
    if (dt instanceof IntegerType) {
      int[] result = new int[array.length];
      ColumnVector data = array.data;
      for (int i = 0; i < result.length; i++) {
        if (data.isNullAt(array.offset + i)) {
          throw new RuntimeException("Cannot handle NULL values.");
        }
        result[i] = data.getInt(array.offset + i);
      }
      return result;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private static void appendValue(ColumnVector dst, DataType t, Object o) {
    if (o == null) {
      if (t instanceof CalendarIntervalType) {
        dst.appendStruct(true);
      } else {
        dst.appendNull();
      }
    } else {
      if (t == DataTypes.BooleanType) {
        dst.appendBoolean(((Boolean)o).booleanValue());
      } else if (t == DataTypes.ByteType) {
        dst.appendByte(((Byte) o).byteValue());
      } else if (t == DataTypes.ShortType) {
        dst.appendShort(((Short)o).shortValue());
      } else if (t == DataTypes.IntegerType) {
        dst.appendInt(((Integer)o).intValue());
      } else if (t == DataTypes.LongType) {
        dst.appendLong(((Long)o).longValue());
      } else if (t == DataTypes.FloatType) {
        dst.appendFloat(((Float)o).floatValue());
      } else if (t == DataTypes.DoubleType) {
        dst.appendDouble(((Double)o).doubleValue());
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
        dst.getChildColumn(0).appendInt(c.months);
        dst.getChildColumn(1).appendLong(c.microseconds);
      } else if (t instanceof DateType) {
        dst.appendInt(DateTimeUtils.fromJavaDate((Date)o));
      } else {
        throw new UnsupportedOperationException("Type " + t);
      }
    }
  }

  private static void appendValue(ColumnVector dst, DataType t, Row src, int fieldIdx) {
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
          appendValue(dst.getChildColumn(i), st.fields()[i].dataType(), c, i);
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
    ColumnarBatch batch = ColumnarBatch.allocate(schema, memMode);
    int n = 0;
    while (row.hasNext()) {
      Row r = row.next();
      for (int i = 0; i < schema.fields().length; i++) {
        appendValue(batch.column(i), schema.fields()[i].dataType(), r, i);
      }
      n++;
    }
    batch.setNumRows(n);
    return batch;
  }
}
