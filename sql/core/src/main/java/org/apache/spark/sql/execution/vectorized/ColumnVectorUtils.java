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
import org.apache.spark.sql.catalyst.types.*;
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
   * Populates the value of `row[fieldIdx]` into `ConstantColumnVector`.
   */
  public static void populate(ConstantColumnVector col, InternalRow row, int fieldIdx) {
    DataType dt = col.dataType();
    PhysicalDataType pdt = dt.physicalDataType();

    if (row.isNullAt(fieldIdx)) {
      col.setNull();
    } else {
      if (pdt == DataTypes.BooleanType.physicalDataType()) {
        col.setBoolean(row.getBoolean(fieldIdx));
      } else if (pdt == DataTypes.BinaryType.physicalDataType()) {
        col.setBinary(row.getBinary(fieldIdx));
      } else if (pdt == DataTypes.ByteType.physicalDataType()) {
        col.setByte(row.getByte(fieldIdx));
      } else if (pdt == DataTypes.ShortType.physicalDataType()) {
        col.setShort(row.getShort(fieldIdx));
      } else if (pdt == DataTypes.IntegerType.physicalDataType()) {
        col.setInt(row.getInt(fieldIdx));
      } else if (pdt == DataTypes.LongType.physicalDataType()) {
        col.setLong(row.getLong(fieldIdx));
      } else if (pdt == DataTypes.FloatType.physicalDataType()) {
        col.setFloat(row.getFloat(fieldIdx));
      } else if (pdt == DataTypes.DoubleType.physicalDataType()) {
        col.setDouble(row.getDouble(fieldIdx));
      } else if (pdt == DataTypes.StringType.physicalDataType()) {
        UTF8String v = row.getUTF8String(fieldIdx);
        col.setUtf8String(v);
      } else if (pdt instanceof PhysicalDecimalType) {
        PhysicalDecimalType pd = (PhysicalDecimalType) pdt;
        Decimal d = row.getDecimal(fieldIdx, pd.precision(), pd.scale());
        if (pd.precision() <= Decimal.MAX_INT_DIGITS()) {
          col.setInt((int)d.toUnscaledLong());
        } else if (pd.precision() <= Decimal.MAX_LONG_DIGITS()) {
          col.setLong(d.toUnscaledLong());
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          byte[] bytes = integer.toByteArray();
          col.setBinary(bytes);
        }
      } else if (pdt == DataTypes.CalendarIntervalType.physicalDataType()) {
        // The value of `numRows` is irrelevant.
        col.setCalendarInterval((CalendarInterval) row.get(fieldIdx, dt));
      } else {
        throw new RuntimeException(String.format("DataType %s is not supported" +
            " in column vectorized reader.", dt.sql()));
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
    PhysicalDataType pdt = t.physicalDataType();
    if (o == null) {
      if (pdt instanceof PhysicalCalendarIntervalType) {
        dst.appendStruct(true);
      } else {
        dst.appendNull();
      }
    } else {
      if (pdt == DataTypes.BooleanType.physicalDataType()) {
        dst.appendBoolean((Boolean) o);
      } else if (pdt == DataTypes.ByteType.physicalDataType()) {
        dst.appendByte((Byte) o);
      } else if (pdt == DataTypes.ShortType.physicalDataType()) {
        dst.appendShort((Short) o);
      } else if (pdt == DataTypes.IntegerType.physicalDataType()) {
        if (o instanceof Date) {
          dst.appendInt(DateTimeUtils.fromJavaDate((Date) o));
        } else {
          dst.appendInt((Integer) o);
        }
      } else if (pdt == DataTypes.LongType.physicalDataType()) {
        dst.appendLong((Long) o);
      } else if (pdt == DataTypes.FloatType.physicalDataType()) {
        dst.appendFloat((Float) o);
      } else if (pdt == DataTypes.DoubleType.physicalDataType()) {
        dst.appendDouble((Double) o);
      } else if (pdt == DataTypes.StringType.physicalDataType()) {
        byte[] b =((String)o).getBytes(StandardCharsets.UTF_8);
        dst.appendByteArray(b, 0, b.length);
      } else if (pdt instanceof PhysicalDecimalType) {
        PhysicalDecimalType dt = (PhysicalDecimalType) pdt;
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
      } else if (pdt instanceof PhysicalCalendarIntervalType) {
        CalendarInterval c = (CalendarInterval)o;
        dst.appendStruct(false);
        dst.getChild(0).appendInt(c.months);
        dst.getChild(1).appendInt(c.days);
        dst.getChild(2).appendLong(c.microseconds);
      } else {
        throw new UnsupportedOperationException("Type " + t);
      }
    }
  }

  private static void appendValue(WritableColumnVector dst, DataType t, Row src, int fieldIdx) {
    PhysicalDataType pdt = t.physicalDataType();
    if (pdt instanceof PhysicalArrayType) {
      PhysicalArrayType at = (PhysicalArrayType) pdt;
      if (src.isNullAt(fieldIdx)) {
        dst.appendNull();
      } else {
        List<Object> values = src.getList(fieldIdx);
        dst.appendArray(values.size());
        for (Object o : values) {
          appendValue(dst.arrayData(), at.elementType(), o);
        }
      }
    } else if (pdt instanceof PhysicalStructType) {
      PhysicalStructType st = (PhysicalStructType) pdt;
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
