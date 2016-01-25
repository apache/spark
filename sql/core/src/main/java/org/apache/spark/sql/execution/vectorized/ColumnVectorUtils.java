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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.StringTranslate;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.commons.lang.NotImplementedException;

/**
 * Utilities to help manipulate data associate with ColumnVectors. These should be used mostly
 * for debugging or other non-performance critical paths.
 * These utilities are mostly used to convert ColumnVectors into other formats.
 */
public class ColumnVectorUtils {
  public static String toString(ColumnVector.Array a) {
    return new String(a.byteArray, a.byteArrayOffset, a.length);
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
        if (data.getIsNull(array.offset + i)) {
          throw new RuntimeException("Cannot handle NULL values.");
        }
        result[i] = data.getInt(array.offset + i);
      }
      return result;
    } else {
      throw new NotImplementedException();
    }
  }

  public static GenericMutableRow toRow(ColumnVector.Struct struct) {
    GenericMutableRow row = new GenericMutableRow(struct.fields.length);

    for (int i = 0; i < struct.fields.length; i++) {
      if (struct.getIsNull(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = struct.fields[i].dataType();
        if (dt instanceof ByteType) {
          row.setByte(i, struct.getByte(i));
        } else if (dt instanceof IntegerType) {
          row.setInt(i, struct.getInt(i));
        } else if (dt instanceof LongType) {
          row.setLong(i, struct.getLong(i));
        } else if (dt instanceof DoubleType) {
          row.setDouble(i, struct.getDouble(i));
        } else if (dt instanceof StringType) {
          ColumnVector.Array a = struct.getByteArray(i);
          row.update(i, UTF8String.fromBytes(a.byteArray, a.byteArrayOffset, a.length));
        } else if (dt instanceof StructType) {
          GenericMutableRow child = toRow(struct.getStruct(i));
          row.update(i, child);
        } else if (dt instanceof ArrayType) {
          row.update(i, toGenericArray(struct.getArray(i)));
        } else {
          throw new RuntimeException("Not implemented. " + dt);
        }
      }
    }

    return row;
  }

  /**
   * Converts an ColumnVector array into a GenericArrayData. This is very expensive to do.
   */
  public static GenericArrayData toGenericArray(ColumnVector.Array array) {
    DataType dt = array.data.dataType();
    List<Object> list = new ArrayList<Object>(array.length);
    ColumnVector data = array.data;

    if (dt instanceof ByteType) {
      for (int i = 0; i < array.length; i++) {
        if (data.getIsNull(array.offset + i)) {
          list.add(null);
        } else {
          list.add(data.getByte(array.offset + i));
        }
      }
    } else if (dt instanceof IntegerType) {
      for (int i = 0; i < array.length; i++) {
        if (data.getIsNull(array.offset + i)) {
          list.add(null);
        } else {
          list.add(data.getInt(array.offset + i));
        }
      }
    } else if (dt instanceof DoubleType) {
      for (int i = 0; i < array.length; i++) {
        if (data.getIsNull(array.offset + i)) {
          list.add(null);
        } else {
          list.add(data.getDouble(array.offset + i));
        }
      }
    } else if (dt instanceof LongType) {
      for (int i = 0; i < array.length; i++) {
        if (data.getIsNull(array.offset + i)) {
          list.add(null);
        } else {
          list.add(data.getLong(array.offset + i));
        }
      }
    } else if (dt instanceof StringType) {
      for (int i = 0; i < array.length; i++) {
        if (data.getIsNull(array.offset + i)) {
          list.add(null);
        } else {
          list.add(toString(data.getByteArray(array.offset + i)));
        }
      }
    } else {
      throw new NotImplementedException("Type " + dt);
    }
    return new GenericArrayData(list);
  }

  private static void appendValue(ColumnVector dst, DataType t, Object o) {
    if (o == null) {
      dst.appendNull();
    } else {
      if (t == DataTypes.ByteType) {
        dst.appendByte(((Byte)o).byteValue());
      } else if (t == DataTypes.IntegerType) {
        dst.appendInt(((Integer)o).intValue());
      } else if (t == DataTypes.LongType) {
        dst.appendLong(((Long)o).longValue());
      } else if (t == DataTypes.DoubleType) {
        dst.appendDouble(((Double)o).doubleValue());
      } else if (t == DataTypes.StringType) {
        byte[] b =((String)o).getBytes();
        dst.appendByteArray(b, 0, b.length);
      } else {
        throw new NotImplementedException("Type " + t);
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
