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

import java.util.Iterator;
import java.util.List;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

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
