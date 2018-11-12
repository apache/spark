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

package org.apache.spark.sql.execution.datasources.orc;

import java.math.BigDecimal;

import org.apache.orc.storage.ql.exec.vector.*;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector class wrapping Hive's ColumnVector. Because Spark ColumnarBatch only accepts
 * Spark's vectorized.ColumnVector, this column vector is used to adapt Hive ColumnVector with
 * Spark ColumnarVector.
 */
public class OrcColumnVector extends org.apache.spark.sql.vectorized.ColumnVector {
  private ColumnVector baseData;
  private LongColumnVector longData;
  private DoubleColumnVector doubleData;
  private BytesColumnVector bytesData;
  private DecimalColumnVector decimalData;
  private TimestampColumnVector timestampData;
  private final boolean isTimestamp;

  private int batchSize;

  OrcColumnVector(DataType type, ColumnVector vector) {
    super(type);

    if (type instanceof TimestampType) {
      isTimestamp = true;
    } else {
      isTimestamp = false;
    }

    baseData = vector;
    if (vector instanceof LongColumnVector) {
      longData = (LongColumnVector) vector;
    } else if (vector instanceof DoubleColumnVector) {
      doubleData = (DoubleColumnVector) vector;
    } else if (vector instanceof BytesColumnVector) {
      bytesData = (BytesColumnVector) vector;
    } else if (vector instanceof DecimalColumnVector) {
      decimalData = (DecimalColumnVector) vector;
    } else if (vector instanceof TimestampColumnVector) {
      timestampData = (TimestampColumnVector) vector;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean hasNull() {
    return !baseData.noNulls;
  }

  @Override
  public int numNulls() {
    if (baseData.isRepeating) {
      if (baseData.isNull[0]) {
        return batchSize;
      } else {
        return 0;
      }
    } else if (baseData.noNulls) {
      return 0;
    } else {
      int count = 0;
      for (int i = 0; i < batchSize; i++) {
        if (baseData.isNull[i]) count++;
      }
      return count;
    }
  }

  /* A helper method to get the row index in a column. */
  private int getRowIndex(int rowId) {
    return baseData.isRepeating ? 0 : rowId;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return baseData.isNull[getRowIndex(rowId)];
  }

  @Override
  public boolean getBoolean(int rowId) {
    return longData.vector[getRowIndex(rowId)] == 1;
  }

  @Override
  public byte getByte(int rowId) {
    return (byte) longData.vector[getRowIndex(rowId)];
  }

  @Override
  public short getShort(int rowId) {
    return (short) longData.vector[getRowIndex(rowId)];
  }

  @Override
  public int getInt(int rowId) {
    return (int) longData.vector[getRowIndex(rowId)];
  }

  @Override
  public long getLong(int rowId) {
    int index = getRowIndex(rowId);
    if (isTimestamp) {
      return timestampData.time[index] * 1000 + timestampData.nanos[index] / 1000 % 1000;
    } else {
      return longData.vector[index];
    }
  }

  @Override
  public float getFloat(int rowId) {
    return (float) doubleData.vector[getRowIndex(rowId)];
  }

  @Override
  public double getDouble(int rowId) {
    return doubleData.vector[getRowIndex(rowId)];
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    BigDecimal data = decimalData.vector[getRowIndex(rowId)].getHiveDecimal().bigDecimalValue();
    return Decimal.apply(data, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) return null;
    int index = getRowIndex(rowId);
    BytesColumnVector col = bytesData;
    return UTF8String.fromBytes(col.vector[index], col.start[index], col.length[index]);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    int index = getRowIndex(rowId);
    byte[] binary = new byte[bytesData.length[index]];
    System.arraycopy(bytesData.vector[index], bytesData.start[index], binary, 0, binary.length);
    return binary;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.spark.sql.vectorized.ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException();
  }
}
