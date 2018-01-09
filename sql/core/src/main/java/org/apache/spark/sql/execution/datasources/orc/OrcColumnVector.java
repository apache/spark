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

  private int batchSize;

  public OrcColumnVector(DataType type, ColumnVector vector) {
    super(type);

    baseData = vector;
    if (vector instanceof LongColumnVector) {
      longData = (LongColumnVector) vector;
    } else if (vector instanceof DoubleColumnVector) {
      doubleData = (DoubleColumnVector) vector;
    } else if (vector instanceof BytesColumnVector) {
      bytesData = (BytesColumnVector) vector;
    } else if (vector instanceof DecimalColumnVector) {
      decimalData = (DecimalColumnVector) vector;
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
  public boolean[] getBooleans(int rowId, int count) {
    boolean[] res = new boolean[count];
    for (int i = 0; i < count; i++) {
      res[i] = getBoolean(rowId + i);
    }
    return res;
  }

  @Override
  public byte getByte(int rowId) {
    return (byte) longData.vector[getRowIndex(rowId)];
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    byte[] res = new byte[count];
    for (int i = 0; i < count; i++) {
      res[i] = getByte(rowId + i);
    }
    return res;
  }

  @Override
  public short getShort(int rowId) {
    return (short) longData.vector[getRowIndex(rowId)];
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    short[] res = new short[count];
    for (int i = 0; i < count; i++) {
      res[i] = getShort(rowId + i);
    }
    return res;
  }

  @Override
  public int getInt(int rowId) {
    return (int) longData.vector[getRowIndex(rowId)];
  }

  @Override
  public int[] getInts(int rowId, int count) {
    int[] res = new int[count];
    for (int i = 0; i < count; i++) {
      res[i] = getInt(rowId + i);
    }
    return res;
  }

  @Override
  public long getLong(int rowId) {
    return longData.vector[getRowIndex(rowId)];
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    long[] res = new long[count];
    for (int i = 0; i < count; i++) {
      res[i] = getLong(rowId + i);
    }
    return res;
  }

  @Override
  public float getFloat(int rowId) {
    return (float) doubleData.vector[getRowIndex(rowId)];
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    float[] res = new float[count];
    for (int i = 0; i < count; i++) {
      res[i] = getFloat(rowId + i);
    }
    return res;
  }

  @Override
  public double getDouble(int rowId) {
    return doubleData.vector[getRowIndex(rowId)];
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    double[] res = new double[count];
    for (int i = 0; i < count; i++) {
      res[i] = getDouble(rowId + i);
    }
    return res;
  }

  @Override
  public int getArrayLength(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getArrayOffset(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    BigDecimal data = decimalData.vector[getRowIndex(rowId)].getHiveDecimal().bigDecimalValue();
    return Decimal.apply(data, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    int index = getRowIndex(rowId);
    BytesColumnVector col = bytesData;
    return UTF8String.fromBytes(col.vector[index], col.start[index], col.length[index]);
  }

  @Override
  public byte[] getBinary(int rowId) {
    int index = getRowIndex(rowId);
    byte[] binary = new byte[bytesData.length[index]];
    System.arraycopy(bytesData.vector[index], bytesData.start[index], binary, 0, binary.length);
    return binary;
  }

  @Override
  public org.apache.spark.sql.vectorized.ColumnVector arrayData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.spark.sql.vectorized.ColumnVector getChildColumn(int ordinal) {
    throw new UnsupportedOperationException();
  }
}
