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

import org.apache.hadoop.hive.ql.exec.vector.*;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector implementation for Spark's AtomicType.
 */
public class OrcAtomicColumnVector extends OrcColumnVector {
  private final boolean isTimestamp;
  private final boolean isDate;

  // Column vector for each type. Only 1 is populated for any type.
  private LongColumnVector longData;
  private DoubleColumnVector doubleData;
  private BytesColumnVector bytesData;
  private DecimalColumnVector decimalData;
  private TimestampColumnVector timestampData;

  OrcAtomicColumnVector(DataType type, ColumnVector vector) {
    super(type, vector);

    if (type instanceof TimestampType) {
      isTimestamp = true;
    } else {
      isTimestamp = false;
    }

    if (type instanceof DateType) {
      isDate = true;
    } else {
      isDate = false;
    }

    if (vector instanceof LongColumnVector longColumnVector) {
      longData = longColumnVector;
    } else if (vector instanceof DoubleColumnVector doubleColumnVector) {
      doubleData = doubleColumnVector;
    } else if (vector instanceof BytesColumnVector bytesColumnVector) {
      bytesData = bytesColumnVector;
    } else if (vector instanceof DecimalColumnVector decimalColumnVector) {
      decimalData = decimalColumnVector;
    } else if (vector instanceof TimestampColumnVector timestampColumnVector) {
      timestampData = timestampColumnVector;
    } else {
      throw SparkUnsupportedOperationException.apply();
    }
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
    int value = (int) longData.vector[getRowIndex(rowId)];
    if (isDate) {
      return RebaseDateTime.rebaseJulianToGregorianDays(value);
    } else {
      return value;
    }
  }

  @Override
  public long getLong(int rowId) {
    int index = getRowIndex(rowId);
    if (isTimestamp) {
      return DateTimeUtils.fromJavaTimestamp(timestampData.asScratchTimestamp(index));
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
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public org.apache.spark.sql.vectorized.ColumnVector getChild(int ordinal) {
    throw SparkUnsupportedOperationException.apply();
  }
}
