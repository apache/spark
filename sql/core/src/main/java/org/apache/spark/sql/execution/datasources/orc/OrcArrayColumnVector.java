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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector implementation for Spark's {@link ArrayType}.
 */
public class OrcArrayColumnVector extends OrcColumnVector {
  private final OrcColumnVector data;

  OrcArrayColumnVector(
      DataType type,
      ColumnVector vector,
      OrcColumnVector data) {

    super(type, vector);

    this.data = data;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    int offsets = (int) ((ListColumnVector) baseData).offsets[rowId];
    int lengths = (int) ((ListColumnVector) baseData).lengths[rowId];
    return new ColumnarArray(data, offsets, lengths);
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary(int rowId) {
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
