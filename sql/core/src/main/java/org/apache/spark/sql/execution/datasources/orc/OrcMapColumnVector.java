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
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector implementation for Spark's {@link MapType}.
 */
public class OrcMapColumnVector extends OrcColumnVector {
  private final OrcColumnVector keys;
  private final OrcColumnVector values;

  OrcMapColumnVector(
      DataType type,
      ColumnVector vector,
      OrcColumnVector keys,
      OrcColumnVector values) {

    super(type, vector);

    this.keys = keys;
    this.values = values;
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    int offsets = (int) ((MapColumnVector) baseData).offsets[ordinal];
    int lengths = (int) ((MapColumnVector) baseData).lengths[ordinal];
    return new ColumnarMap(keys, values, offsets, lengths);
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public byte getByte(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public short getShort(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public int getInt(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public long getLong(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public float getFloat(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public double getDouble(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public org.apache.spark.sql.vectorized.ColumnVector getChild(int ordinal) {
    throw SparkUnsupportedOperationException.apply();
  }
}
