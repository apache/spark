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

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector implementation for Spark's {@link StructType}.
 */
public class OrcStructColumnVector extends OrcColumnVector {
  private final OrcColumnVector[] fields;

  OrcStructColumnVector(DataType type, ColumnVector vector, OrcColumnVector[] fields) {
    super(type, vector);

    this.fields = fields;
  }

  @Override
  public org.apache.spark.sql.vectorized.ColumnVector getChild(int ordinal) {
    return fields[ordinal];
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
  public ColumnarMap getMap(int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }
}
