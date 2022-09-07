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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A column vector interface wrapping Hive's {@link ColumnVector}.
 *
 * Because Spark {@link ColumnarBatch} only accepts Spark's vectorized.ColumnVector,
 * this column vector is used to adapt Hive ColumnVector with Spark ColumnarVector.
 */
public abstract class OrcColumnVector extends org.apache.spark.sql.vectorized.ColumnVector {
  protected final ColumnVector baseData;
  private int batchSize;

  OrcColumnVector(DataType type, ColumnVector vector) {
    super(type);

    baseData = vector;
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

  @Override
  public boolean isNullAt(int rowId) {
    return baseData.isNull[getRowIndex(rowId)];
  }


  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  /* A helper method to get the row index in a column. */
  protected int getRowIndex(int rowId) {
    return baseData.isRepeating ? 0 : rowId;
  }
}
