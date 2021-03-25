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
  private final ColumnVector baseData;
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
