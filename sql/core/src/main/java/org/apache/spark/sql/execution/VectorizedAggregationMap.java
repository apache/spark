package org.apache.spark.sql.execution;

import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.LongType;

public class VectorizedAggregationMap {
  public ColumnarBatch batch;
  private int[] buckets;
  private int numBuckets = 4096;
  private int numRows = 0;
  private int MAX_STEPS = 10;

  public VectorizedAggregationMap() {
    StructType schema = new StructType()
        .add("key1", LongType)
        .add("key2", LongType)
        .add("value", LongType);
    batch = ColumnarBatch.allocate(schema);
    buckets = new int[numBuckets];
    Arrays.fill(buckets, -1);
  }

  private int findOrInsert(long key1, long key2) {
    int h = Math.abs(hash(key1) & hash(key2));
    int step = 0;
    int idx = h & numBuckets;

    while (step < MAX_STEPS) {
      if (buckets[idx] == -1) {
        batch.column(0).putLong(numRows, key1);
        batch.column(1).putLong(numRows, key2);
        batch.column(2).putLong(numRows, 0);
        buckets[idx] = numRows++;
        batch.setNumRows(numRows);
        return idx;
      } else {
        if (equals(idx, key1, key2)) {
          return idx;
        }
      }

      idx += step;
      step += 1;
    }

    // Didn't find it
    return -1;
  }

  public String incrementCount(long key1, long key2) {
    int idx = findOrInsert(key1, key2);
    if (idx == -1) {
      System.out.println(key1 + " " + key2);
      return null;
    }
    batch.column(2).putLong(buckets[idx], batch.column(2).getLong(buckets[idx]) + 1);
    //return batch.getRow(buckets[idx]);
    return "";
  }

  private int hash(long key) {
    return Murmur3_x86_32.hashLong(key, 42);
  }

  private boolean equals(int idx, long key1, long key2) {
    return batch.column(0).getLong(buckets[idx]) == key1 &&
        batch.column(1).getLong(buckets[idx]) == key2;
  }
}
