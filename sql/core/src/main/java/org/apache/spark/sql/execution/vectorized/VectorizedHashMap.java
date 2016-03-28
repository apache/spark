package org.apache.spark.sql.execution.vectorized;

import java.util.Arrays;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.LongType;

/**
 * This is an illustrative implementation of a single-key/single value vectorized hash map that can
 * be potentially 'codegened' in TungstenAggregate to speed up aggregate w/ key
 */
public class VectorizedHashMap {
  public ColumnarBatch batch;
  public int[] buckets;
  private int numBuckets;
  private int numRows = 0;
  private int maxSteps = 3;

  public VectorizedHashMap(int capacity, double loadFactor, int maxSteps) {
    StructType schema = new StructType()
        .add("key", LongType)
        .add("value", LongType);
    this.maxSteps = maxSteps;
    numBuckets = capacity;
    batch = ColumnarBatch.allocate(schema, MemoryMode.ON_HEAP, (int) (numBuckets * loadFactor));
    buckets = new int[numBuckets];
    Arrays.fill(buckets, -1);
  }

  public int findOrInsert(long key) {
    int idx = find(key);
    if (idx != -1 && buckets[idx] == -1) {
      batch.column(0).putLong(numRows, key);
      batch.column(1).putLong(numRows, 0);
      buckets[idx] = numRows++;
    }
    return idx;
  }

  public int find(long key) {
    long h = hash(key);
    int step = 0;
    int idx = (int) h & (numBuckets - 1);
    while (step < maxSteps) {
      if ((buckets[idx] == -1) || (buckets[idx] != -1 && equals(idx, key))) return idx;
      idx = (idx + 1) & (numBuckets - 1);
      step++;
    }
    // Didn't find it
    return -1;
  }

  private long hash(long key) {
    return key;
  }

  private boolean equals(int idx, long key1) {
    return batch.column(0).getLong(buckets[idx]) == key1;
  }
}
