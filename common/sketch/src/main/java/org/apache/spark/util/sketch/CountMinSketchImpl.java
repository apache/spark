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

package org.apache.spark.util.sketch;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Random;

class CountMinSketchImpl extends CountMinSketch {
  public static final long PRIME_MODULUS = (1L << 31) - 1;

  private int depth;
  private int width;
  private long[][] table;
  private long[] hashA;
  private long totalCount;
  private double eps;
  private double confidence;

  public CountMinSketchImpl(int depth, int width, int seed) {
    this.depth = depth;
    this.width = width;
    this.eps = 2.0 / width;
    this.confidence = 1 - 1 / Math.pow(2, depth);
    initTablesWith(depth, width, seed);
  }

  public CountMinSketchImpl(double eps, double confidence, int seed) {
    // 2/w = eps ; w = 2/eps
    // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
    this.eps = eps;
    this.confidence = confidence;
    this.width = (int) Math.ceil(2 / eps);
    this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
    initTablesWith(depth, width, seed);
  }

  private void initTablesWith(int depth, int width, int seed) {
    this.table = new long[depth][width];
    this.hashA = new long[depth];
    Random r = new Random(seed);
    // We're using a linear hash functions
    // of the form (a*x+b) mod p.
    // a,b are chosen independently for each hash function.
    // However we can set b = 0 as all it does is shift the results
    // without compromising their uniformity or independence with
    // the other hashes.
    for (int i = 0; i < depth; ++i) {
      hashA[i] = r.nextInt(Integer.MAX_VALUE);
    }
  }

  @Override
  public double relativeError() {
    return eps;
  }

  @Override
  public double confidence() {
    return confidence;
  }

  @Override
  public int depth() {
    return depth;
  }

  @Override
  public int width() {
    return width;
  }

  @Override
  public long totalCount() {
    return totalCount;
  }

  @Override
  public void add(Object item) {
    add(item, 1);
  }

  @Override
  public void add(Object item, long count) {
    if (item instanceof String) {
      addString((String) item, count);
    } else {
      long longValue;

      if (item instanceof Long) {
        longValue = (Long) item;
      } else if (item instanceof Integer) {
        longValue = ((Integer) item).longValue();
      } else if (item instanceof Short) {
        longValue = ((Short) item).longValue();
      } else if (item instanceof Byte) {
        longValue = ((Byte) item).longValue();
      } else {
        throw new IllegalArgumentException(
          "Support for " + item.getClass().getName() + " not implemented"
        );
      }

      addLong(longValue, count);
    }
  }

  private void addString(String item, long count) {
    if (count < 0) {
      throw new IllegalArgumentException("Negative increments not implemented");
    }

    int[] buckets = getHashBuckets(item, depth, width);

    for (int i = 0; i < depth; ++i) {
      table[i][buckets[i]] += count;
    }

    totalCount += count;
  }

  private void addLong(long item, long count) {
    if (count < 0) {
      throw new IllegalArgumentException("Negative increments not implemented");
    }

    for (int i = 0; i < depth; ++i) {
      table[i][hash(item, i)] += count;
    }

    totalCount += count;
  }

  private int hash(long item, int count) {
    long hash = hashA[count] * item;
    // A super fast way of computing x mod 2^p-1
    // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
    // page 149, right after Proposition 7.
    hash += hash >> 32;
    hash &= PRIME_MODULUS;
    // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
    return ((int) hash) % width;
  }

  private static int[] getHashBuckets(String key, int hashCount, int max) {
    byte[] b;
    try {
      b = key.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    return getHashBuckets(b, hashCount, max);
  }

  private static int[] getHashBuckets(byte[] b, int hashCount, int max) {
    int[] result = new int[hashCount];
    int hash1 = Murmur3_x86_32.hashUnsafeBytes(b, Platform.BYTE_ARRAY_OFFSET, b.length, 0);
    int hash2 = Murmur3_x86_32.hashUnsafeBytes(b, Platform.BYTE_ARRAY_OFFSET, b.length, hash1);
    for (int i = 0; i < hashCount; i++) {
      result[i] = Math.abs((hash1 + i * hash2) % max);
    }
    return result;
  }

  @Override
  public long estimateCount(Object item) {
    if (item instanceof String) {
      return estimateCountForStringItem((String) item);
    } else {
      long longValue;

      if (item instanceof Long) {
        longValue = (Long) item;
      } else if (item instanceof Integer) {
        longValue = ((Integer) item).longValue();
      } else if (item instanceof Short) {
        longValue = ((Short) item).longValue();
      } else if (item instanceof Byte) {
        longValue = ((Byte) item).longValue();
      } else {
        throw new IllegalArgumentException(
            "Support for " + item.getClass().getName() + " not implemented"
        );
      }

      return estimateCountForLongItem(longValue);
    }
  }

  private long estimateCountForLongItem(long item) {
    long res = Long.MAX_VALUE;
    for (int i = 0; i < depth; ++i) {
      res = Math.min(res, table[i][hash(item, i)]);
    }
    return res;
  }

  private long estimateCountForStringItem(String item) {
    long res = Long.MAX_VALUE;
    int[] buckets = getHashBuckets(item, depth, width);
    for (int i = 0; i < depth; ++i) {
      res = Math.min(res, table[i][buckets[i]]);
    }
    return res;
  }

  @Override
  public CountMinSketch mergeInPlace(CountMinSketch other) {
    if (other == null) {
      throw new CMSMergeException("Cannot merge null estimator");
    }

    if (!(other instanceof CountMinSketchImpl)) {
      throw new CMSMergeException("Cannot merge estimator of class " + other.getClass().getName());
    }

    CountMinSketchImpl that = (CountMinSketchImpl) other;

    if (this.depth != that.depth) {
      throw new CMSMergeException("Cannot merge estimators of different depth");
    }

    if (this.width != that.width) {
      throw new CMSMergeException("Cannot merge estimators of different width");
    }

    if (!Arrays.equals(this.hashA, that.hashA)) {
      throw new CMSMergeException("Cannot merge estimators of different seed");
    }

    for (int i = 0; i < this.table.length; ++i) {
      for (int j = 0; j < this.table[i].length; ++j) {
        this.table[i][j] = this.table[i][j] + that.table[i][j];
      }
    }

    this.totalCount += that.totalCount;

    return this;
  }

  @Override
  public void writeTo(OutputStream out) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  protected static class CMSMergeException extends RuntimeException {
    public CMSMergeException(String message) {
      super(message);
    }
  }
}
