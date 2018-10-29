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

import java.io.*;
import java.util.Arrays;
import java.util.Random;

class CountMinSketchImpl extends CountMinSketch implements Serializable {
  private static final long PRIME_MODULUS = (1L << 31) - 1;

  private int depth;
  private int width;
  private long[][] table;
  private long[] hashA;
  private long totalCount;
  private double eps;
  private double confidence;

  private CountMinSketchImpl() {}

  CountMinSketchImpl(int depth, int width, int seed) {
    if (depth <= 0 || width <= 0) {
      throw new IllegalArgumentException("Depth and width must be both positive");
    }

    this.depth = depth;
    this.width = width;
    this.eps = 2.0 / width;
    this.confidence = 1 - 1 / Math.pow(2, depth);
    initTablesWith(depth, width, seed);
  }

  CountMinSketchImpl(double eps, double confidence, int seed) {
    if (eps <= 0D) {
      throw new IllegalArgumentException("Relative error must be positive");
    }

    if (confidence <= 0D || confidence >= 1D) {
      throw new IllegalArgumentException("Confidence must be within range (0.0, 1.0)");
    }

    // 2/w = eps ; w = 2/eps
    // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
    this.eps = eps;
    this.confidence = confidence;
    this.width = (int) Math.ceil(2 / eps);
    this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
    initTablesWith(depth, width, seed);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (other == null || !(other instanceof CountMinSketchImpl)) {
      return false;
    }

    CountMinSketchImpl that = (CountMinSketchImpl) other;

    return
      this.depth == that.depth &&
      this.width == that.width &&
      this.totalCount == that.totalCount &&
      Arrays.equals(this.hashA, that.hashA) &&
      Arrays.deepEquals(this.table, that.table);
  }

  @Override
  public int hashCode() {
    int hash = depth;

    hash = hash * 31 + width;
    hash = hash * 31 + (int) (totalCount ^ (totalCount >>> 32));
    hash = hash * 31 + Arrays.hashCode(hashA);
    hash = hash * 31 + Arrays.deepHashCode(table);

    return hash;
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
    } else if (item instanceof byte[]) {
      addBinary((byte[]) item, count);
    } else {
      addLong(Utils.integralToLong(item), count);
    }
  }

  @Override
  public void addString(String item) {
    addString(item, 1);
  }

  @Override
  public void addString(String item, long count) {
    addBinary(Utils.getBytesFromUTF8String(item), count);
  }

  @Override
  public void addLong(long item) {
    addLong(item, 1);
  }

  @Override
  public void addLong(long item, long count) {
    if (count < 0) {
      throw new IllegalArgumentException("Negative increments not implemented");
    }

    for (int i = 0; i < depth; ++i) {
      table[i][hash(item, i)] += count;
    }

    totalCount += count;
  }

  @Override
  public void addBinary(byte[] item) {
    addBinary(item, 1);
  }

  @Override
  public void addBinary(byte[] item, long count) {
    if (count < 0) {
      throw new IllegalArgumentException("Negative increments not implemented");
    }

    int[] buckets = getHashBuckets(item, depth, width);

    for (int i = 0; i < depth; ++i) {
      table[i][buckets[i]] += count;
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
    return getHashBuckets(Utils.getBytesFromUTF8String(key), hashCount, max);
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
    } else if (item instanceof byte[]) {
      return estimateCountForBinaryItem((byte[]) item);
    } else {
      return estimateCountForLongItem(Utils.integralToLong(item));
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

  private long estimateCountForBinaryItem(byte[] item) {
    long res = Long.MAX_VALUE;
    int[] buckets = getHashBuckets(item, depth, width);
    for (int i = 0; i < depth; ++i) {
      res = Math.min(res, table[i][buckets[i]]);
    }
    return res;
  }

  @Override
  public CountMinSketch mergeInPlace(CountMinSketch other) throws IncompatibleMergeException {
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null estimator");
    }

    if (!(other instanceof CountMinSketchImpl)) {
      throw new IncompatibleMergeException(
          "Cannot merge estimator of class " + other.getClass().getName()
      );
    }

    CountMinSketchImpl that = (CountMinSketchImpl) other;

    if (this.depth != that.depth) {
      throw new IncompatibleMergeException("Cannot merge estimators of different depth");
    }

    if (this.width != that.width) {
      throw new IncompatibleMergeException("Cannot merge estimators of different width");
    }

    if (!Arrays.equals(this.hashA, that.hashA)) {
      throw new IncompatibleMergeException("Cannot merge estimators of different seed");
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
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);

    dos.writeInt(Version.V1.getVersionNumber());

    dos.writeLong(this.totalCount);
    dos.writeInt(this.depth);
    dos.writeInt(this.width);

    for (int i = 0; i < this.depth; ++i) {
      dos.writeLong(this.hashA[i]);
    }

    for (int i = 0; i < this.depth; ++i) {
      for (int j = 0; j < this.width; ++j) {
        dos.writeLong(table[i][j]);
      }
    }
  }

  @Override
  public byte[] toByteArray() throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      writeTo(out);
      return out.toByteArray();
    }
  }

  public static CountMinSketchImpl readFrom(InputStream in) throws IOException {
    CountMinSketchImpl sketch = new CountMinSketchImpl();
    sketch.readFrom0(in);
    return sketch;
  }

  private void readFrom0(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);

    int version = dis.readInt();
    if (version != Version.V1.getVersionNumber()) {
      throw new IOException("Unexpected Count-Min Sketch version number (" + version + ")");
    }

    this.totalCount = dis.readLong();
    this.depth = dis.readInt();
    this.width = dis.readInt();
    this.eps = 2.0 / width;
    this.confidence = 1 - 1 / Math.pow(2, depth);

    this.hashA = new long[depth];
    for (int i = 0; i < depth; ++i) {
      this.hashA[i] = dis.readLong();
    }

    this.table = new long[depth][width];
    for (int i = 0; i < depth; ++i) {
      for (int j = 0; j < width; ++j) {
        this.table[i][j] = dis.readLong();
      }
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    this.writeTo(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    this.readFrom0(in);
  }
}
