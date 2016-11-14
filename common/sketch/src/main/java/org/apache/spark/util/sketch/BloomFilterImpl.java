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

class BloomFilterImpl extends BloomFilter implements Serializable {

  private int numHashFunctions;

  private RoaringBitmapArray bits;

  BloomFilterImpl(int numHashFunctions, long numBits) {
    this(new RoaringBitmapArray(numBits), numHashFunctions);
  }

  private BloomFilterImpl(RoaringBitmapArray bits, int numHashFunctions) {
    this.bits = bits;
    this.numHashFunctions = numHashFunctions;
  }

  private BloomFilterImpl() {}

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (other == null || !(other instanceof BloomFilterImpl)) {
      return false;
    }

    BloomFilterImpl that = (BloomFilterImpl) other;

    return (this.numHashFunctions == that.numHashFunctions) && this.bits.equals(that.bits);
  }

  @Override
  public int hashCode() {
    return bits.hashCode() * 31 + numHashFunctions;
  }

  @Override
  public double expectedFpp() {
    return Math.pow((double) bits.cardinality() / bits.bitSize(), numHashFunctions);
  }

  @Override
  public long bitSize() {
    return bits.bitSize();
  }

  @Override
  public boolean put(Object item) {
    if (item instanceof String) {
      return putString((String) item);
    } else if (item instanceof byte[]) {
      return putBinary((byte[]) item);
    } else {
      return putLong(Utils.integralToLong(item));
    }
  }

  @Override
  public boolean putString(String item) {
    return putBinary(Utils.getBytesFromUTF8String(item));
  }

  @Override
  public boolean putBinary(byte[] item) {
    // Strategy is taken from guava`s  BloomFilterStrategies.MURMUR128_MITZ_64
    long[] hashes = new long[2];
    Murmur3_128.hashBytes(item, 0, hashes);
    long h1 = hashes[0];
    long h2 = hashes[1];

    long bitSize = bits.bitSize();
    boolean bitsChanged = false;
    long combinedHash = h1;
    for (int i = 1; i <= numHashFunctions; i++) {
      // Make combinedHash positive and indexable
      bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
      combinedHash += h2;
    }
    return bitsChanged;
  }

  @Override
  public boolean mightContainString(String item) {
    return mightContainBinary(Utils.getBytesFromUTF8String(item));
  }

  @Override
  public boolean mightContainBinary(byte[] item) {
    // Strategy is taken from guava`s  BloomFilterStrategies.MURMUR128_MITZ_64
    long[] hashes = new long[2];
    Murmur3_128.hashBytes(item, 0, hashes);

    long h1 = hashes[0];
    long h2 = hashes[1];

    long bitSize = bits.bitSize();
    long combinedHash = h1;
    for (int i = 1; i <= numHashFunctions; i++) {
      // Make combinedHash positive and indexable
      if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
        return false;
      }
      combinedHash += h2;
    }
    return true;
  }

  @Override
  public boolean putLong(long item) {
    // Strategy is taken from guava`s  BloomFilterStrategies.MURMUR128_MITZ_64
    long[] hashes = new long[2];
    Murmur3_128.hashLong(item, 0, hashes);
    long h1 = hashes[0];
    long h2 = hashes[1];

    long bitSize = bits.bitSize();
    boolean bitsChanged = false;
    long combinedHash = h1;
    for (int i = 1; i <= numHashFunctions; i++) {
      // Make combinedHash positive and indexable
      bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
      combinedHash += h2;
    }
    return bitsChanged;
  }

  @Override
  public boolean mightContainLong(long item) {
    // Strategy is taken from guava`s  BloomFilterStrategies.MURMUR128_MITZ_64
    long[] hashes = new long[2];
    Murmur3_128.hashLong(item, 0, hashes);
    long h1 = hashes[0];
    long h2 = hashes[1];

    long bitSize = bits.bitSize();
    long combinedHash = h1;
    for (int i = 1; i <= numHashFunctions; i++) {
      if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
        return false;
      }
      combinedHash += h2;
    }
    return true;
  }

  @Override
  public boolean mightContain(Object item) {
    if (item instanceof String) {
      return mightContainString((String) item);
    } else if (item instanceof byte[]) {
      return mightContainBinary((byte[]) item);
    } else {
      return mightContainLong(Utils.integralToLong(item));
    }
  }

  @Override
  public boolean isCompatible(BloomFilter other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof BloomFilterImpl)) {
      return false;
    }

    BloomFilterImpl that = (BloomFilterImpl) other;
    return this.bitSize() == that.bitSize() && this.numHashFunctions == that.numHashFunctions;
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException {
    // Duplicates the logic of `isCompatible` here to provide better error message.
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof BloomFilterImpl)) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filter of class " + other.getClass().getName()
      );
    }

    BloomFilterImpl that = (BloomFilterImpl) other;

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filters with different number of hash functions"
      );
    }

    this.bits.putAll(that.bits);
    return this;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);

    dos.writeInt(Version.V1.getVersionNumber());
    dos.writeInt(numHashFunctions);
    bits.writeTo(dos);
  }

  private void readFrom0(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);

    int version = dis.readInt();
    if (version != Version.V1.getVersionNumber()) {
      throw new IOException("Unexpected Bloom filter version number (" + version + ")");
    }

    this.numHashFunctions = dis.readInt();
    this.bits = RoaringBitmapArray.readFrom(dis);
  }

  public static BloomFilterImpl readFrom(InputStream in) throws IOException {
    BloomFilterImpl filter = new BloomFilterImpl();
    filter.readFrom0(in);
    return filter;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeTo(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    readFrom0(in);
  }

}
