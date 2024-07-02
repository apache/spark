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

  private BitArray bits;

  BloomFilterImpl(int numHashFunctions, long numBits, BloomFilterStrategy strategy) {
    this(new BitArray(numBits), numHashFunctions, strategy);
  }

  private BloomFilterImpl(BitArray bits, int numHashFunctions, BloomFilterStrategy strategy) {
    this.bits = bits;
    this.numHashFunctions = numHashFunctions;
    this.strategy = strategy;
  }

  private BloomFilterImpl() {}

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof BloomFilterImpl that)) {
      return false;
    }

    return this.numHashFunctions == that.numHashFunctions && this.bits.equals(that.bits);
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
    if (item instanceof String str) {
      return putString(str);
    } else if (item instanceof byte[] bytes) {
      return putBinary(bytes);
    } else {
      return putLong(Utils.integralToLong(item));
    }
  }

  @Override
  public boolean putString(String item) {
    return strategy.putString(item, bits, numHashFunctions);
  }

  @Override
  public boolean putLong(long item) {
    return strategy.putLong(item, bits, numHashFunctions);
  }

  @Override
  public boolean putBinary(byte[] item) {
    return strategy.putBinary(item, bits, numHashFunctions);
  }

  @Override
  public boolean isCompatible(BloomFilter other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof BloomFilterImpl that)) {
      return false;
    }

    return this.bitSize() == that.bitSize() && this.numHashFunctions == that.numHashFunctions;
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException {
    BloomFilterImpl otherImplInstance = checkCompatibilityForMerge(other);

    this.bits.putAll(otherImplInstance.bits);
    return this;
  }

  @Override
  public BloomFilter intersectInPlace(BloomFilter other) throws IncompatibleMergeException {
    BloomFilterImpl otherImplInstance = checkCompatibilityForMerge(other);

    this.bits.and(otherImplInstance.bits);
    return this;
  }

  private BloomFilterImpl checkCompatibilityForMerge(BloomFilter other)
    throws IncompatibleMergeException {
    // Duplicates the logic of `isCompatible` here to provide better error message.
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof BloomFilterImpl that)) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filter of class " + other.getClass().getName()
      );
    }

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filters with different number of hash functions"
      );
    }
    return that;
  }

  @Override
  public long cardinality() {
    return this.bits.cardinality();
  }

  @Override
  public boolean mightContain(Object item) {
    if (item instanceof String str) {
      return mightContainString(str);
    } else if (item instanceof byte[] bytes) {
      return mightContainBinary(bytes);
    } else {
      return mightContainLong(Utils.integralToLong(item));
    }
  }

  @Override
  public boolean mightContainString(String item) {
    return strategy.mightContainString(item, bits, numHashFunctions);
  }

  @Override
  public boolean mightContainLong(long item) {
    return strategy.mightContainLong(item, bits, numHashFunctions);
  }

  @Override
  public boolean mightContainBinary(byte[] item) {
    return strategy.mightContainBinary(item, bits, numHashFunctions);
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);

    dos.writeInt(Version.V1.getVersionNumber());
    dos.writeInt(numHashFunctions);
    bits.writeTo(dos);
  }

  public static BloomFilterImpl readFrom(InputStream in) throws IOException {
    BloomFilterImpl filter = new BloomFilterImpl();
    filter.readFrom0(in);
    return filter;
  }

  public static BloomFilterImpl readFrom(byte[] bytes) throws IOException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
      return readFrom(bis);
    }
  }

  private void readFrom0(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);

    int version = dis.readInt();
    if (version != Version.V1.getVersionNumber()) {
      throw new IOException("Unexpected Bloom filter version number (" + version + ")");
    }

    this.numHashFunctions = dis.readInt();
    this.bits = BitArray.readFrom(dis);
    this.strategy = currentStrategy();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeTo(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    readFrom0(in);
  }
}
