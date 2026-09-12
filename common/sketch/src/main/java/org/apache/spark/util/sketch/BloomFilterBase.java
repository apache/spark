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

import java.util.Objects;

abstract class BloomFilterBase extends BloomFilter {

  public static final int DEFAULT_SEED = 0;

  protected int seed;
  protected int numHashFunctions;
  protected BitArray bits;

  protected BloomFilterBase(int numHashFunctions, long numBits) {
    this(numHashFunctions, numBits, DEFAULT_SEED);
  }

  protected BloomFilterBase(int numHashFunctions, long numBits, int seed) {
    this(new BitArray(numBits), numHashFunctions, seed);
  }

  protected BloomFilterBase(BitArray bits, int numHashFunctions, int seed) {
    this.bits = bits;
    this.numHashFunctions = numHashFunctions;
    this.seed = seed;
  }

  protected BloomFilterBase() {}

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof BloomFilterBase that)) {
      return false;
    }

    return
      this.getClass() == that.getClass()
      && this.numHashFunctions == that.numHashFunctions
      && this.seed == that.seed
      // TODO: this.bits can be null temporarily, during deserialization,
      //  should we worry about this?
      && this.bits.equals(that.bits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(numHashFunctions, seed, bits);
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

  protected HiLoHash hashLongToIntPair(long item, int seed) {
    // Here we first hash the input long element into 2 int hash values, h1 and h2, then produce n
    // hash values by `h1 + i * h2` with 1 <= i <= numHashFunctions.
    // Note that `CountMinSketch` use a different strategy, it hash the input long element with
    // every i to produce n hash values.
    // TODO: the strategy of `CountMinSketch` looks more advanced, should we follow it here?
    int h1 = Murmur3_x86_32.hashLong(item, seed);
    int h2 = Murmur3_x86_32.hashLong(item, h1);
    return new HiLoHash(h1, h2);
  }

  protected HiLoHash hashBytesToIntPair(byte[] item, int seed) {
    int h1 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, seed);
    int h2 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, h1);
    return new HiLoHash(h1, h2);
  }

  /**
   * Reduces a non-negative {@code hash} into a bit index of a bit array of {@code bitSize} bits.
   *
   * <p>A bloom filter probe performs this reduction once per hash function, so it sits in the
   * hottest loop of both {@code put} and {@code mightContain}. {@code bitSize} is not a compile
   * time constant, so {@code hash % bitSize} compiles down to a hardware division. When the bit
   * size is a power of two - which the Spark SQL runtime bloom filter sizes are, see
   * {@code spark.sql.optimizer.runtime.bloomFilter.numBits} - taking a non-negative value modulo
   * it is exactly the same as masking off the low bits, so {@code bitSizeMask} lets us skip the
   * division. The mask is 0 when the bit size is not a power of two, in which case we fall back
   * to the modulo.
   *
   * @param bitSizeMask {@link BitArray#bitSizeMask()} of the bit array being indexed
   */
  protected static long bitIndex(long hash, long bitSize, long bitSizeMask) {
    return bitSizeMask != 0 ? hash & bitSizeMask : hash % bitSize;
  }

  /**
   * Same as {@link #bitIndex(long, long, long)}, for a non-negative {@code hash} that fits in an
   * int. A bit size that fits in an int as well lets the fallback use a 32 bit division, which is
   * cheaper than the 64 bit one that widening the operands would otherwise require.
   */
  protected static long bitIndex(int hash, long bitSize, long bitSizeMask) {
    if (bitSizeMask != 0) {
      return hash & bitSizeMask;
    } else if (bitSize <= Integer.MAX_VALUE) {
      return hash % (int) bitSize;
    } else {
      return hash % bitSize;
    }
  }

  protected abstract boolean scatterHashAndSetAllBits(HiLoHash inputHash);

  protected abstract boolean scatterHashAndGetAllBits(HiLoHash inputHash);

  @Override
  public boolean putString(String item) {
    return putBinary(Utils.getBytesFromUTF8String(item));
  }

  @Override
  public boolean putBinary(byte[] item) {
    HiLoHash hiLoHash = hashBytesToIntPair(item, seed);
    return scatterHashAndSetAllBits(hiLoHash);
  }

  @Override
  public boolean mightContainString(String item) {
    return mightContainBinary(Utils.getBytesFromUTF8String(item));
  }

  @Override
  public boolean mightContainBinary(byte[] item) {
    HiLoHash hiLoHash = hashBytesToIntPair(item, seed);
    return scatterHashAndGetAllBits(hiLoHash);
  }

  public boolean putLong(long item) {
    HiLoHash hiLoHash = hashLongToIntPair(item, seed);
    return scatterHashAndSetAllBits(hiLoHash);
  }

  @Override
  public boolean mightContainLong(long item) {
    HiLoHash hiLoHash = hashLongToIntPair(item, seed);
    return scatterHashAndGetAllBits(hiLoHash);
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
  public boolean isCompatible(BloomFilter other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof BloomFilterBase that)) {
      return false;
    }

    return
      this.getClass() == that.getClass()
      && this.bitSize() == that.bitSize()
      && this.numHashFunctions == that.numHashFunctions
      && this.seed == that.seed;
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException {
    BloomFilterBase otherImplInstance = checkCompatibilityForMerge(other);

    this.bits.putAll(otherImplInstance.bits);
    return this;
  }

  @Override
  public BloomFilter intersectInPlace(BloomFilter other) throws IncompatibleMergeException {
    BloomFilterBase otherImplInstance = checkCompatibilityForMerge(other);

    this.bits.and(otherImplInstance.bits);
    return this;
  }

  @Override
  public long cardinality() {
    return this.bits.cardinality();
  }

  protected abstract BloomFilterBase checkCompatibilityForMerge(BloomFilter other)
    throws IncompatibleMergeException;

  public record HiLoHash(int hi, int lo) {}

}
