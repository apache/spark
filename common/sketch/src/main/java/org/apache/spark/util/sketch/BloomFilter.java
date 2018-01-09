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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Bloom filter is a space-efficient probabilistic data structure that offers an approximate
 * containment test with one-sided error: if it claims that an item is contained in it, this
 * might be in error, but if it claims that an item is <i>not</i> contained in it, then this is
 * definitely true. Currently supported data types include:
 * <ul>
 *   <li>{@link Byte}</li>
 *   <li>{@link Short}</li>
 *   <li>{@link Integer}</li>
 *   <li>{@link Long}</li>
 *   <li>{@link String}</li>
 * </ul>
 * The false positive probability ({@code FPP}) of a Bloom filter is defined as the probability that
 * {@linkplain #mightContain(Object)} will erroneously return {@code true} for an object that has
 * not actually been put in the {@code BloomFilter}.
 *
 * The implementation is largely based on the {@code BloomFilter} class from Guava.
 */
public abstract class BloomFilter {

  public enum Version {
    /**
     * {@code BloomFilter} binary format version 1. All values written in big-endian order:
     * <ul>
     *   <li>Version number, always 1 (32 bit)</li>
     *   <li>Number of hash functions (32 bit)</li>
     *   <li>Total number of words of the underlying bit array (32 bit)</li>
     *   <li>The words/longs (numWords * 64 bit)</li>
     * </ul>
     */
    V1(1);

    private final int versionNumber;

    Version(int versionNumber) {
      this.versionNumber = versionNumber;
    }

    int getVersionNumber() {
      return versionNumber;
    }
  }

  /**
   * Returns the probability that {@linkplain #mightContain(Object)} erroneously return {@code true}
   * for an object that has not actually been put in the {@code BloomFilter}.
   *
   * Ideally, this number should be close to the {@code fpp} parameter passed in
   * {@linkplain #create(long, double)}, or smaller. If it is significantly higher, it is usually
   * the case that too many items (more than expected) have been put in the {@code BloomFilter},
   * degenerating it.
   */
  public abstract double expectedFpp();

  /**
   * Returns the number of bits in the underlying bit array.
   */
  public abstract long bitSize();

  /**
   * Puts an item into this {@code BloomFilter}. Ensures that subsequent invocations of
   * {@linkplain #mightContain(Object)} with the same item will always return {@code true}.
   *
   * @return true if the bloom filter's bits changed as a result of this operation. If the bits
   *     changed, this is <i>definitely</i> the first time {@code object} has been added to the
   *     filter. If the bits haven't changed, this <i>might</i> be the first time {@code object}
   *     has been added to the filter. Note that {@code put(t)} always returns the
   *     <i>opposite</i> result to what {@code mightContain(t)} would have returned at the time
   *     it is called.
   */
  public abstract boolean put(Object item);

  /**
   * A specialized variant of {@link #put(Object)} that only supports {@code String} items.
   */
  public abstract boolean putString(String item);

  /**
   * A specialized variant of {@link #put(Object)} that only supports {@code long} items.
   */
  public abstract boolean putLong(long item);

  /**
   * A specialized variant of {@link #put(Object)} that only supports byte array items.
   */
  public abstract boolean putBinary(byte[] item);

  /**
   * Determines whether a given bloom filter is compatible with this bloom filter. For two
   * bloom filters to be compatible, they must have the same bit size.
   *
   * @param other The bloom filter to check for compatibility.
   */
  public abstract boolean isCompatible(BloomFilter other);

  /**
   * Combines this bloom filter with another bloom filter by performing a bitwise OR of the
   * underlying data. The mutations happen to <b>this</b> instance. Callers must ensure the
   * bloom filters are appropriately sized to avoid saturating them.
   *
   * @param other The bloom filter to combine this bloom filter with. It is not mutated.
   * @throws IncompatibleMergeException if {@code isCompatible(other) == false}
   */
  public abstract BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException;

  /**
   * Returns {@code true} if the element <i>might</i> have been put in this Bloom filter,
   * {@code false} if this is <i>definitely</i> not the case.
   */
  public abstract boolean mightContain(Object item);

  /**
   * A specialized variant of {@link #mightContain(Object)} that only tests {@code String} items.
   */
  public abstract boolean mightContainString(String item);

  /**
   * A specialized variant of {@link #mightContain(Object)} that only tests {@code long} items.
   */
  public abstract boolean mightContainLong(long item);

  /**
   * A specialized variant of {@link #mightContain(Object)} that only tests byte array items.
   */
  public abstract boolean mightContainBinary(byte[] item);

  /**
   * Writes out this {@link BloomFilter} to an output stream in binary format. It is the caller's
   * responsibility to close the stream.
   */
  public abstract void writeTo(OutputStream out) throws IOException;

  /**
   * Reads in a {@link BloomFilter} from an input stream. It is the caller's responsibility to close
   * the stream.
   */
  public static BloomFilter readFrom(InputStream in) throws IOException {
    return BloomFilterImpl.readFrom(in);
  }

  /**
   * Computes the optimal k (number of hashes per item inserted in Bloom filter), given the
   * expected insertions and total number of bits in the Bloom filter.
   *
   * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param m total number of bits in Bloom filter (must be positive)
   */
  private static int optimalNumOfHashFunctions(long n, long m) {
    // (m / n) * log(2), but avoid truncation due to division!
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  /**
   * Computes m (total bits of Bloom filter) which is expected to achieve, for the specified
   * expected insertions, the required false positive probability.
   *
   * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param p false positive rate (must be 0 < p < 1)
   */
  private static long optimalNumOfBits(long n, double p) {
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  static final double DEFAULT_FPP = 0.03;

  /**
   * Creates a {@link BloomFilter} with the expected number of insertions and a default expected
   * false positive probability of 3%.
   *
   * Note that overflowing a {@code BloomFilter} with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  public static BloomFilter create(long expectedNumItems) {
    return create(expectedNumItems, DEFAULT_FPP);
  }

  /**
   * Creates a {@link BloomFilter} with the expected number of insertions and expected false
   * positive probability.
   *
   * Note that overflowing a {@code BloomFilter} with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  public static BloomFilter create(long expectedNumItems, double fpp) {
    if (fpp <= 0D || fpp >= 1D) {
      throw new IllegalArgumentException(
        "False positive probability must be within range (0.0, 1.0)"
      );
    }

    return create(expectedNumItems, optimalNumOfBits(expectedNumItems, fpp));
  }

  /**
   * Creates a {@link BloomFilter} with given {@code expectedNumItems} and {@code numBits}, it will
   * pick an optimal {@code numHashFunctions} which can minimize {@code fpp} for the bloom filter.
   */
  public static BloomFilter create(long expectedNumItems, long numBits) {
    if (expectedNumItems <= 0) {
      throw new IllegalArgumentException("Expected insertions must be positive");
    }

    if (numBits <= 0) {
      throw new IllegalArgumentException("Number of bits must be positive");
    }

    return new BloomFilterImpl(optimalNumOfHashFunctions(expectedNumItems, numBits), numBits);
  }
}
