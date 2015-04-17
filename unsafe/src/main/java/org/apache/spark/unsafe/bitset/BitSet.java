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

package org.apache.spark.unsafe.bitset;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A fixed size uncompressed bit set backed by a {@link LongArray}.
 *
 * Each bit occupies exactly one bit of storage.
 */
public final class BitSet {

  /** A long array for the bits. */
  private final LongArray words;

  /** Length of the long array. */
  private final long numWords;

  /**
   * Creates a new {@link BitSet} using the specified memory block. Size of the memory block must be
   * multiple of 8 bytes (i.e. 64 bits).
   */
  public BitSet(MemoryBlock memory) {
    words = new LongArray(memory);
    numWords = words.size();
  }

  public MemoryBlock memoryBlock() {
    return words.memoryBlock();
  }

  /**
   * Returns the number of bits in this {@code BitSet}.
   */
  public long capacity() {
    return numWords * 64;
  }

  /**
   * Sets the bit at the specified index to {@code true}.
   */
  public void set(long index) {
    assert index < numWords * 64 : "index (" + index + ") should < length (" + numWords * 64 + ")";
    BitSetMethods.set(
      words.memoryBlock().getBaseObject(), words.memoryBlock().getBaseOffset(), index);
  }

  /**
   * Sets the bit at the specified index to {@code false}.
   */
  public void unset(long index) {
    assert index < numWords * 64 : "index (" + index + ") should < length (" + numWords * 64 + ")";
    BitSetMethods.unset(
      words.memoryBlock().getBaseObject(), words.memoryBlock().getBaseOffset(), index);
  }

  /**
   * Returns {@code true} if the bit is set at the specified index.
   */
  public boolean isSet(long index) {
    assert index < numWords * 64 : "index (" + index + ") should < length (" + numWords * 64 + ")";
    return BitSetMethods.isSet(
      words.memoryBlock().getBaseObject(), words.memoryBlock().getBaseOffset(), index);
  }

  /**
   * Returns the number of bits set to {@code true} in this {@link BitSet}.
   */
  public long cardinality() {
    long sum = 0L;
    for (long i = 0; i < numWords; i++) {
      sum += java.lang.Long.bitCount(words.get(i));
    }
    return sum;
  }

  /**
   * Returns the index of the first bit that is set to true that occurs on or after the
   * specified starting index. If no such bit exists then {@code -1} is returned.
   * <p>
   * To iterate over the true bits in a BitSet, use the following loop:
   * <pre>
   * <code>
   *  for (long i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
   *    // operate on index i here
   *  }
   * </code>
   * </pre>
   *
   * @param fromIndex the index to start checking from (inclusive)
   * @return the index of the next set bit, or -1 if there is no such bit
   */
  public long nextSetBit(long fromIndex) {
    return BitSetMethods.nextSetBit(
      words.memoryBlock().getBaseObject(),
      words.memoryBlock().getBaseOffset(),
      fromIndex,
      numWords);
  }
}
