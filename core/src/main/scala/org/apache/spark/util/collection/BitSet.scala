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

package org.apache.spark.util.collection

/**
 * A simple, fixed-size bit set implementation. This implementation is fast because it avoids
 * safety/bound checking.
 */
class BitSet(numBits: Int) extends Serializable {

  private val words = new Array[Long](bit2words(numBits))
  private val numWords = words.length

  /**
   * Compute the capacity (number of bits) that can be represented
   * by this bitset.
   */
  def capacity: Int = numWords * 64

  /**
   * Clear all set bits.
   */
  def clear(): Unit = {
    var i = 0
    while (i < numWords) {
      words(i) = 0L
      i += 1
    }
  }

  /**
   * Set all the bits up to a given index
   */
  def setUntil(bitIndex: Int) {
    val wordIndex = bitIndex >> 6 // divide by 64
    var i = 0
    while(i < wordIndex) { words(i) = -1; i += 1 }
    if(wordIndex < words.length) {
      // Set the remaining bits (note that the mask could still be zero)
      val mask = ~(-1L << (bitIndex & 0x3f))
      words(wordIndex) |= mask
    }
  }

  /**
   * Clear all the bits up to a given index
   */
  def clearUntil(bitIndex: Int) {
    val wordIndex = bitIndex >> 6 // divide by 64
    var i = 0
    while(i < wordIndex) { words(i) = 0; i += 1 }
    if(wordIndex < words.length) {
      // Set the remaining bits (note that the mask could still be zero)
      val mask = ~(-1L << (bitIndex & 0x3f))
      words(wordIndex) &= ~mask
    }
  }

  /**
   * Compute the bit-wise AND of the two sets returning the
   * result.
   */
  def &(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    var ind = 0
    while( ind < smaller ) {
      newBS.words(ind) = words(ind) & other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
   * Compute the bit-wise OR of the two sets returning the
   * result.
   */
  def |(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while( ind < smaller ) {
      newBS.words(ind) = words(ind) | other.words(ind)
      ind += 1
    }
    while( ind < numWords ) {
      newBS.words(ind) = words(ind)
      ind += 1
    }
    while( ind < other.numWords ) {
      newBS.words(ind) = other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
   * Compute the symmetric difference by performing bit-wise XOR of the two sets returning the
   * result.
   */
  def ^(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) ^ other.words(ind)
      ind += 1
    }
    if (ind < numWords) {
      Array.copy( words, ind, newBS.words, ind, numWords - ind )
    }
    if (ind < other.numWords) {
      Array.copy( other.words, ind, newBS.words, ind, other.numWords - ind )
    }
    newBS
  }

  /**
   * Compute the difference of the two sets by performing bit-wise AND-NOT returning the
   * result.
   */
  def andNot(other: BitSet): BitSet = {
    val newBS = new BitSet(capacity)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) & ~other.words(ind)
      ind += 1
    }
    if (ind < numWords) {
      Array.copy( words, ind, newBS.words, ind, numWords - ind )
    }
    newBS
  }

  /**
   * Sets the bit at the specified index to true.
   * @param index the bit index
   */
  def set(index: Int) {
    val bitmask = 1L << (index & 0x3f)  // mod 64 and shift
    words(index >> 6) |= bitmask        // div by 64 and mask
  }

  def unset(index: Int) {
    val bitmask = 1L << (index & 0x3f)  // mod 64 and shift
    words(index >> 6) &= ~bitmask        // div by 64 and mask
  }

  /**
   * Return the value of the bit with the specified index. The value is true if the bit with
   * the index is currently set in this BitSet; otherwise, the result is false.
   *
   * @param index the bit index
   * @return the value of the bit with the specified index
   */
  def get(index: Int): Boolean = {
    val bitmask = 1L << (index & 0x3f)   // mod 64 and shift
    (words(index >> 6) & bitmask) != 0  // div by 64 and mask
  }

  /**
   * Get an iterator over the set bits.
   */
  def iterator: Iterator[Int] = new Iterator[Int] {
    var ind = nextSetBit(0)
    override def hasNext: Boolean = ind >= 0
    override def next(): Int = {
      val tmp = ind
      ind = nextSetBit(ind + 1)
      tmp
    }
  }


  /** Return the number of bits set to true in this BitSet. */
  def cardinality(): Int = {
    var sum = 0
    var i = 0
    while (i < numWords) {
      sum += java.lang.Long.bitCount(words(i))
      i += 1
    }
    sum
  }

  /**
   * Returns the index of the first bit that is set to true that occurs on or after the
   * specified starting index. If no such bit exists then -1 is returned.
   *
   * To iterate over the true bits in a BitSet, use the following loop:
   *
   *  for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
   *    // operate on index i here
   *  }
   *
   * @param fromIndex the index to start checking from (inclusive)
   * @return the index of the next set bit, or -1 if there is no such bit
   */
  def nextSetBit(fromIndex: Int): Int = {
    var wordIndex = fromIndex >> 6
    if (wordIndex >= numWords) {
      return -1
    }

    // Try to find the next set bit in the current word
    val subIndex = fromIndex & 0x3f
    var word = words(wordIndex) >> subIndex
    if (word != 0) {
      return (wordIndex << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word)
    }

    // Find the next set bit in the rest of the words
    wordIndex += 1
    while (wordIndex < numWords) {
      word = words(wordIndex)
      if (word != 0) {
        return (wordIndex << 6) + java.lang.Long.numberOfTrailingZeros(word)
      }
      wordIndex += 1
    }

    -1
  }

  /** Return the number of longs it would take to hold numBits. */
  private def bit2words(numBits: Int) = ((numBits - 1) >> 6) + 1
}
