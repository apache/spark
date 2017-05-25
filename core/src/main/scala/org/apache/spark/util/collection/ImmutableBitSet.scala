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
 * An immutable fixed-size bitset backed by `ImmutableVector[Long]`.
 */
private[spark] class ImmutableBitSet(val numBits: Int, val words: ImmutableVector[Long])
  extends Serializable {

  def this(numBits: Int) =
    this(numBits, ImmutableVector.fromArray(new Array(ImmutableBitSet.bit2words(numBits))))

  private def numWords: Int = ImmutableBitSet.bit2words(numBits)

  /** The capacity (number of bits) that can be represented by this bitset. */
  def capacity: Int = numWords * 64

  /** Returns the bit-wise AND of the two sets. */
  def &(other: ImmutableBitSet): ImmutableBitSet = {
    val newWords = new Array[Long](math.max(numWords, other.numWords))
    val thisWordsIter = words.iterator
    val otherWordsIter = other.words.iterator
    var i = 0
    while (thisWordsIter.hasNext && otherWordsIter.hasNext) {
      newWords(i) = thisWordsIter.next() & otherWordsIter.next()
      i += 1
    }
    new ImmutableBitSet(math.max(numBits, other.numBits), ImmutableVector.fromArray(newWords))
  }

  /** Returns the bit-wise OR of the two sets. */
  def |(other: ImmutableBitSet): ImmutableBitSet = {
    val newWords = new Array[Long](math.max(numWords, other.numWords))
    val thisWordsIter = words.iterator
    val otherWordsIter = other.words.iterator
    var i = 0
    while (thisWordsIter.hasNext && otherWordsIter.hasNext) {
      newWords(i) = thisWordsIter.next() | otherWordsIter.next()
      i += 1
    }
    while (thisWordsIter.hasNext) {
      newWords(i) = thisWordsIter.next()
      i += 1
    }
    while (otherWordsIter.hasNext) {
      newWords(i) = otherWordsIter.next()
      i += 1
    }
    new ImmutableBitSet(math.max(numBits, other.numBits), ImmutableVector.fromArray(newWords))
  }

  /** Returns the difference of the two sets by performing bit-wise AND-NOT. */
  def andNot(other: ImmutableBitSet): ImmutableBitSet = {
    val newWords = new Array[Long](math.max(numWords, other.numWords))
    val thisWordsIter = words.iterator
    val otherWordsIter = other.words.iterator
    var i = 0
    while (thisWordsIter.hasNext && otherWordsIter.hasNext) {
      newWords(i) = thisWordsIter.next() & ~otherWordsIter.next()
      i += 1
    }
    while (thisWordsIter.hasNext) {
      newWords(i) = thisWordsIter.next()
      i += 1
    }
    new ImmutableBitSet(math.max(numBits, other.numBits), ImmutableVector.fromArray(newWords))
  }

  /** Returns a new ImmutableBitSet where the bit at the specified index is set to true. */
  def set(index: Int): ImmutableBitSet = {
    val wordIdx = index >> 6         // div by 64
    val bitmask = 1L << (index & 0x3f) // mod 64 and shift
    val oldWord = words(wordIdx)
    val newWord = oldWord | bitmask
    if (newWord == oldWord) this else new ImmutableBitSet(numBits, words.updated(wordIdx, newWord))
  }

  /** Returns a new ImmutableBitSet where the bit at the specified index is set to false. */
  def unset(index: Int): ImmutableBitSet = {
    val wordIdx = index >> 6         // div by 64
    val bitmask = 1L << (index & 0x3f) // mod 64 and shift
    val oldWord = words(wordIdx)
    val newWord = oldWord & ~bitmask
    if (newWord == oldWord) this else new ImmutableBitSet(numBits, words.updated(wordIdx, newWord))
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
  def iterator = words.iterator.zipWithIndex.flatMap(pair => wordIterator(pair._1, pair._2 * 64))

  /** Get an iterator over the bits in the specified word. */
  private def wordIterator(word: Long, globalOffset: Int): Iterator[Int] = new Iterator[Int] {
    private[this] var w = word
    private[this] var o = 0
    override def hasNext: Boolean = w != 0 && o < 64
    override def next() = {
      val step = java.lang.Long.numberOfTrailingZeros(w)
      val result = o + step + globalOffset
      // If step == 63, the shift will have no effect, but o will become >= 64 on the next line,
      // halting iteration.
      w = w >>> step + 1
      o += step + 1
      result
    }
  }

  /** Return the number of bits set to true in this BitSet. */
  def cardinality(): Int = {
    var sum = 0
    val iter = words.iterator
    while (iter.hasNext) {
      sum += java.lang.Long.bitCount(iter.next)
    }
    sum
  }
}

private[spark] object ImmutableBitSet {
  /** Return the number of longs it would take to hold numBits. */
  private def bit2words(numBits: Int) = ((numBits - 1) >> 6) + 1
}
