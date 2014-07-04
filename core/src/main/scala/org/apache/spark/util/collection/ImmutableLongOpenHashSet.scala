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

import scala.reflect._
import com.google.common.hash.Hashing

/**
 * A fast, immutable hash set optimized for insertions and lookups (but not deletions) of `Long`
 * elements. Because it exposes the position of a key in the underlying array, this is useful as a
 * building block for higher level data structures such as a hash map (for example,
 * IndexedRDDPartition).
 *
 * It uses quadratic probing with a power-of-2 hash table size, which is guaranteed to explore all
 * spaces for each key (see http://en.wikipedia.org/wiki/Quadratic_probing).
 */
private[spark] class ImmutableLongOpenHashSet(
    /** Underlying array of elements used as a hash table. */
    val data: ImmutableVector[Long],
    /** Whether or not there is an element at the corresponding position in `data`. */
    val bitset: ImmutableBitSet,
    /**
     * Position of a focused element. This is useful when returning a modified set along with a
     * pointer to the location of modification.
     */
    val focus: Int,
    /** Load threshold at which to grow the underlying vectors. */
    loadFactor: Double
  ) extends Serializable {

  require(loadFactor < 1.0, "Load factor must be less than 1.0")
  require(loadFactor > 0.0, "Load factor must be greater than 0.0")
  require(capacity == nextPowerOf2(capacity), "data capacity must be a power of 2")

  import OpenHashSet.{INVALID_POS, NONEXISTENCE_MASK, POSITION_MASK, Hasher, LongHasher}

  private val hasher: Hasher[Long] = new LongHasher

  private def mask = capacity - 1
  private def growThreshold = (loadFactor * capacity).toInt

  def withFocus(focus: Int): ImmutableLongOpenHashSet =
    new ImmutableLongOpenHashSet(data, bitset, focus, loadFactor)

  /** The number of elements in the set. */
  def size: Int = bitset.cardinality

  /** The capacity of the set (i.e. size of the underlying vector). */
  def capacity: Int = data.size

  /** Return true if this set contains the specified element. */
  def contains(k: Long): Boolean = getPos(k) != INVALID_POS

  /**
   * Nondestructively add an element to the set, returning a new set. If the set is over capacity
   * after the insertion, grows the set and rehashes all elements.
   */
  def add(k: Long): ImmutableLongOpenHashSet = {
    addWithoutResize(k).rehashIfNeeded(ImmutableLongOpenHashSet.grow, ImmutableLongOpenHashSet.move)
  }

  /**
   * Add an element to the set. This one differs from add in that it doesn't trigger rehashing.
   * The caller is responsible for calling rehashIfNeeded.
   *
   * Use (retval.focus & POSITION_MASK) to get the actual position, and
   * (retval.focus & NONEXISTENCE_MASK) == 0 for prior existence.
   */
  def addWithoutResize(k: Long): ImmutableLongOpenHashSet = {
    var pos = hashcode(hasher.hash(k)) & mask
    var i = 1
    var result: ImmutableLongOpenHashSet = null
    while (result == null) {
      if (!bitset.get(pos)) {
        // This is a new key.
        result = new ImmutableLongOpenHashSet(
          data.updated(pos, k), bitset.set(pos), pos | NONEXISTENCE_MASK, loadFactor)
      } else if (data(pos) == k) {
        // Found an existing key.
        result = this.withFocus(pos)
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    result
  }

  /**
   * Rehash the set if it is overloaded.
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   */
  def rehashIfNeeded(
      allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit): ImmutableLongOpenHashSet = {
    if (size > growThreshold) {
      rehash(allocateFunc, moveFunc)
    } else {
      this
    }
  }

  /**
   * Return the position of the element in the underlying array, or INVALID_POS if it is not found.
   */
  def getPos(k: Long): Int = {
    var pos = hashcode(hasher.hash(k)) & mask
    var i = 1
    val maxProbe = capacity
    while (i < maxProbe) {
      if (!bitset.get(pos)) {
        return INVALID_POS
      } else if (k == data(pos)) {
        return pos
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    // Never reached here
    INVALID_POS
  }

  /** Return the value at the specified position. */
  def getValue(pos: Int): Long = data(pos)

  def iterator: Iterator[Long] = bitset.iterator.map { pos => getValue(pos) }

  /** Return the value at the specified position. */
  def getValueSafe(pos: Int): Long = {
    assert(bitset.get(pos))
    data(pos)
  }

  /**
   * Double the table's size and re-hash everything.
   *
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   */
  private def rehash(
      allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit): ImmutableLongOpenHashSet = {
    val newCapacity = capacity * 2
    allocateFunc(newCapacity)
    val newBitset = new BitSet(newCapacity)
    val newData = new Array[Long](newCapacity)
    val newMask = newCapacity - 1

    var oldPos = 0
    while (oldPos < capacity) {
      if (bitset.get(oldPos)) {
        val key = data(oldPos)
        var newPos = hashcode(hasher.hash(key)) & newMask
        var i = 1
        var keepGoing = true
        // No need to check for equality here when we insert so this has one less if branch than
        // the similar code path in addWithoutResize.
        while (keepGoing) {
          if (!newBitset.get(newPos)) {
            // Inserting the key at newPos
            newData(newPos) = key
            newBitset.set(newPos)
            moveFunc(oldPos, newPos)
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }

    new ImmutableLongOpenHashSet(
      ImmutableVector.fromArray(newData), newBitset.toImmutableBitSet, -1, loadFactor)
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  private def hashcode(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }
}

private[spark] object ImmutableLongOpenHashSet {
  def empty(initialCapacity: Int, loadFactor: Double): ImmutableLongOpenHashSet =
    new ImmutableLongOpenHashSet(
      ImmutableVector.fromArray(new Array[Long](initialCapacity)),
      new ImmutableBitSet(initialCapacity), -1, loadFactor)

  def empty(initialCapacity: Int): ImmutableLongOpenHashSet = empty(initialCapacity, 0.7)

  def empty(): ImmutableLongOpenHashSet = empty(64, 0.7)

  def fromLongOpenHashSet(set: OpenHashSet[Long]): ImmutableLongOpenHashSet =
    new ImmutableLongOpenHashSet(
      ImmutableVector.fromArray(set.data), set.getBitSet.toImmutableBitSet, -1, set.loadFactor)

  private def grow1(newSize: Int) {}
  private def move1(oldPos: Int, newPos: Int) { }

  private val grow = grow1 _
  private val move = move1 _
}
