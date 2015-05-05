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
 * A simple, fast hash set optimized for non-null insertion-only use case, where keys are never
 * removed.
 *
 * The underlying implementation uses Scala compiler's specialization to generate optimized
 * storage for two primitive types (Long and Int). It is much faster than Java's standard HashSet
 * while incurring much less memory overhead. This can serve as building blocks for higher level
 * data structures such as an optimized HashMap.
 *
 * This OpenHashSet is designed to serve as building blocks for higher level data structures
 * such as an optimized hash map. Compared with standard hash set implementations, this class
 * provides its various callbacks interfaces (e.g. allocateFunc, moveFunc) and interfaces to
 * retrieve the position of a key in the underlying array.
 *
 * It uses quadratic probing with a power-of-2 hash table size, which is guaranteed
 * to explore all spaces for each key (see http://en.wikipedia.org/wiki/Quadratic_probing).
 */
private[spark]
class OpenHashSet[@specialized(Long, Int) T: ClassTag](
    initialCapacity: Int,
    loadFactor: Double)
  extends Serializable {

  require(initialCapacity <= (1 << 29), "Can't make capacity bigger than 2^29 elements")
  require(initialCapacity >= 1, "Invalid initial capacity")
  require(loadFactor < 1.0, "Load factor must be less than 1.0")
  require(loadFactor > 0.0, "Load factor must be greater than 0.0")

  import OpenHashSet._

  def this(initialCapacity: Int) = this(initialCapacity, 0.7)

  def this() = this(64)

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).

  protected val hasher: Hasher[T] = {
    // It would've been more natural to write the following using pattern matching. But Scala 2.9.x
    // compiler has a bug when specialization is used together with this pattern matching, and
    // throws:
    // scala.tools.nsc.symtab.Types$TypeError: type mismatch;
    //  found   : scala.reflect.AnyValManifest[Long]
    //  required: scala.reflect.ClassTag[Int]
    //         at scala.tools.nsc.typechecker.Contexts$Context.error(Contexts.scala:298)
    //         at scala.tools.nsc.typechecker.Infer$Inferencer.error(Infer.scala:207)
    //         ...
    val mt = classTag[T]
    if (mt == ClassTag.Long) {
      (new LongHasher).asInstanceOf[Hasher[T]]
    } else if (mt == ClassTag.Int) {
      (new IntHasher).asInstanceOf[Hasher[T]]
    } else {
      new Hasher[T]
    }
  }

  protected var _capacity = nextPowerOf2(initialCapacity)
  protected var _mask = _capacity - 1
  protected var _size = 0
  protected var _growThreshold = (loadFactor * _capacity).toInt

  protected var _bitset = new BitSet(_capacity)

  def getBitSet: BitSet = _bitset

  // Init of the array in constructor (instead of in declaration) to work around a Scala compiler
  // specialization bug that would generate two arrays (one for Object and one for specialized T).
  protected var _data: Array[T] = _
  _data = new Array[T](_capacity)

  /** Number of elements in the set. */
  def size: Int = _size

  /** The capacity of the set (i.e. size of the underlying array). */
  def capacity: Int = _capacity

  /** Return true if this set contains the specified element. */
  def contains(k: T): Boolean = getPos(k) != INVALID_POS

  /**
   * Add an element to the set. If the set is over capacity after the insertion, grow the set
   * and rehash all elements.
   */
  def add(k: T) {
    addWithoutResize(k)
    rehashIfNeeded(k, grow, move)
  }

  /**
   * Add an element to the set. This one differs from add in that it doesn't trigger rehashing.
   * The caller is responsible for calling rehashIfNeeded.
   *
   * Use (retval & POSITION_MASK) to get the actual position, and
   * (retval & NONEXISTENCE_MASK) == 0 for prior existence.
   *
   * @return The position where the key is placed, plus the highest order bit is set if the key
   *         does not exists previously.
   */
  def addWithoutResize(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {
        // This is a new key.
        _data(pos) = k
        _bitset.set(pos)
        _size += 1
        return pos | NONEXISTENCE_MASK
      } else if (_data(pos) == k) {
        // Found an existing key.
        return pos
      } else {
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /**
   * Rehash the set if it is overloaded.
   * @param k A parameter unused in the function, but to force the Scala compiler to specialize
   *          this method.
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   */
  def rehashIfNeeded(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    if (_size > _growThreshold) {
      rehash(k, allocateFunc, moveFunc)
    }
  }

  /**
   * Return the position of the element in the underlying array, or INVALID_POS if it is not found.
   */
  def getPos(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {
        return INVALID_POS
      } else if (k == _data(pos)) {
        return pos
      } else {
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /** Return the value at the specified position. */
  def getValue(pos: Int): T = _data(pos)

  def iterator: Iterator[T] = new Iterator[T] {
    var pos = nextPos(0)
    override def hasNext: Boolean = pos != INVALID_POS
    override def next(): T = {
      val tmp = getValue(pos)
      pos = nextPos(pos + 1)
      tmp
    }
  }

  /** Return the value at the specified position. */
  def getValueSafe(pos: Int): T = {
    assert(_bitset.get(pos))
    _data(pos)
  }

  /**
   * Return the next position with an element stored, starting from the given position inclusively.
   */
  def nextPos(fromPos: Int): Int = _bitset.nextSetBit(fromPos)

  /**
   * Double the table's size and re-hash everything. We are not really using k, but it is declared
   * so Scala compiler can specialize this method (which leads to calling the specialized version
   * of putInto).
   *
   * @param k A parameter unused in the function, but to force the Scala compiler to specialize
   *          this method.
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   */
  private def rehash(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    val newCapacity = _capacity * 2
    allocateFunc(newCapacity)
    val newBitset = new BitSet(newCapacity)
    val newData = new Array[T](newCapacity)
    val newMask = newCapacity - 1

    var oldPos = 0
    while (oldPos < capacity) {
      if (_bitset.get(oldPos)) {
        val key = _data(oldPos)
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

    _bitset = newBitset
    _data = newData
    _capacity = newCapacity
    _mask = newMask
    _growThreshold = (loadFactor * newCapacity).toInt
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


private[spark]
object OpenHashSet {

  val INVALID_POS = -1
  val NONEXISTENCE_MASK = 0x80000000
  val POSITION_MASK = 0xEFFFFFF

  /**
   * A set of specialized hash function implementation to avoid boxing hash code computation
   * in the specialized implementation of OpenHashSet.
   */
  sealed class Hasher[@specialized(Long, Int) T] extends Serializable {
    def hash(o: T): Int = o.hashCode()
  }

  class LongHasher extends Hasher[Long] {
    override def hash(o: Long): Int = (o ^ (o >>> 32)).toInt
  }

  class IntHasher extends Hasher[Int] {
    override def hash(o: Int): Int = o
  }

  private def grow1(newSize: Int) {}
  private def move1(oldPos: Int, newPos: Int) { }

  private val grow = grow1 _
  private val move = move1 _
}
