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
class OpenHashSet[@specialized(Long, Int) T: ClassManifest](
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
    //  required: scala.reflect.ClassManifest[Int]
    //         at scala.tools.nsc.typechecker.Contexts$Context.error(Contexts.scala:298)
    //         at scala.tools.nsc.typechecker.Infer$Inferencer.error(Infer.scala:207)
    //         ...
    val mt = classManifest[T]
    if (mt == ClassManifest.Long) {
      (new LongHasher).asInstanceOf[Hasher[T]]
    } else if (mt == ClassManifest.Int) {
      (new IntHasher).asInstanceOf[Hasher[T]]
    } else {
      new Hasher[T]
    }
  }

  protected var _capacity = nextPowerOf2(initialCapacity)
  protected var _mask = _capacity - 1
  protected var _size = 0

  protected var _bitset = new BitSet(_capacity)

  def getBitSet = _bitset

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
   * (retval & EXISTENCE_MASK) != 0 for prior existence.
   *
   * @return The position where the key is placed, plus the highest order bit is set if the key
   *         exists previously.
   */
  def addWithoutResize(k: T): Int = putInto(_bitset, _data, k)

  /**
   * Rehash the set if it is overloaded.
   * @param k A parameter unused in the function, but to force the Scala compiler to specialize
   *          this method.
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   */
  def rehashIfNeeded(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    if (_size > loadFactor * _capacity) {
      rehash(k, allocateFunc, moveFunc)
    }
  }

  /**
   * Return the position of the element in the underlying array, or INVALID_POS if it is not found.
   */
  def getPos(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var i = 1
    val maxProbe = _data.size
    while (i < maxProbe) {
      if (!_bitset.get(pos)) {
        return INVALID_POS
      } else if (k == _data(pos)) {
        return pos
      } else {
        val delta = i
        pos = (pos + delta) & _mask
        i += 1
      }
    }
    // Never reached here
    INVALID_POS
  }

  /** Return the value at the specified position. */
  def getValue(pos: Int): T = _data(pos)

  def iterator = new Iterator[T] {
    var pos = nextPos(0)
    override def hasNext: Boolean = pos != INVALID_POS
    override def next(): T = {
      val tmp = getValue(pos)
      pos = nextPos(pos+1)
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
   * Put an entry into the set. Return the position where the key is placed. In addition, the
   * highest bit in the returned position is set if the key exists prior to this put.
   *
   * This function assumes the data array has at least one empty slot.
   */
  private def putInto(bitset: BitSet, data: Array[T], k: T): Int = {
    val mask = data.length - 1
    var pos = hashcode(hasher.hash(k)) & mask
    var i = 1
    while (true) {
      if (!bitset.get(pos)) {
        // This is a new key.
        data(pos) = k
        bitset.set(pos)
        _size += 1
        return pos | NONEXISTENCE_MASK
      } else if (data(pos) == k) {
        // Found an existing key.
        return pos
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    // Never reached here
    assert(INVALID_POS != INVALID_POS)
    INVALID_POS
  }

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
    require(newCapacity <= (1 << 29), "Can't make capacity bigger than 2^29 elements")

    allocateFunc(newCapacity)
    val newData = new Array[T](newCapacity)
    val newBitset = new BitSet(newCapacity)
    var pos = 0
    _size = 0
    while (pos < _capacity) {
      if (_bitset.get(pos)) {
        val newPos = putInto(newBitset, newData, _data(pos))
        moveFunc(pos, newPos & POSITION_MASK)
      }
      pos += 1
    }
    _bitset = newBitset
    _data = newData
    _capacity = newCapacity
    _mask = newCapacity - 1
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ
   * in the lower bits, similar to java.util.HashMap
   */
  private def hashcode(h: Int): Int = {
    val r = h ^ (h >>> 20) ^ (h >>> 12)
    r ^ (r >>> 7) ^ (r >>> 4)
  }

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
  sealed class Hasher[@specialized(Long, Int) T] {
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
