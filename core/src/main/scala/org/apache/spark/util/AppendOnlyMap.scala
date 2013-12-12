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

package org.apache.spark.util

/**
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 */
private[spark]
class AppendOnlyMap[K, V](initialCapacity: Int = 64) extends Iterable[(K, V)] with Serializable {
  require(initialCapacity <= (1 << 29), "Can't make capacity bigger than 2^29 elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  private var capacity = nextPowerOf2(initialCapacity)
  private var mask = capacity - 1
  private var curSize = 0
  private var growThreshold = LOAD_FACTOR * capacity

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  private var haveNullValue = false
  private var nullValue: V = null.asInstanceOf[V]

  private val LOAD_FACTOR = 0.7

  /** Get the value for a given key */
  def apply(key: K): V = {
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    return null.asInstanceOf[V]
  }

  /** Set the value for a key */
  def update(key: K, value: V): Unit = {
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = value
      haveNullValue = true
      return
    }
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        incrementSize()  // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else if (curKey.eq(null)) {
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** Iterator method from Iterable */
  override def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    var pos = -1

    /** Get the next value we should return from next(), or null if we're finished iterating */
    def nextValue(): (K, V) = {
      if (pos == -1) {    // Treat position -1 as looking at the null value
        if (haveNullValue) {
          return (null.asInstanceOf[K], nullValue)
        }
        pos += 1
      }
      while (pos < capacity) {
        if (!data(2 * pos).eq(null)) {
          return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
        }
        pos += 1
      }
      null
    }

    override def hasNext: Boolean = nextValue() != null

    override def next(): (K, V) = {
      val value = nextValue()
      if (value == null) {
        throw new NoSuchElementException("End of iterator")
      }
      pos += 1
      value
    }
  }

  override def size: Int = curSize

  /** Increase table size by 1, rehashing if necessary */
  private def incrementSize() {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   * We use the Murmur Hash 3 finalization step that's also used in fastutil.
   */
  private def rehash(h: Int): Int = {
    it.unimi.dsi.fastutil.HashCommon.murmurHash3(h)
  }

  /** Double the table's size and re-hash everything */
  private def growTable() {
    val newCapacity = capacity * 2
    if (newCapacity >= (1 << 30)) {
      // We can't make the table this big because we want an array of 2x
      // that size for our data, but array sizes are at most Int.MaxValue
      throw new Exception("Can't make capacity bigger than 2^29 elements")
    }
    val newData = new Array[AnyRef](2 * newCapacity)
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
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
    data = newData
    capacity = newCapacity
    mask = newMask
    growThreshold = LOAD_FACTOR * newCapacity
  }

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }
}
