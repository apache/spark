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

import org.apache.spark.SparkFunSuite

class BitSetSuite extends SparkFunSuite {

  test("basic set and get") {
    val setBits = Seq(0, 9, 1, 10, 90, 96)
    val bitset = new BitSet(100)

    for (i <- 0 until 100) {
      assert(!bitset.get(i))
    }

    setBits.foreach(i => bitset.set(i))

    for (i <- 0 until 100) {
      if (setBits.contains(i)) {
        assert(bitset.get(i))
      } else {
        assert(!bitset.get(i))
      }
    }
    assert(bitset.cardinality() === setBits.size)
  }

  test("100% full bit set") {
    val bitset = new BitSet(10000)
    for (i <- 0 until 10000) {
      assert(!bitset.get(i))
      bitset.set(i)
    }
    for (i <- 0 until 10000) {
      assert(bitset.get(i))
    }
    assert(bitset.cardinality() === 10000)
  }

  test("nextSetBit") {
    val setBits = Seq(0, 9, 1, 10, 90, 96)
    val bitset = new BitSet(100)
    setBits.foreach(i => bitset.set(i))

    assert(bitset.nextSetBit(0) === 0)
    assert(bitset.nextSetBit(1) === 1)
    assert(bitset.nextSetBit(2) === 9)
    assert(bitset.nextSetBit(9) === 9)
    assert(bitset.nextSetBit(10) === 10)
    assert(bitset.nextSetBit(11) === 90)
    assert(bitset.nextSetBit(80) === 90)
    assert(bitset.nextSetBit(91) === 96)
    assert(bitset.nextSetBit(96) === 96)
    assert(bitset.nextSetBit(97) === -1)
  }

  test( "xor len(bitsetX) < len(bitsetY)" ) {
    val setBitsX = Seq( 0, 2, 3, 37, 41 )
    val setBitsY = Seq( 0, 1, 3, 37, 38, 41, 85)
    val bitsetX = new BitSet(60)
    setBitsX.foreach( i => bitsetX.set(i))
    val bitsetY = new BitSet(100)
    setBitsY.foreach( i => bitsetY.set(i))

    val bitsetXor = bitsetX ^ bitsetY

    assert(bitsetXor.nextSetBit(0) === 1)
    assert(bitsetXor.nextSetBit(1) === 1)
    assert(bitsetXor.nextSetBit(2) === 2)
    assert(bitsetXor.nextSetBit(3) === 38)
    assert(bitsetXor.nextSetBit(38) === 38)
    assert(bitsetXor.nextSetBit(39) === 85)
    assert(bitsetXor.nextSetBit(42) === 85)
    assert(bitsetXor.nextSetBit(85) === 85)
    assert(bitsetXor.nextSetBit(86) === -1)

  }

  test( "xor len(bitsetX) > len(bitsetY)" ) {
    val setBitsX = Seq( 0, 1, 3, 37, 38, 41, 85)
    val setBitsY = Seq( 0, 2, 3, 37, 41)
    val bitsetX = new BitSet(100)
    setBitsX.foreach( i => bitsetX.set(i))
    val bitsetY = new BitSet(60)
    setBitsY.foreach( i => bitsetY.set(i))

    val bitsetXor = bitsetX ^ bitsetY

    assert(bitsetXor.nextSetBit(0) === 1)
    assert(bitsetXor.nextSetBit(1) === 1)
    assert(bitsetXor.nextSetBit(2) === 2)
    assert(bitsetXor.nextSetBit(3) === 38)
    assert(bitsetXor.nextSetBit(38) === 38)
    assert(bitsetXor.nextSetBit(39) === 85)
    assert(bitsetXor.nextSetBit(42) === 85)
    assert(bitsetXor.nextSetBit(85) === 85)
    assert(bitsetXor.nextSetBit(86) === -1)

  }

  test( "andNot len(bitsetX) < len(bitsetY)" ) {
    val setBitsX = Seq( 0, 2, 3, 37, 41, 48 )
    val setBitsY = Seq( 0, 1, 3, 37, 38, 41, 85)
    val bitsetX = new BitSet(60)
    setBitsX.foreach( i => bitsetX.set(i))
    val bitsetY = new BitSet(100)
    setBitsY.foreach( i => bitsetY.set(i))

    val bitsetDiff = bitsetX.andNot( bitsetY )

    assert(bitsetDiff.nextSetBit(0) === 2)
    assert(bitsetDiff.nextSetBit(1) === 2)
    assert(bitsetDiff.nextSetBit(2) === 2)
    assert(bitsetDiff.nextSetBit(3) === 48)
    assert(bitsetDiff.nextSetBit(48) === 48)
    assert(bitsetDiff.nextSetBit(49) === -1)
    assert(bitsetDiff.nextSetBit(65) === -1)
  }

  test( "andNot len(bitsetX) > len(bitsetY)" ) {
    val setBitsX = Seq( 0, 1, 3, 37, 38, 41, 85)
    val setBitsY = Seq( 0, 2, 3, 37, 41, 48 )
    val bitsetX = new BitSet(100)
    setBitsX.foreach( i => bitsetX.set(i))
    val bitsetY = new BitSet(60)
    setBitsY.foreach( i => bitsetY.set(i))

    val bitsetDiff = bitsetX.andNot( bitsetY )

    assert(bitsetDiff.nextSetBit(0) === 1)
    assert(bitsetDiff.nextSetBit(1) === 1)
    assert(bitsetDiff.nextSetBit(2) === 38)
    assert(bitsetDiff.nextSetBit(3) === 38)
    assert(bitsetDiff.nextSetBit(38) === 38)
    assert(bitsetDiff.nextSetBit(39) === 85)
    assert(bitsetDiff.nextSetBit(85) === 85)
    assert(bitsetDiff.nextSetBit(86) === -1)
  }

  test( "[gs]etUntil" ) {
    val bitSet = new BitSet(100)

    bitSet.setUntil(bitSet.capacity)

    (0 until bitSet.capacity).foreach { i =>
      assert(bitSet.get(i))
    }

    bitSet.clearUntil(bitSet.capacity)

    (0 until bitSet.capacity).foreach { i =>
      assert(!bitSet.get(i))
    }

    val setUntil = bitSet.capacity / 2
    bitSet.setUntil(setUntil)

    val clearUntil = setUntil / 2
    bitSet.clearUntil(clearUntil)

    (0 until clearUntil).foreach { i =>
      assert(!bitSet.get(i))
    }
    (clearUntil until setUntil).foreach { i =>
      assert(bitSet.get(i))
    }
    (setUntil until bitSet.capacity).foreach { i =>
      assert(!bitSet.get(i))
    }
  }

  test("empty intersect non-empty") {
    val emptyBitSet = new BitSet(numBits = 10)
    val bitSet = new BitSet(numBits = 10)

    val setBits = Seq(0, 10, 20, 30)
    setBits.foreach(bitSet.set)

    assert(!bitSet.intersects(emptyBitSet))
  }

  test("intersect bitsets of different size") {
    val oneBitSet = new BitSet(numBits = 1000)
    val otherBitSet = new BitSet(numBits = 20)

    Seq(0, 10, 20, 30).foreach(otherBitSet.set)
    Seq(0, 10).foreach(oneBitSet.set)
    val e = intercept[java.lang.AssertionError] {
      oneBitSet.intersects(otherBitSet)
    }
    assert(e.getMessage.contains("assertion failed"))
  }

  test("intersect bitsets ") {
    val oneBitSet = new BitSet(numBits = 1000)
    val otherBitSet = new BitSet(numBits = 1000)
    Seq(0, 10, 20, 900).foreach(otherBitSet.set)
    Seq(1, 12, 22, 900).foreach(oneBitSet.set)
    assert(oneBitSet.intersects(otherBitSet))
  }
}
