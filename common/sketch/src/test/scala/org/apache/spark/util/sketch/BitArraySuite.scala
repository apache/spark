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

package org.apache.spark.util.sketch

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class BitArraySuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("error case when create BitArray") {
    intercept[IllegalArgumentException](new BitArray(0))
    intercept[IllegalArgumentException](new BitArray(64L * Integer.MAX_VALUE + 1))
  }

  test("bitSize") {
    assert(new BitArray(64).bitSize() == 64)
    // BitArray is word-aligned, so 65~128 bits need 2 long to store, which is 128 bits.
    assert(new BitArray(65).bitSize() == 128)
    assert(new BitArray(127).bitSize() == 128)
    assert(new BitArray(128).bitSize() == 128)
  }

  test("set") {
    val bitArray = new BitArray(64)
    assert(bitArray.set(1))
    // Only returns true if the bit changed.
    assert(!bitArray.set(1))
    assert(bitArray.set(2))
  }

  test("normal operation") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)

    val bitArray = new BitArray(320)
    val indexes = (1 to 100).map(_ => r.nextInt(320).toLong).distinct

    indexes.foreach(bitArray.set)
    indexes.foreach(i => assert(bitArray.get(i)))
    assert(bitArray.cardinality() == indexes.length)
  }

  test("merge") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)

    val bitArray1 = new BitArray(64 * 6)
    val bitArray2 = new BitArray(64 * 6)

    val indexes1 = (1 to 100).map(_ => r.nextInt(64 * 6).toLong).distinct
    val indexes2 = (1 to 100).map(_ => r.nextInt(64 * 6).toLong).distinct

    indexes1.foreach(bitArray1.set)
    indexes2.foreach(bitArray2.set)

    bitArray1.putAll(bitArray2)
    indexes1.foreach(i => assert(bitArray1.get(i)))
    indexes2.foreach(i => assert(bitArray1.get(i)))
    assert(bitArray1.cardinality() == (indexes1 ++ indexes2).distinct.length)
  }
}
