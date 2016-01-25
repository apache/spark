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

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class BloomFilterSuite extends FunSuite { // scalastyle:ignore funsuite

  def testAccuracy[T: ClassTag](typeName: String, numItems: Int)(itemGen: Random => T): Unit = {
    test(s"accuracy - $typeName") {
      // use a fixed seed to make the test predictable.
      val r = new Random(37)
      val fpp = 0.05
      val numInsertion = numItems / 10

      val allItems = Array.fill(numItems)(itemGen(r))

      val filter = BloomFilter.create(numInsertion, fpp)

      // insert first `numInsertion` items.
      var i = 0
      while (i < numInsertion) {
        filter.put(allItems(i))
        i += 1
      }

      i = 0
      while (i < numInsertion) {
        // false negative is not allowed.
        assert(filter.mightContain(allItems(i)))
        i += 1
      }

      // The number of inserted items doesn't exceed `expectedNumItems`, so the `expectedFpp`
      // should not be significantly higher than the one we passed in to create this bloom filter.
      assert(filter.expectedFpp() - fpp < 0.001)

      var errorCount = 0
      while (i < numItems) {
        if (filter.mightContain(allItems(i))) errorCount += 1
        i += 1
      }

      // Also check the actual fpp is not significantly higher than we expected.
      val actualFpp = errorCount.toDouble / (numItems - numInsertion)
      // Skip error count that is too small.
      assert(errorCount < 50 || actualFpp - fpp < 0.001)
    }
  }

  def testMergeInPlace[T: ClassTag](typeName: String, numItems: Int)(itemGen: Random => T): Unit = {
    test(s"mergeInPlace - $typeName") {
      // use a fixed seed to make the test predictable.
      val r = new Random(37)

      val items1 = Array.fill(numItems / 2)(itemGen(r))
      val items2 = Array.fill(numItems / 2)(itemGen(r))

      val filter1 = BloomFilter.create(numItems)
      items1.foreach(filter1.put)

      val filter2 = BloomFilter.create(numItems)
      items2.foreach(filter2.put)

      filter1.mergeInPlace(filter2)

      // After merge, `filter1` has `numItems` items which doesn't exceed `expectedNumItems`, so the
      // `expectedFpp` should not be significantly higher than the default one: 3%
      // Skip byte type as it has too little distinct values.
      assert(typeName == "Byte" || 0.03 - filter1.expectedFpp() < 0.001)

      items1.foreach(i => assert(filter1.mightContain(i)))
      items2.foreach(i => assert(filter1.mightContain(i)))
    }
  }

  def testItemType[T: ClassTag](typeName: String, numItems: Int)(itemGen: Random => T): Unit = {
    testAccuracy[T](typeName, numItems)(itemGen)
    testMergeInPlace[T](typeName, numItems)(itemGen)
  }

  testItemType[Byte]("Byte", 200) { _.nextInt().toByte }

  testItemType[Short]("Short", 1000) { _.nextInt().toShort }

  testItemType[Int]("Int", 100000) { _.nextInt() }

  testItemType[Long]("Long", 100000) { _.nextLong() }

  testItemType[String]("String", 100000) { r => r.nextString(r.nextInt(512)) }
}
