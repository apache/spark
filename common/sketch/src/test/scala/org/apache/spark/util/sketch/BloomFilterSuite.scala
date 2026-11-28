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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class BloomFilterSuite extends AnyFunSuite { // scalastyle:ignore funsuite
  private final val EPSILON = 0.01

  // Serializes and deserializes a given `BloomFilter`, then checks whether the deserialized
  // version is equivalent to the original one.
  private def checkSerDe(filter: BloomFilter): Unit = {
    val out = new ByteArrayOutputStream()
    filter.writeTo(out)
    out.close()

    val deserialized = BloomFilter.readFrom(out.toByteArray)

    assert(filter == deserialized)
  }

  // A filter of `version` for `numItems` items at a false positive probability of `fpp`.
  private def create(
      version: BloomFilter.Version,
      numItems: Long,
      fpp: Double = BloomFilter.DEFAULT_FPP): BloomFilter =
    BloomFilter.create(version, numItems, BloomFilter.optimalNumOfBits(numItems, fpp), 0)

  def testAccuracy[T: ClassTag](version: BloomFilter.Version, typeName: String, numItems: Int)(
      itemGen: Random => T): Unit = {
    test(s"accuracy - $typeName ($version)") {
      // use a fixed seed to make the test predictable.
      val r = new Random(37)
      val fpp = 0.05
      val numInsertion = numItems / 10

      // using a Set to avoid duplicates,
      // inserting twice as many random values as used, to compensate for lost dupes
      val allItems = Set.fill(2 * numItems)(itemGen(r)).take(numItems)

      val filter = create(version, numInsertion, fpp)

      // insert first `numInsertion` items.
      allItems.take(numInsertion).foreach(filter.put)

      // false negative is not allowed.
      assert(allItems.take(numInsertion).forall(filter.mightContain))

      // The number of inserted items doesn't exceed `expectedNumItems`, so the `expectedFpp`
      // should not be significantly higher than the one we passed in to create this bloom filter.
      assert(filter.expectedFpp() - fpp < EPSILON)

      val errorCount = allItems.drop(numInsertion).count(filter.mightContain)

      // Also check the actual fpp is not significantly higher than we expected.
      val actualFpp = errorCount.toDouble / (numItems - numInsertion)
      assert(actualFpp - fpp < EPSILON)

      checkSerDe(filter)
    }
  }

  def testMergeInPlace[T: ClassTag](version: BloomFilter.Version, typeName: String, numItems: Int)(
      itemGen: Random => T): Unit = {
    test(s"mergeInPlace - $typeName ($version)") {
      // use a fixed seed to make the test predictable.
      val r = new Random(37)

      val items1 = Array.fill(numItems / 2)(itemGen(r))
      val items2 = Array.fill(numItems / 2)(itemGen(r))

      val filter1 = create(version, numItems)
      items1.foreach(filter1.put)

      val filter2 = create(version, numItems)
      items2.foreach(filter2.put)

      filter1.mergeInPlace(filter2)

      // After merge, `filter1` has `numItems` items which doesn't exceed `expectedNumItems`, so the
      // `expectedFpp` should not be significantly higher than the default one.
      assert(filter1.expectedFpp() - BloomFilter.DEFAULT_FPP < EPSILON)

      items1.foreach(i => assert(filter1.mightContain(i)))
      items2.foreach(i => assert(filter1.mightContain(i)))

      checkSerDe(filter1)
    }
  }

  def testIntersectInPlace[T: ClassTag]
  (version: BloomFilter.Version, typeName: String, numItems: Int)(itemGen: Random => T): Unit = {
    test(s"intersectInPlace - $typeName ($version)") {
      // use a fixed seed to make the test predictable.
      val r = new Random(37)

      val items1 = Array.fill(numItems / 2)(itemGen(r))
      val items2 = Array.fill(numItems / 2)(itemGen(r))

      val filter1 = create(version, numItems / 2)
      items1.foreach(filter1.put)

      val filter2 = create(version, numItems / 2)
      items2.foreach(filter2.put)

      filter1.intersectInPlace(filter2)

      val common_items = items1.intersect(items2)
      common_items.foreach(i => assert(filter1.mightContain(i)))

      // After intersect, `filter1` still has `numItems/2` items
      // which doesn't exceed `expectedNumItems`,
      // so the `expectedFpp` should not be higher than the default one.
      assert(filter1.expectedFpp() - BloomFilter.DEFAULT_FPP < EPSILON)

      checkSerDe(filter1)
    }
  }

  def testItemType[T: ClassTag](typeName: String, numItems: Int)(itemGen: Random => T): Unit = {
    BloomFilter.Version.values().foreach { version =>
      testAccuracy[T](version, typeName, numItems)(itemGen)
      testMergeInPlace[T](version, typeName, numItems)(itemGen)
      testIntersectInPlace[T](version, typeName, numItems)(itemGen)
    }
  }

  testItemType[Byte]("Byte", 160) { _.nextInt().toByte }

  testItemType[Short]("Short", 1000) { _.nextInt().toShort }

  testItemType[Int]("Int", 100000) { _.nextInt() }

  testItemType[Long]("Long", 100000) { _.nextLong() }

  testItemType[String]("String", 100000) { r => r.nextString(r.nextInt(512)) }

  test("incompatible merge") {
    intercept[IncompatibleMergeException] {
      BloomFilter.create(1000).mergeInPlace(null)
    }

    intercept[IncompatibleMergeException] {
      val filter1 = BloomFilter.create(1000, 6400)
      val filter2 = BloomFilter.create(1000, 3200)
      filter1.mergeInPlace(filter2)
    }

    intercept[IncompatibleMergeException] {
      val filter1 = BloomFilter.create(1000, 6400)
      val filter2 = BloomFilter.create(2000, 6400)
      filter1.mergeInPlace(filter2)
    }

    intercept[IncompatibleMergeException] {
      val filter1 = BloomFilter.create(BloomFilter.Version.V1, 1000L, 6400L, 0)
      val filter2 = BloomFilter.create(BloomFilter.Version.V2, 1000L, 6400L, 0)
      filter1.mergeInPlace(filter2)
    }

    // V3 extends V2, but sets other bits for the same items.
    Seq(
      BloomFilter.Version.V2 -> BloomFilter.Version.V3,
      BloomFilter.Version.V3 -> BloomFilter.Version.V2).foreach { case (v1, v2) =>
      val filter1 = BloomFilter.create(v1, 1000L, 6400L, 0)
      val filter2 = BloomFilter.create(v2, 1000L, 6400L, 0)
      assert(!filter1.isCompatible(filter2))
      assert(filter1 != filter2)
      intercept[IncompatibleMergeException] {
        filter1.mergeInPlace(filter2)
      }
    }
  }

  test("V3 is the default version") {
    assert(BloomFilter.create(1000).isInstanceOf[BloomFilterImplV3])
    assert(BloomFilter.create(1000, 0.01).isInstanceOf[BloomFilterImplV3])
    assert(BloomFilter.create(1000, 6400L).isInstanceOf[BloomFilterImplV3])
    assert(BloomFilter.create(1000, 6400L, 7).isInstanceOf[BloomFilterImplV3])
  }

  test("each version reads back as itself with Java serialization") {
    BloomFilter.Version.values().foreach { version =>
      val filter = create(version, 100, 0.01)
      (0L until 100L).foreach(filter.putLong)
      val out = new ByteArrayOutputStream()
      val objectOut = new ObjectOutputStream(out)
      objectOut.writeObject(filter)
      objectOut.close()
      val deserialized = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray))
        .readObject()
      assert(deserialized == filter, version)
    }
  }

  test("V3 keeps its bit pattern") {
    // Filters written before must keep reading the same, so a change to the bit positions of V3
    // needs a new version.
    val filter = BloomFilter.create(BloomFilter.Version.V3, 10, 192, 0)
    (1L to 10L).foreach(filter.putLong)
    val out = new ByteArrayOutputStream()
    filter.writeTo(out)
    assert(out.toByteArray.map("%02X".format(_)).mkString ==
      "000000030000000D0000000000000003BCA1BEA6303CE97B9E04A02633051A3A396061BBE7689C56")
  }

  test("V2 and V3 keep their serialVersionUID") {
    // V2 keeps the one of earlier versions.
    assert(ObjectStreamClass.lookup(classOf[BloomFilterImplV2]).getSerialVersionUID ==
      -7609404176825362195L)
    assert(ObjectStreamClass.lookup(classOf[BloomFilterImplV3]).getSerialVersionUID ==
      9151286223420309825L)
  }

  test("V3 maps a 64-bit value below the number of bits as the unsigned product does") {
    // Up to the largest bit array, which needs all 64 bits of the product.
    val bitSizes = Seq(64L, 448L, 1L << 32, (1L << 32) + 64, 64L * Int.MaxValue)
    val r = new Random(37)
    val values = Seq(0L, 1L, -1L, Long.MinValue, Long.MaxValue) ++ Seq.fill(1000)(r.nextLong())
    val twoTo64 = BigInt(1) << 64
    for (bitSize <- bitSizes; z <- values) {
      val unsigned = if (z < 0) BigInt(z) + twoTo64 else BigInt(z)
      assert(BloomFilterImplV3.reduce(z, bitSize) == ((unsigned * bitSize) >> 64).toLong,
        s"$z below $bitSize")
    }
  }

  test("V3 keeps the expected false positive probability of small filters") {
    // V2 is 5 to 43 times over the expected fpp for these filters.
    val r = new Random(37)
    Seq((16, 1e-3), (16, 1e-4), (100, 1e-4)).foreach { case (numItems, fpp) =>
      val numFilters = 200
      val probesPerFilter = 10000
      var falsePositives = 0L
      (0 until numFilters).foreach { _ =>
        val filter = create(BloomFilter.Version.V3, numItems, fpp)
        (0 until numItems).foreach(_ => filter.putLong(r.nextLong()))
        // Random probes are not among the items, unless by a chance far below `fpp`.
        var i = 0
        while (i < probesPerFilter) {
          if (filter.mightContainLong(r.nextLong())) falsePositives += 1
          i += 1
        }
      }
      val actualFpp = falsePositives.toDouble / numFilters / probesPerFilter
      assert(actualFpp < 1.5 * fpp, s"$numItems items at $fpp: $actualFpp")
    }
  }
}
