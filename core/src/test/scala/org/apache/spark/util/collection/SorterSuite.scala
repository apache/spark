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

import java.lang.{Float => JFloat}
import java.util.{Arrays, Comparator}

import org.scalatest.FunSuite

import org.apache.spark.util.random.XORShiftRandom

class SorterSuite extends FunSuite {

  test("equivalent to Arrays.sort") {
    val rand = new XORShiftRandom(123)
    val data0 = Array.tabulate[Int](10000) { i => rand.nextInt() }
    val data1 = data0.clone()

    Arrays.sort(data0)
    new Sorter(new IntArraySortDataFormat).sort(data1, 0, data1.length, Ordering.Int)

    data0.zip(data1).foreach { case (x, y) => assert(x === y) }
  }

  test("KVArraySorter") {
    val rand = new XORShiftRandom(456)

    // Construct an array of keys (to Java sort) and an array where the keys and values
    // alternate. Keys are random doubles, values are ordinals from 0 to length.
    val keys = Array.tabulate[Double](5000) { i => rand.nextDouble() }
    val keyValueArray = Array.tabulate[Number](10000) { i =>
      if (i % 2 == 0) keys(i / 2) else new Integer(i / 2)
    }

    // Map from generated keys to values, to verify correctness later
    val kvMap =
      keyValueArray.grouped(2).map { case Array(k, v) => k.doubleValue() -> v.intValue() }.toMap

    Arrays.sort(keys)
    new Sorter(new KVArraySortDataFormat[Double, Number])
      .sort(keyValueArray, 0, keys.length, Ordering.Double)

    keys.zipWithIndex.foreach { case (k, i) =>
      assert(k === keyValueArray(2 * i))
      assert(kvMap(k) === keyValueArray(2 * i + 1))
    }
  }

  /**
   * This provides a simple benchmark for comparing the Sorter with Java internal sorting.
   * Ideally these would be executed one at a time, each in their own JVM, so their listing
   * here is mainly to have the code.
   *
   * The goal of this code is to sort an array of key-value pairs, where the array physically
   * has the keys and values alternating. The basic Java sorts work only on the keys, so the
   * real Java solution is to make Tuple2s to store the keys and values and sort an array of
   * those, while the Sorter approach can work directly on the input data format.
   *
   * Note that the Java implementation varies tremendously between Java 6 and Java 7, when
   * the Java sort changed from merge sort to Timsort.
   */
  ignore("Sorter benchmark") {

    /** Runs an experiment several times. */
    def runExperiment(name: String)(f: => Unit): Unit = {
      val firstTry = org.apache.spark.util.Utils.timeIt(1)(f)
      System.gc()

      var i = 0
      var next10: Long = 0
      while (i < 10) {
        val time = org.apache.spark.util.Utils.timeIt(1)(f)
        next10 += time
        println(s"$name: Took $time ms")
        i += 1
      }

      println(s"$name: ($firstTry ms first try, ${next10 / 10} ms average)")
    }

    val numElements = 25000000 // 25 mil
    val rand = new XORShiftRandom(123)

    val keys = Array.tabulate[JFloat](numElements) { i =>
      new JFloat(rand.nextFloat())
    }

    // Test our key-value pairs where each element is a Tuple2[Float, Integer)
    val kvTupleArray = Array.tabulate[AnyRef](numElements) { i =>
      (keys(i / 2): Float, i / 2: Int)
    }
    runExperiment("Tuple-sort using Arrays.sort()") {
      Arrays.sort(kvTupleArray, new Comparator[AnyRef] {
        override def compare(x: AnyRef, y: AnyRef): Int =
          Ordering.Float.compare(x.asInstanceOf[(Float, _)]._1, y.asInstanceOf[(Float, _)]._1)
      })
    }

    // Test our Sorter where each element alternates between Float and Integer, non-primitive
    val keyValueArray = Array.tabulate[AnyRef](numElements * 2) { i =>
      if (i % 2 == 0) keys(i / 2) else new Integer(i / 2)
    }
    val sorter = new Sorter(new KVArraySortDataFormat[JFloat, AnyRef])
    runExperiment("KV-sort using Sorter") {
      sorter.sort(keyValueArray, 0, keys.length, new Comparator[JFloat] {
        override def compare(x: JFloat, y: JFloat): Int = Ordering.Float.compare(x, y)
      })
    }

    // Test non-primitive sort on float array
    runExperiment("Java Arrays.sort()") {
      Arrays.sort(keys, new Comparator[JFloat] {
        override def compare(x: JFloat, y: JFloat): Int = Ordering.Float.compare(x, y)
      })
    }

    // Test primitive sort on float array
    val primitiveKeys = Array.tabulate[Float](numElements) { i => rand.nextFloat() }
    runExperiment("Java Arrays.sort() on primitive keys") {
      Arrays.sort(primitiveKeys)
    }
  }
}


/** Format to sort a simple Array[Int]. Could be easily generified and specialized. */
class IntArraySortDataFormat extends SortDataFormat[Int, Array[Int]] {
  override protected def getKey(data: Array[Int], pos: Int): Int = {
    data(pos)
  }

  override protected def swap(data: Array[Int], pos0: Int, pos1: Int): Unit = {
    val tmp = data(pos0)
    data(pos0) = data(pos1)
    data(pos1) = tmp
  }

  override protected def copyElement(src: Array[Int], srcPos: Int, dst: Array[Int], dstPos: Int) {
    dst(dstPos) = src(srcPos)
  }

  /** Copy a range of elements starting at src(srcPos) to dest, starting at destPos. */
  override protected def copyRange(src: Array[Int], srcPos: Int,
                                   dst: Array[Int], dstPos: Int, length: Int) {
    System.arraycopy(src, srcPos, dst, dstPos, length)
  }

  /** Allocates a new structure that can hold up to 'length' elements. */
  override protected def allocate(length: Int): Array[Int] = {
    new Array[Int](length)
  }
}
