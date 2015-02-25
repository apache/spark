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

import java.lang.{Float => JFloat, Integer => JInteger}
import java.util.{Arrays, Comparator}

import org.scalatest.FunSuite

import org.apache.spark.util.random.XORShiftRandom

class SorterSuite extends FunSuite {

  test("equivalent to Arrays.sort") {
    val rand = new XORShiftRandom(123)
    val data0 = Array.tabulate[Int](10000) { i => rand.nextInt() }
    val data1 = data0.clone()
    val data2 = data0.clone()

    Arrays.sort(data0)
    new Sorter(new IntArraySortDataFormat).sort(data1, 0, data1.length, Ordering.Int)
    new Sorter(new KeyReuseIntArraySortDataFormat)
      .sort(data2, 0, data2.length, Ordering[IntWrapper])

    assert(data0.view === data1.view)
    assert(data0.view === data2.view)
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

  /** Runs an experiment several times. */
  def runExperiment(name: String, skip: Boolean = false)(f: => Unit, prepare: () => Unit): Unit = {
    if (skip) {
      println(s"Skipped experiment $name.")
      return
    }

    val firstTry = org.apache.spark.util.Utils.timeIt(1)(f, Some(prepare))
    System.gc()

    var i = 0
    var next10: Long = 0
    while (i < 10) {
      val time = org.apache.spark.util.Utils.timeIt(1)(f, Some(prepare))
      next10 += time
      println(s"$name: Took $time ms")
      i += 1
    }

    println(s"$name: ($firstTry ms first try, ${next10 / 10} ms average)")
  }

  /**
   * This provides a simple benchmark for comparing the Sorter with Java internal sorting.
   * Ideally these would be executed one at a time, each in their own JVM, so their listing
   * here is mainly to have the code. Running multiple tests within the same JVM session would
   * prevent JIT inlining overridden methods and hence hurt the performance.
   *
   * The goal of this code is to sort an array of key-value pairs, where the array physically
   * has the keys and values alternating. The basic Java sorts work only on the keys, so the
   * real Java solution is to make Tuple2s to store the keys and values and sort an array of
   * those, while the Sorter approach can work directly on the input data format.
   *
   * Note that the Java implementation varies tremendously between Java 6 and Java 7, when
   * the Java sort changed from merge sort to TimSort.
   */
  ignore("Sorter benchmark for key-value pairs") {
    val numElements = 25000000 // 25 mil
    val rand = new XORShiftRandom(123)

    // Test our key-value pairs where each element is a Tuple2[Float, Integer].

    val kvTuples = Array.tabulate(numElements) { i =>
      (new JFloat(rand.nextFloat()), new JInteger(i))
    }

    val kvTupleArray = new Array[AnyRef](numElements)
    val prepareKvTupleArray = () => {
      System.arraycopy(kvTuples, 0, kvTupleArray, 0, numElements)
    }
    runExperiment("Tuple-sort using Arrays.sort()")({
      Arrays.sort(kvTupleArray, new Comparator[AnyRef] {
        override def compare(x: AnyRef, y: AnyRef): Int =
          x.asInstanceOf[(JFloat, _)]._1.compareTo(y.asInstanceOf[(JFloat, _)]._1)
      })
    }, prepareKvTupleArray)

    // Test our Sorter where each element alternates between Float and Integer, non-primitive

    val keyValues = {
      val data = new Array[AnyRef](numElements * 2)
      var i = 0
      while (i < numElements) {
        data(2 * i) = kvTuples(i)._1
        data(2 * i + 1) = kvTuples(i)._2
        i += 1
      }
      data
    }

    val keyValueArray = new Array[AnyRef](numElements * 2)
    val prepareKeyValueArray = () => {
      System.arraycopy(keyValues, 0, keyValueArray, 0, numElements * 2)
    }

    val sorter = new Sorter(new KVArraySortDataFormat[JFloat, AnyRef])
    runExperiment("KV-sort using Sorter")({
      sorter.sort(keyValueArray, 0, numElements, new Comparator[JFloat] {
        override def compare(x: JFloat, y: JFloat): Int = x.compareTo(y)
      })
    }, prepareKeyValueArray)
  }

  /**
   * Tests for sorting with primitive keys with/without key reuse. Java's Arrays.sort is used as
   * reference, which is expected to be faster but it can only sort a single array. Sorter can be
   * used to sort parallel arrays.
   *
   * Ideally these would be executed one at a time, each in their own JVM, so their listing
   * here is mainly to have the code. Running multiple tests within the same JVM session would
   * prevent JIT inlining overridden methods and hence hurt the performance.
   */
  ignore("Sorter benchmark for primitive int array") {
    val numElements = 25000000 // 25 mil
    val rand = new XORShiftRandom(123)

    val ints = Array.fill(numElements)(rand.nextInt())
    val intObjects = {
      val data = new Array[JInteger](numElements)
      var i = 0
      while (i < numElements) {
        data(i) = new JInteger(ints(i))
        i += 1
      }
      data
    }

    val intObjectArray = new Array[JInteger](numElements)
    val prepareIntObjectArray = () => {
      System.arraycopy(intObjects, 0, intObjectArray, 0, numElements)
    }

    runExperiment("Java Arrays.sort() on non-primitive int array")({
      Arrays.sort(intObjectArray, new Comparator[JInteger] {
        override def compare(x: JInteger, y: JInteger): Int = x.compareTo(y)
      })
    }, prepareIntObjectArray)

    val intPrimitiveArray = new Array[Int](numElements)
    val prepareIntPrimitiveArray = () => {
      System.arraycopy(ints, 0, intPrimitiveArray, 0, numElements)
    }

    runExperiment("Java Arrays.sort() on primitive int array")({
      Arrays.sort(intPrimitiveArray)
    }, prepareIntPrimitiveArray)

    val sorterWithoutKeyReuse = new Sorter(new IntArraySortDataFormat)
    runExperiment("Sorter without key reuse on primitive int array")({
      sorterWithoutKeyReuse.sort(intPrimitiveArray, 0, numElements, Ordering[Int])
    }, prepareIntPrimitiveArray)

    val sorterWithKeyReuse = new Sorter(new KeyReuseIntArraySortDataFormat)
    runExperiment("Sorter with key reuse on primitive int array")({
      sorterWithKeyReuse.sort(intPrimitiveArray, 0, numElements, Ordering[IntWrapper])
    }, prepareIntPrimitiveArray)
  }
}

abstract class AbstractIntArraySortDataFormat[K] extends SortDataFormat[K, Array[Int]] {

  override def swap(data: Array[Int], pos0: Int, pos1: Int): Unit = {
    val tmp = data(pos0)
    data(pos0) = data(pos1)
    data(pos1) = tmp
  }

  override def copyElement(src: Array[Int], srcPos: Int, dst: Array[Int], dstPos: Int) {
    dst(dstPos) = src(srcPos)
  }

  /** Copy a range of elements starting at src(srcPos) to dest, starting at destPos. */
  override def copyRange(src: Array[Int], srcPos: Int, dst: Array[Int], dstPos: Int, length: Int) {
    System.arraycopy(src, srcPos, dst, dstPos, length)
  }

  /** Allocates a new structure that can hold up to 'length' elements. */
  override def allocate(length: Int): Array[Int] = {
    new Array[Int](length)
  }
}

/** Format to sort a simple Array[Int]. Could be easily generified and specialized. */
class IntArraySortDataFormat extends AbstractIntArraySortDataFormat[Int] {

  override protected def getKey(data: Array[Int], pos: Int): Int = {
    data(pos)
  }
}

/** Wrapper of Int for key reuse. */
class IntWrapper(var key: Int = 0) extends Ordered[IntWrapper] {

  override def compare(that: IntWrapper): Int = {
    Ordering.Int.compare(key, that.key)
  }
}

/** SortDataFormat for Array[Int] with reused keys. */
class KeyReuseIntArraySortDataFormat extends AbstractIntArraySortDataFormat[IntWrapper] {

  override def newKey(): IntWrapper = {
    new IntWrapper()
  }

  override def getKey(data: Array[Int], pos: Int, reuse: IntWrapper): IntWrapper = {
    if (reuse == null) {
      new IntWrapper(data(pos))
    } else {
      reuse.key = data(pos)
      reuse
    }
  }

  override protected def getKey(data: Array[Int], pos: Int): IntWrapper = {
    getKey(data, pos, null)
  }
}
