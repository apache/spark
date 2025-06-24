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
import java.util

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.random.XORShiftRandom

/**
 * Benchmark for o.a.s.util.collection.Sorter.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/SorterBenchmark-results.txt".
 * }}}
 * */
object SorterBenchmark extends BenchmarkBase {

  def keyValuePairsSortBenchmark(): Unit = {
    val numElements = 25000000 // 25 mil
    val rand = new XORShiftRandom(123)
    val benchmark =
      new Benchmark(s"key-value pairs sort $numElements", numElements, output = output)

    // Test key-value pairs where each element is a Tuple2[Float, Integer]
    val kvTuples = Array.tabulate(numElements) { i =>
      (JFloat.valueOf(rand.nextFloat()), Integer.valueOf(i))
    }

    benchmark.addTimerCase("Tuple-sort using Arrays.sort()") { timer =>
      val kvTupleArray = new Array[AnyRef](numElements)
      System.arraycopy(kvTuples, 0, kvTupleArray, 0, numElements)
      timer.startTiming()
      util.Arrays.sort(kvTupleArray, (x: AnyRef, y: AnyRef) =>
        x.asInstanceOf[(JFloat, _)]._1.compareTo(y.asInstanceOf[(JFloat, _)]._1))
      timer.stopTiming()
    }

    // Test Sorter where each element alternates between Float and Integer, non-primitive
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

    benchmark.addTimerCase("KV-sort using Sorter") { timer =>
      val keyValueArray = new Array[AnyRef](numElements * 2)
      System.arraycopy(keyValues, 0, keyValueArray, 0, numElements * 2)
      val sorter = new Sorter(new KVArraySortDataFormat[JFloat, AnyRef])
      timer.startTiming()
      sorter.sort(keyValueArray, 0, numElements, (x: JFloat, y: JFloat) => x.compareTo(y))
      timer.stopTiming()
    }

    benchmark.run()
  }

  def primitiveIntArraySortBenchmark(): Unit = {
    val numElements = 25000000 // 25 mil
    val rand = new XORShiftRandom(123)
    val benchmark =
      new Benchmark(s"primitive int array sort $numElements", numElements, output = output)

    val ints = Array.fill(numElements)(rand.nextInt())
    val intObjects = {
      val data = new Array[Integer](numElements)
      var i = 0
      while (i < numElements) {
        data(i) = Integer.valueOf(ints(i))
        i += 1
      }
      data
    }

    benchmark.addTimerCase("Java Arrays.sort() on non-primitive int array") { timer =>
      val intObjectArray = new Array[Integer](numElements)
      System.arraycopy(intObjects, 0, intObjectArray, 0, numElements)
      timer.startTiming()
      util.Arrays.sort(intObjectArray, (x: Integer, y: Integer) => x.compareTo(y))
      timer.stopTiming()
    }

    benchmark.addTimerCase("Java Arrays.sort() on primitive int array") { timer =>
      val intPrimitiveArray = new Array[Int](numElements)
      System.arraycopy(ints, 0, intPrimitiveArray, 0, numElements)
      timer.startTiming()
      util.Arrays.sort(intPrimitiveArray)
      timer.stopTiming()
    }

    benchmark.addTimerCase("Sorter without key reuse on primitive int array") { timer =>
      val intPrimitiveArray = new Array[Int](numElements)
      System.arraycopy(ints, 0, intPrimitiveArray, 0, numElements)
      val sorterWithoutKeyReuse = new Sorter(new IntArraySortDataFormat)
      timer.startTiming()
      sorterWithoutKeyReuse.sort(intPrimitiveArray, 0, numElements, Ordering[Int])
      timer.stopTiming()
    }

    benchmark.addTimerCase("Sorter with key reuse on primitive int array") { timer =>
      val intPrimitiveArray = new Array[Int](numElements)
      System.arraycopy(ints, 0, intPrimitiveArray, 0, numElements)
      val sorterWithKeyReuse = new Sorter(new KeyReuseIntArraySortDataFormat)
      timer.startTiming()
      sorterWithKeyReuse.sort(intPrimitiveArray, 0, numElements, Ordering[IntWrapper])
      timer.stopTiming()
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("key-value pairs sort") {
      keyValuePairsSortBenchmark()
    }
    runBenchmark("primitive int array sort") {
      primitiveIntArraySortBenchmark()
    }
  }

  /** Format to sort a simple Array[Int]. Could be easily generified and specialized. */
  class IntArraySortDataFormat extends AbstractIntArraySortDataFormat[Int] {

    override protected def getKey(data: Array[Int], pos: Int): Int = {
      data(pos)
    }
  }

  abstract class AbstractIntArraySortDataFormat[K] extends SortDataFormat[K, Array[Int]] {

    override def swap(data: Array[Int], pos0: Int, pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def copyElement(src: Array[Int], srcPos: Int, dst: Array[Int], dstPos: Int): Unit = {
      dst(dstPos) = src(srcPos)
    }

    /** Copy a range of elements starting at src(srcPos) to dest, starting at destPos. */
    override def copyRange(src: Array[Int], srcPos: Int,
        dst: Array[Int], dstPos: Int, length: Int): Unit = {
      System.arraycopy(src, srcPos, dst, dstPos, length)
    }

    /** Allocates a new structure that can hold up to 'length' elements. */
    override def allocate(length: Int): Array[Int] = {
      new Array[Int](length)
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
}
