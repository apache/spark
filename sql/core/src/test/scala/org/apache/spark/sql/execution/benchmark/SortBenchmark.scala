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

package org.apache.spark.sql.execution.benchmark

import java.util.Arrays

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.collection.Sorter
import org.apache.spark.util.collection.unsafe.sort._
import org.apache.spark.util.random.XORShiftRandom

/**
 * Benchmark to measure performance for aggregate primitives.
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/<this class>-results.txt".
 * }}}
 */
object SortBenchmark extends BenchmarkBase {

  private def referenceKeyPrefixSort(buf: LongArray, lo: Int, hi: Int,
      refCmp: PrefixComparator): Unit = {
    val sortBuffer = new LongArray(MemoryBlock.fromLongArray(new Array[Long](buf.size().toInt)))
    new Sorter(new UnsafeSortDataFormat(sortBuffer)).sort(buf, lo, hi,
      (r1: RecordPointerAndKeyPrefix, r2: RecordPointerAndKeyPrefix) =>
        refCmp.compare(r1.keyPrefix, r2.keyPrefix))
  }

  private def generateKeyPrefixTestData(size: Int, rand: => Long): (LongArray, LongArray) = {
    val ref = Array.tabulate[Long](size * 2) { i => rand }
    val extended = ref ++ Array.ofDim[Long](size * 2)
    (new LongArray(MemoryBlock.fromLongArray(ref)),
      new LongArray(MemoryBlock.fromLongArray(extended)))
  }

  def sortBenchmark(): Unit = {
    val size = 25000000
    val rand = new XORShiftRandom(123)
    val benchmark = new Benchmark("radix sort " + size, size, output = output)
    benchmark.addTimerCase("reference TimSort key prefix array") { timer =>
      val array = Array.tabulate[Long](size * 2) { i => rand.nextLong }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      referenceKeyPrefixSort(buf, 0, size, PrefixComparators.BINARY)
      timer.stopTiming()
    }
    benchmark.addTimerCase("reference Arrays.sort") { timer =>
      val ref = Array.tabulate[Long](size) { i => rand.nextLong }
      timer.startTiming()
      Arrays.sort(ref)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort one byte") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong & 0xff
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort two bytes") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong & 0xffff
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort eight bytes") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort key prefix array") { timer =>
      val (_, buf2) = generateKeyPrefixTestData(size, rand.nextLong)
      timer.startTiming()
      RadixSort.sortKeyPrefixArray(buf2, 0, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("radix sort") {
      sortBenchmark()
    }
  }
}
