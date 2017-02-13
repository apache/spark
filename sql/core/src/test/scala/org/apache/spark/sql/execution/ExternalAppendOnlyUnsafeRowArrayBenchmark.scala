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

package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.Benchmark

object ExternalAppendOnlyUnsafeRowArrayBenchmark {

  def test(numSpillThreshold: Int, numRows: Int, iterations: Int): Unit = {
    val random = new java.util.Random()
    val rows = (1 to numRows).map(_ => {
      val row = new UnsafeRow(1)
      row.pointTo(new Array[Byte](64), 16)
      row.setLong(0, random.nextLong())
      row
    })

    val benchmark = new Benchmark(s"Array with $numRows rows", iterations * numRows)

    benchmark.addCase("ArrayBuffer version") { _: Int =>
      var sum = 0L
      for (_ <- 0L until iterations) {
        val array = new ArrayBuffer[UnsafeRow]()
        rows.foreach(array.append(_))

        var i = 0
        val n = array.length
        while (i < n) {
          if (i < n) {
            sum = sum + array(i).getLong(0)
            i += 1
          }
        }
        array.clear()
      }
    }

    benchmark.addCase("ExternalAppendOnlyUnsafeRowArray version") { _: Int =>
      var sum = 0L
      for (_ <- 0L until iterations) {
        val array = new ExternalAppendOnlyUnsafeRowArray(numSpillThreshold)
        rows.foreach(array.add)

        val iterator = array.generateIterator()
        while (iterator.hasNext) {
          sum = sum + iterator.next().getLong(0)
        }

        array.clear()
      }
    }

    val conf = new SparkConf(false)
    // Make the Java serializer write a reset instruction (TC_RESET) after each object to test
    // for a bug we had with bytes written past the last object in a batch (SPARK-2792)
    conf.set("spark.serializer.objectStreamReset", "1")
    conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext("local", "test", conf)
    val taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)
    benchmark.run()
    sc.stop()
  }

  def main(args: Array[String]): Unit = {

    // ----------------------------------------------------------------------------------------- //
    // WITHOUT SPILL
    // ----------------------------------------------------------------------------------------- //

    var spillThreshold = 100 * 1000

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 1000 rows:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer version                           1969 / 2004         33.3          30.0       1.0X
    ExternalAppendOnlyUnsafeRowArray version      2184 / 2233         30.0          33.3       0.9X
    */
    test(spillThreshold, 1000, 1 << 16)

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 30000 rows:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer version                           2442 / 2465         50.3          19.9       1.0X
    ExternalAppendOnlyUnsafeRowArray version      5015 / 5039         24.5          40.8       0.5X
    */
    test(spillThreshold, 30 * 1000, 1 << 12)

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 100000 rows:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer version                           4492 / 4575         22.8          43.9       1.0X
    ExternalAppendOnlyUnsafeRowArray version      6654 / 6656         15.4          65.0       0.7X
    */
    test(spillThreshold, 100 * 1000, 1 << 10)

    // ----------------------------------------------------------------------------------------- //
    // WITH SPILL
    // ----------------------------------------------------------------------------------------- //

    spillThreshold = 100

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 1000 rows:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer version                              4 /    5         61.2          16.3       1.0X
    ExternalAppendOnlyUnsafeRowArray version      2331 / 2356          0.1        9106.1       0.0X
    */
    test(spillThreshold, 1000, 1 << 8)

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 10000 rows:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer version                              3 /    4         57.6          17.3       1.0X
    ExternalAppendOnlyUnsafeRowArray version      2229 / 2310          0.1       13928.8       0.0X
    */
    test(spillThreshold, 10 * 1000, 1 << 4)
  }
}
