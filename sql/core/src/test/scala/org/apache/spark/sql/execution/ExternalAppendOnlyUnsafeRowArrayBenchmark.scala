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

    // Internally, `ExternalAppendOnlyUnsafeRowArray` will create an
    // in-memory buffer of size `numSpillThreshold`. This will mimic that
    val initialSize =
      Math.min(
        ExternalAppendOnlyUnsafeRowArray.DefaultInitialSizeOfInMemoryBuffer,
        numSpillThreshold)

    benchmark.addCase("ArrayBuffer") { _: Int =>
      var sum = 0L
      for (_ <- 0L until iterations) {
        val array = new ArrayBuffer[UnsafeRow](initialSize)

        // Internally, `ExternalAppendOnlyUnsafeRowArray` will create a
        // copy of the row. This will mimic that
        rows.foreach(x => array += x.copy())

        var i = 0
        val n = array.length
        while (i < n) {
          sum = sum + array(i).getLong(0)
          i += 1
        }
        array.clear()
      }
    }

    benchmark.addCase("ExternalAppendOnlyUnsafeRowArray") { _: Int =>
      var sum = 0L
      for (_ <- 0L until iterations) {
        val array = new ExternalAppendOnlyUnsafeRowArray(numSpillThreshold)
        rows.foreach(x => array.add(x))

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

    // ========================================================================================= //
    // WITHOUT SPILL
    // ========================================================================================= //

    var spillThreshold = 100 * 1000

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 1000 rows:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer                                   7821 / 7941         33.5          29.8       1.0X
    ExternalAppendOnlyUnsafeRowArray              8798 / 8819         29.8          33.6       0.9X
    */
    test(spillThreshold, 1000, 1 << 18)

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 30000 rows:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer                                 19200 / 19206         25.6          39.1       1.0X
    ExternalAppendOnlyUnsafeRowArray            19558 / 19562         25.1          39.8       1.0X
    */
    test(spillThreshold, 30 * 1000, 1 << 14)

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 100000 rows:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer                                   5949 / 6028         17.2          58.1       1.0X
    ExternalAppendOnlyUnsafeRowArray              6078 / 6138         16.8          59.4       1.0X
    */
    test(spillThreshold, 100 * 1000, 1 << 10)

    // ========================================================================================= //
    // WITH SPILL
    // ========================================================================================= //

    spillThreshold = 100

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 1000 rows:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer                                      8 /    9         31.8          31.4       1.0X
    ExternalAppendOnlyUnsafeRowArray              2218 / 2247          0.1        8663.2       0.0X
    */
    test(spillThreshold, 1000, 1 << 8)

    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Array with 10000 rows:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    ArrayBuffer                                      5 /    6         30.5          32.8       1.0X
    ExternalAppendOnlyUnsafeRowArray              2498 / 2585          0.1       15615.3       0.0X
    */
    test(spillThreshold, 10 * 1000, 1 << 4)
  }
}
