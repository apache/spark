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

import java.util.{Arrays, Comparator, Random}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.Benchmark
import org.apache.spark.util.collection.Sorter
import org.apache.spark.util.collection.unsafe.sort._

/**
 * Benchmark to measure performance for accessing primitive arrays
 * To run this:
 *  1. Replace ignore(...) with test(...)
 *  2. build/sbt "sql/test-only *benchmark.PrimitiveArrayBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class PrimitiveArrayBenchmark extends BenchmarkBase {

  test("Read array in Dataset") {
    import sparkSession.implicits._

    val iters = 5
    val n = 1024 * 1024
    val rows = 15

    val benchmark = new Benchmark("Read primnitive array", n)

    val rand = new Random(511)
    val intDS = sparkSession.sparkContext.parallelize(0 until rows, 1)
      .map(i => Array.tabulate(n)(i => i)).toDS()
    intDS.count() // force to create ds
    val lastElement = n - 1
    val randElement = rand.nextInt(lastElement)

    benchmark.addCase(s"Read int array in Dataset", numIters = iters)(iter => {
      val idx0 = randElement
      val idx1 = lastElement
      intDS.map(a => a(0) + a(idx0) + a(idx1)).collect
    })

    val doubleDS = sparkSession.sparkContext.parallelize(0 until rows, 1)
      .map(i => Array.tabulate(n)(i => i.toDouble)).toDS()
    doubleDS.count() // force to create ds

    benchmark.addCase(s"Read double array in Dataset", numIters = iters)(iter => {
      val idx0 = randElement
      val idx1 = lastElement
      doubleDS.map(a => a(0) + a(idx0) + a(idx1)).collect
    })

    benchmark.run()
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.10.4
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz

    Read primnitive array:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Read int array in Dataset                      400 /  492          2.6         381.5       1.0X
    Read double array in Dataset                   788 /  870          1.3         751.4       0.5X
    */
  }
}
