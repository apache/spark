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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.memory.{StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.Benchmark

/**
  * Benchmark to measure whole stage codegen performance.
  * To run this:
  *  build/sbt "sql/test-only *BenchmarkWholeStageCodegen"
  */
class BenchmarkWholeStageCodegen extends SparkFunSuite {
  lazy val conf = new SparkConf().setMaster("local[1]").setAppName("benchmark")
    .set("spark.sql.shuffle.partitions", "1")
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlContext = SQLContext.getOrCreate(sc)

  def runBenchmark(name: String, values: Int)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, values)

    Seq(false, true).foreach { enabled =>
      benchmark.addCase(s"$name codegen=$enabled") { iter =>
        sqlContext.setConf("spark.sql.codegen.wholeStage", enabled.toString)
        f
      }
    }

    benchmark.run()
  }

  def testWholeStage(values: Int): Unit = {

    runBenchmark("rang/filter/sum", values) {
      sqlContext.range(values).filter("(id & 1) = 1").groupBy().sum().collect()
    }
    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      rang/filter/sum:                median Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------
      rang/filter/sum codegen=false          12823.55      40.88          24.46     1.00 X
      rang/filter/sum codegen=true             831.80     630.30           1.59    15.42 X
    */
  }

  def testStatFunctions(values: Int): Unit = {

    runBenchmark("stddev", values) {
      sqlContext.range(values).groupBy().agg("id" -> "stddev").collect()
    }

    runBenchmark("kurtosis", values) {
      sqlContext.range(values).groupBy().agg("id" -> "kurtosis").collect()
    }


    /**
      Using ImperativeAggregate (as implemented in Spark 1.6):

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      stddev:                            Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------------
      stddev w/o codegen                      2019.04            10.39         1.00 X
      stddev w codegen                        2097.29            10.00         0.96 X
      kurtosis w/o codegen                    2108.99             9.94         0.96 X
      kurtosis w codegen                      2090.69            10.03         0.97 X

      Using DeclarativeAggregate:

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      stddev:                         median Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------
      stddev codegen=false                    1644.14      12.76          78.40     1.00 X
      stddev codegen=true                      349.35      60.03          16.66     4.71 X

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      kurtosis:                         median Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------
      kurtosis codegen=false                    3491.49       6.01         166.49     1.00 X
      kurtosis codegen=true                      561.54      37.35          26.78     6.22 X
      */
  }

  def testAggregateWithKey(values: Int): Unit = {

    runBenchmark("Aggregate w keys", values) {
      sqlContext.range(values).selectExpr("(id & 65535) as k").groupBy("k").sum().collect()
    }

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Aggregate w keys:               median Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------
    Aggregate w keys codegen=false          2390.44       8.77         113.99     1.00 X
    Aggregate w keys codegen=true           1669.62      12.56          79.61     1.43 X
    */
  }

  def testBytesToBytesMap(values: Int): Unit = {
    val benchmark = new Benchmark("BytesToBytesMap", values)

    benchmark.addCase("hash") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val valueBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      val value = new UnsafeRow(2)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      var s = 0
      while (i < values) {
        key.setInt(0, i % 1000)
        val h = Murmur3_x86_32.hashUnsafeWords(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes, 0)
        s += h
        i += 1
      }
    }

    Seq("off", "on").foreach { heap =>
      benchmark.addCase(s"BytesToBytesMap ($heap Heap)") { iter =>
        val taskMemoryManager = new TaskMemoryManager(
          new StaticMemoryManager(
            new SparkConf().set("spark.memory.offHeap.enabled", s"${heap == "off"}")
              .set("spark.memory.offHeap.size", "102400000"),
            Long.MaxValue,
            Long.MaxValue,
            1),
          0)
        val map = new BytesToBytesMap(taskMemoryManager, 1024, 64L<<20)
        val keyBytes = new Array[Byte](16)
        val valueBytes = new Array[Byte](16)
        val key = new UnsafeRow(1)
        key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
        val value = new UnsafeRow(2)
        value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
        var i = 0
        while (i < values) {
          key.setInt(0, i % 65536)
          val loc = map.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes)
          if (loc.isDefined) {
            value.pointTo(loc.getValueAddress.getBaseObject, loc.getValueAddress.getBaseOffset,
              loc.getValueLength)
            value.setInt(0, value.getInt(0) + 1)
            i += 1
          } else {
            loc.putNewKey(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
              value.getBaseObject, value.getBaseOffset, value.getSizeInBytes)
          }
        }
      }
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    BytesToBytesMap:                median Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------
    hash                                     663.70      78.99          12.66     1.00 X
    BytesToBytesMap (off Heap)              3389.42      15.47          64.65     0.20 X
    BytesToBytesMap (on Heap)               3476.07      15.08          66.30     0.19 X
      */
    benchmark.run()
  }

  // These benchmark are skipped in normal build
  test("benchmark") {
    // testWholeStage(200 << 20)
    // testStatFunctions(20 << 20)
    // testAggregateWithKey(20 << 20)
    // testBytesToBytesMap(50 << 20)
  }
}
