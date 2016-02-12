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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
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

  // These benchmark are skipped in normal build
  ignore("range/filter/sum") {
    val N = 500 << 20
    runBenchmark("rang/filter/sum", N) {
      sqlContext.range(N).filter("(id & 1) = 1").groupBy().sum().collect()
    }
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    rang/filter/sum:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    rang/filter/sum codegen=false          14332 / 16646         36.0          27.8       1.0X
    rang/filter/sum codegen=true              845 /  940        620.0           1.6      17.0X
    */
  }

  ignore("stat functions") {
    val N = 100 << 20

    runBenchmark("stddev", N) {
      sqlContext.range(N).groupBy().agg("id" -> "stddev").collect()
    }

    runBenchmark("kurtosis", N) {
      sqlContext.range(N).groupBy().agg("id" -> "kurtosis").collect()
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
      stddev:                             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      stddev codegen=false                     5630 / 5776         18.0          55.6       1.0X
      stddev codegen=true                      1259 / 1314         83.0          12.0       4.5X

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      kurtosis:                           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      kurtosis codegen=false                 14847 / 15084          7.0         142.9       1.0X
      kurtosis codegen=true                    1652 / 2124         63.0          15.9       9.0X
      */
  }

  ignore("aggregate with keys") {
    val N = 20 << 20

    runBenchmark("Aggregate w keys", N) {
      sqlContext.range(N).selectExpr("(id & 65535) as k").groupBy("k").sum().collect()
    }

    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      Aggregate w keys:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      Aggregate w keys codegen=false           2429 / 2644          8.6         115.8       1.0X
      Aggregate w keys codegen=true            1535 / 1571         13.7          73.2       1.6X
    */
  }

  ignore("broadcast hash join") {
    val N = 100 << 20
    val dim = broadcast(sqlContext.range(1 << 16).selectExpr("id as k", "cast(id as string) as v"))

    runBenchmark("Join w long", N) {
      sqlContext.range(N).join(dim, (col("id") % 60000) === col("k")).count()
    }

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    BroadcastHashJoin:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w long codegen=false        10174 / 10317         10.0         100.0       1.0X
    Join w long codegen=true           1069 / 1107         98.0          10.2       9.5X
    */

    val dim2 = broadcast(sqlContext.range(1 << 16)
      .selectExpr("cast(id as int) as k1", "cast(id as int) as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 ints", N) {
      sqlContext.range(N).join(dim2,
        (col("id") bitwiseAND 60000).cast(IntegerType) === col("k1")
          && (col("id") bitwiseAND 50000).cast(IntegerType) === col("k2")).count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    BroadcastHashJoin:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 ints codegen=false           11435 / 11530          9.0         111.1       1.0X
    Join w 2 ints codegen=true              1265 / 1424         82.0          12.2       9.0X
    */

  }

  ignore("hash and BytesToBytesMap") {
    val N = 50 << 20

    val benchmark = new Benchmark("BytesToBytesMap", N)

    benchmark.addCase("hash") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      var s = 0
      while (i < N) {
        key.setInt(0, i % 1000)
        val h = Murmur3_x86_32.hashUnsafeWords(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes, 42)
        s += h
        i += 1
      }
    }

    benchmark.addCase("fast hash") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      var s = 0
      while (i < N) {
        key.setInt(0, i % 1000)
        val h = Murmur3_x86_32.hashLong(i % 1000, 42)
        s += h
        i += 1
      }
    }

    benchmark.addCase("arrayEqual") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val valueBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      val value = new UnsafeRow(1)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      value.setInt(0, 555)
      var s = 0
      while (i < N) {
        key.setInt(0, i % 1000)
        if (key.equals(value)) {
          s += 1
        }
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
        val value = new UnsafeRow(1)
        value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
        var i = 0
        while (i < N) {
          key.setInt(0, i % 65536)
          val loc = map.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
            Murmur3_x86_32.hashLong(i % 65536, 42))
          if (loc.isDefined) {
            value.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
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
    BytesToBytesMap:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    hash                                      651 /  678         80.0          12.5       1.0X
    fast hash                                 336 /  343        155.9           6.4       1.9X
    arrayEqual                                417 /  428        125.0           8.0       1.6X
    BytesToBytesMap (off Heap)               2594 / 2664         20.2          49.5       0.2X
    BytesToBytesMap (on Heap)                2693 / 2989         19.5          51.4       0.2X
      */
    benchmark.run()
  }
}
