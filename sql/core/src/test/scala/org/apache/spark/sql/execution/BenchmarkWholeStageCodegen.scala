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

import java.util.HashMap

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
    rang/filter/sum codegen=true              897 / 1022        584.6           1.7      16.4X
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
    val M = 1 << 16
    val dim = broadcast(sqlContext.range(M).selectExpr("id as k", "cast(id as string) as v"))

    runBenchmark("Join w long", N) {
      sqlContext.range(N).join(dim, (col("id") bitwiseAND M) === col("k")).count()
    }

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w long:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w long codegen=false                5744 / 5814         18.3          54.8       1.0X
    Join w long codegen=true                  735 /  853        142.7           7.0       7.8X
    */

    val dim2 = broadcast(sqlContext.range(M)
      .selectExpr("cast(id as int) as k1", "cast(id as int) as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 ints", N) {
      sqlContext.range(N).join(dim2,
        (col("id") bitwiseAND M).cast(IntegerType) === col("k1")
          && (col("id") bitwiseAND M).cast(IntegerType) === col("k2")).count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w 2 ints:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 ints codegen=false              7159 / 7224         14.6          68.3       1.0X
    Join w 2 ints codegen=true               1135 / 1197         92.4          10.8       6.3X
    */

    val dim3 = broadcast(sqlContext.range(M)
      .selectExpr("id as k1", "id as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 longs", N) {
      sqlContext.range(N).join(dim3,
        (col("id") bitwiseAND M) === col("k1") && (col("id") bitwiseAND M) === col("k2"))
        .count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w 2 longs:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 longs codegen=false              7877 / 8358         13.3          75.1       1.0X
    Join w 2 longs codegen=true               3877 / 3937         27.0          37.0       2.0X
      */
    runBenchmark("outer join w long", N) {
      sqlContext.range(N).join(dim, (col("id") bitwiseAND M) === col("k"), "left").count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    outer join w long:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    outer join w long codegen=false        15280 / 16497          6.9         145.7       1.0X
    outer join w long codegen=true            769 /  796        136.3           7.3      19.9X
      */
  }

  ignore("rube") {
    val N = 5 << 20

    runBenchmark("cube", N) {
      sqlContext.range(N).selectExpr("id", "id % 1000 as k1", "id & 256 as k2")
        .cube("k1", "k2").sum("id").collect()
    }

    /**
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      cube:                               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      cube codegen=false                       3188 / 3392          1.6         608.2       1.0X
      cube codegen=true                        1239 / 1394          4.2         236.3       2.6X
      */
  }

  ignore("hash and BytesToBytesMap") {
    val N = 10 << 20

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

    benchmark.addCase("Java HashMap (Long)") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val valueBytes = new Array[Byte](16)
      val value = new UnsafeRow(1)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      value.setInt(0, 555)
      val map = new HashMap[Long, UnsafeRow]()
      while (i < 65536) {
        value.setInt(0, i)
        map.put(i.toLong, value)
        i += 1
      }
      var s = 0
      i = 0
      while (i < N) {
        if (map.get(i % 100000) != null) {
          s += 1
        }
        i += 1
      }
    }

    benchmark.addCase("Java HashMap (two ints) ") { iter =>
      var i = 0
      val valueBytes = new Array[Byte](16)
      val value = new UnsafeRow(1)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      value.setInt(0, 555)
      val map = new HashMap[Long, UnsafeRow]()
      while (i < 65536) {
        value.setInt(0, i)
        val key = (i.toLong << 32) + Integer.rotateRight(i, 15)
        map.put(key, value)
        i += 1
      }
      var s = 0
      i = 0
      while (i < N) {
        val key = ((i & 100000).toLong << 32) + Integer.rotateRight(i & 100000, 15)
        if (map.get(key) != null) {
          s += 1
        }
        i += 1
      }
    }

    benchmark.addCase("Java HashMap (UnsafeRow)") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val valueBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      val value = new UnsafeRow(1)
      value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      value.setInt(0, 555)
      val map = new HashMap[UnsafeRow, UnsafeRow]()
      while (i < 65536) {
        key.setInt(0, i)
        value.setInt(0, i)
        map.put(key, value.copy())
        i += 1
      }
      var s = 0
      i = 0
      while (i < N) {
        key.setInt(0, i % 100000)
        if (map.get(key) != null) {
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
    Java HashMap (Long)                       145 /  168         72.2          13.8       0.8X
    Java HashMap (two ints)                   157 /  164         66.8          15.0       0.8X
    Java HashMap (UnsafeRow)                  538 /  573         19.5          51.3       0.2X
    BytesToBytesMap (off Heap)               2594 / 2664         20.2          49.5       0.2X
    BytesToBytesMap (on Heap)                2693 / 2989         19.5          51.4       0.2X
      */
    benchmark.run()
  }
}
