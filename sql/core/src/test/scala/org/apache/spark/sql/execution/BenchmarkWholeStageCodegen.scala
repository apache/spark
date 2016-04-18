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
import org.apache.spark.sql.execution.joins.LongToUnsafeRowMap
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.vectorized.AggregateHashMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
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
    .set("spark.sql.autoBroadcastJoinThreshold", "1")
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlContext = SQLContext.getOrCreate(sc)

  def runBenchmark(name: String, values: Long)(f: => Unit): Unit = {
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
    val N = 500L << 20
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

  ignore("range/limit/sum") {
    val N = 500L << 20
    runBenchmark("range/limit/sum", N) {
      sqlContext.range(N).limit(1000000).groupBy().sum().collect()
    }
    /*
    Westmere E56xx/L56xx/X56xx (Nehalem-C)
    range/limit/sum:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    range/limit/sum codegen=false             609 /  672        861.6           1.2       1.0X
    range/limit/sum codegen=true              561 /  621        935.3           1.1       1.1X
    */
  }

  ignore("range/sample/sum") {
    val N = 500 << 20
    runBenchmark("range/sample/sum", N) {
      sqlContext.range(N).sample(true, 0.01).groupBy().sum().collect()
    }
    /*
    Westmere E56xx/L56xx/X56xx (Nehalem-C)
    range/sample/sum:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    range/sample/sum codegen=false         53888 / 56592          9.7         102.8       1.0X
    range/sample/sum codegen=true          41614 / 42607         12.6          79.4       1.3X
    */

    runBenchmark("range/sample/sum", N) {
      sqlContext.range(N).sample(false, 0.01).groupBy().sum().collect()
    }
    /*
    Westmere E56xx/L56xx/X56xx (Nehalem-C)
    range/sample/sum:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    range/sample/sum codegen=false         12982 / 13384         40.4          24.8       1.0X
    range/sample/sum codegen=true            7074 / 7383         74.1          13.5       1.8X
    */
  }

  ignore("stat functions") {
    val N = 100L << 20

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

  ignore("aggregate with linear keys") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w keys", N)
    def f(): Unit = sqlContext.range(N).selectExpr("(id & 65535) as k").groupBy("k").sum().collect()

    benchmark.addCase(s"codegen = F") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = F") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.setConf("spark.sql.codegen.aggregate.map.enabled", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.setConf("spark.sql.codegen.aggregate.map.enabled", "true")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Aggregate w keys:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    codegen = F                              2067 / 2166         10.1          98.6       1.0X
    codegen = T hashmap = F                  1149 / 1321         18.3          54.8       1.8X
    codegen = T hashmap = T                   388 /  475         54.0          18.5       5.3X
    */
  }

  ignore("aggregate with randomized keys") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w keys", N)
    sqlContext.range(N).selectExpr("id", "floor(rand() * 10000) as k").registerTempTable("test")

    def f(): Unit = sqlContext.sql("select k, k, sum(id) from test group by k, k").collect()

    benchmark.addCase(s"codegen = F") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = F") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.setConf("spark.sql.codegen.aggregate.map.enabled", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.setConf("spark.sql.codegen.aggregate.map.enabled", "true")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Aggregate w keys:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    codegen = F                              2517 / 2608          8.3         120.0       1.0X
    codegen = T hashmap = F                  1484 / 1560         14.1          70.8       1.7X
    codegen = T hashmap = T                   794 /  908         26.4          37.9       3.2X
    */
  }

  ignore("broadcast hash join") {
    val N = 20 << 20
    val M = 1 << 16
    val dim = broadcast(sqlContext.range(M).selectExpr("id as k", "cast(id as string) as v"))

    runBenchmark("Join w long", N) {
      sqlContext.range(N).join(dim, (col("id") % M) === col("k")).count()
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w long:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w long codegen=false                3002 / 3262          7.0         143.2       1.0X
    Join w long codegen=true                  321 /  371         65.3          15.3       9.3X
    */

    runBenchmark("Join w long duplicated", N) {
      val dim = broadcast(sqlContext.range(M).selectExpr("cast(id/10 as long) as k"))
      sqlContext.range(N).join(dim, (col("id") % M) === col("k")).count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w long duplicated:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w long duplicated codegen=false      3446 / 3478          6.1         164.3       1.0X
    Join w long duplicated codegen=true       322 /  351         65.2          15.3      10.7X
    */

    val dim2 = broadcast(sqlContext.range(M)
      .selectExpr("cast(id as int) as k1", "cast(id as int) as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 ints", N) {
      sqlContext.range(N).join(dim2,
        (col("id") % M).cast(IntegerType) === col("k1")
          && (col("id") % M).cast(IntegerType) === col("k2")).count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w 2 ints:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 ints codegen=false              4426 / 4501          4.7         211.1       1.0X
    Join w 2 ints codegen=true                791 /  818         26.5          37.7       5.6X
    */

    val dim3 = broadcast(sqlContext.range(M)
      .selectExpr("id as k1", "id as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 longs", N) {
      sqlContext.range(N).join(dim3,
        (col("id") % M) === col("k1") && (col("id") % M) === col("k2"))
        .count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w 2 longs:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 longs codegen=false             5905 / 6123          3.6         281.6       1.0X
    Join w 2 longs codegen=true              2230 / 2529          9.4         106.3       2.6X
      */

    val dim4 = broadcast(sqlContext.range(M)
      .selectExpr("cast(id/10 as long) as k1", "cast(id/10 as long) as k2"))

    runBenchmark("Join w 2 longs duplicated", N) {
      sqlContext.range(N).join(dim4,
        (col("id") bitwiseAND M) === col("k1") && (col("id") bitwiseAND M) === col("k2"))
        .count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w 2 longs duplicated:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w 2 longs duplicated codegen=false      6420 / 6587          3.3         306.1       1.0X
    Join w 2 longs duplicated codegen=true      2080 / 2139         10.1          99.2       3.1X
     */

    runBenchmark("outer join w long", N) {
      sqlContext.range(N).join(dim, (col("id") % M) === col("k"), "left").count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    outer join w long:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    outer join w long codegen=false          3055 / 3189          6.9         145.7       1.0X
    outer join w long codegen=true            261 /  276         80.5          12.4      11.7X
      */

    runBenchmark("semi join w long", N) {
      sqlContext.range(N).join(dim, (col("id") % M) === col("k"), "leftsemi").count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    semi join w long:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    semi join w long codegen=false           1912 / 1990         11.0          91.2       1.0X
    semi join w long codegen=true             237 /  244         88.3          11.3       8.1X
     */
  }

  ignore("sort merge join") {
    val N = 2 << 20
    runBenchmark("merge join", N) {
      val df1 = sqlContext.range(N).selectExpr(s"id * 2 as k1")
      val df2 = sqlContext.range(N).selectExpr(s"id * 3 as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    merge join:                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    merge join codegen=false                 1588 / 1880          1.3         757.1       1.0X
    merge join codegen=true                  1477 / 1531          1.4         704.2       1.1X
      */

    runBenchmark("sort merge join", N) {
      val df1 = sqlContext.range(N)
        .selectExpr(s"(id * 15485863) % ${N*10} as k1")
      val df2 = sqlContext.range(N)
        .selectExpr(s"(id * 15485867) % ${N*10} as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    sort merge join:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    sort merge join codegen=false            3626 / 3667          0.6        1728.9       1.0X
    sort merge join codegen=true             3405 / 3438          0.6        1623.8       1.1X
      */
  }

  ignore("shuffle hash join") {
    val N = 4 << 20
    sqlContext.setConf("spark.sql.shuffle.partitions", "2")
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "10000000")
    sqlContext.setConf("spark.sql.join.preferSortMergeJoin", "false")
    runBenchmark("shuffle hash join", N) {
      val df1 = sqlContext.range(N).selectExpr(s"id as k1")
      val df2 = sqlContext.range(N / 5).selectExpr(s"id * 3 as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /**
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    shuffle hash join:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    shuffle hash join codegen=false          1101 / 1391          3.8         262.6       1.0X
    shuffle hash join codegen=true            528 /  578          7.9         125.8       2.1X
     */
  }

  ignore("cube") {
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
    val N = 20 << 20

    val benchmark = new Benchmark("BytesToBytesMap", N)

    benchmark.addCase("UnsafeRowhash") { iter =>
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

    benchmark.addCase("murmur3 hash") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      var p = 524283
      var s = 0
      while (i < N) {
        var h = Murmur3_x86_32.hashLong(i, 42)
        key.setInt(0, h)
        s += h
        i += 1
      }
    }

    benchmark.addCase("fast hash") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      var p = 524283
      var s = 0
      while (i < N) {
        var h = i % p
        if (h < 0) {
          h += p
        }
        key.setInt(0, h)
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

    Seq(false, true).foreach { optimized =>
      benchmark.addCase(s"LongToUnsafeRowMap (opt=$optimized)") { iter =>
        var i = 0
        val valueBytes = new Array[Byte](16)
        val value = new UnsafeRow(1)
        value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
        value.setInt(0, 555)
        val taskMemoryManager = new TaskMemoryManager(
          new StaticMemoryManager(
            new SparkConf().set("spark.memory.offHeap.enabled", "false"),
            Long.MaxValue,
            Long.MaxValue,
            1),
          0)
        val map = new LongToUnsafeRowMap(taskMemoryManager, 64)
        while (i < 65536) {
          value.setInt(0, i)
          val key = i % 100000
          map.append(key, value)
          i += 1
        }
        if (optimized) {
          map.optimize()
        }
        var s = 0
        i = 0
        while (i < N) {
          val key = i % 100000
          if (map.getValue(key, value) != null) {
            s += 1
          }
          i += 1
        }
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
        val numKeys = 65536
        while (i < numKeys) {
          key.setInt(0, i % 65536)
          val loc = map.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
            Murmur3_x86_32.hashLong(i % 65536, 42))
          if (!loc.isDefined) {
            loc.append(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
              value.getBaseObject, value.getBaseOffset, value.getSizeInBytes)
          }
          i += 1
        }
        i = 0
        var s = 0
        while (i < N) {
          key.setInt(0, i % 100000)
          val loc = map.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
            Murmur3_x86_32.hashLong(i % 100000, 42))
          if (loc.isDefined) {
            s += 1
          }
          i += 1
        }
      }
    }

    benchmark.addCase("Aggregate HashMap") { iter =>
      var i = 0
      val numKeys = 65536
      val schema = new StructType()
        .add("key", LongType)
        .add("value", LongType)
      val map = new AggregateHashMap(schema)
      while (i < numKeys) {
        val row = map.findOrInsert(i.toLong)
        row.setLong(1, row.getLong(1) +  1)
        i += 1
      }
      var s = 0
      i = 0
      while (i < N) {
        if (map.find(i % 100000) != -1) {
          s += 1
        }
        i += 1
      }
    }

    /**
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    BytesToBytesMap:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    UnsafeRow hash                            267 /  284         78.4          12.8       1.0X
    murmur3 hash                              102 /  129        205.5           4.9       2.6X
    fast hash                                  79 /   96        263.8           3.8       3.4X
    arrayEqual                                164 /  172        128.2           7.8       1.6X
    Java HashMap (Long)                       321 /  399         65.4          15.3       0.8X
    Java HashMap (two ints)                   328 /  363         63.9          15.7       0.8X
    Java HashMap (UnsafeRow)                 1140 / 1200         18.4          54.3       0.2X
    LongToUnsafeRowMap (opt=false)            378 /  400         55.5          18.0       0.7X
    LongToUnsafeRowMap (opt=true)             144 /  152        145.2           6.9       1.9X
    BytesToBytesMap (off Heap)               1300 / 1616         16.1          62.0       0.2X
    BytesToBytesMap (on Heap)                1165 / 1202         18.0          55.5       0.2X
    Aggregate HashMap                         121 /  131        173.3           5.8       2.2X
    */
    benchmark.run()
  }

  ignore("collect") {
    val N = 1 << 20

    val benchmark = new Benchmark("collect", N)
    benchmark.addCase("collect 1 million") { iter =>
      sqlContext.range(N).collect()
    }
    benchmark.addCase("collect 2 millions") { iter =>
      sqlContext.range(N * 2).collect()
    }
    benchmark.addCase("collect 4 millions") { iter =>
      sqlContext.range(N * 4).collect()
    }
    benchmark.run()

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    collect:                            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    collect 1 million                         439 /  654          2.4         418.7       1.0X
    collect 2 millions                        961 / 1907          1.1         916.4       0.5X
    collect 4 millions                       3193 / 3895          0.3        3044.7       0.1X
     */
  }

  ignore("collect limit") {
    val N = 1 << 20

    val benchmark = new Benchmark("collect limit", N)
    benchmark.addCase("collect limit 1 million") { iter =>
      sqlContext.range(N * 4).limit(N).collect()
    }
    benchmark.addCase("collect limit 2 millions") { iter =>
      sqlContext.range(N * 4).limit(N * 2).collect()
    }
    benchmark.run()

    /**
    model name      : Westmere E56xx/L56xx/X56xx (Nehalem-C)
    collect limit:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    collect limit 1 million                   833 / 1284          1.3         794.4       1.0X
    collect limit 2 millions                 3348 / 4005          0.3        3193.3       0.2X
     */
  }
}
