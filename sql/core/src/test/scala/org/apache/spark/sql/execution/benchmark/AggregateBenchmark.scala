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

import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.memory.{StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.joins.LongToUnsafeRowMap
import org.apache.spark.sql.execution.vectorized.AggregateHashMap
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure performance for aggregate primitives.
 * To run this:
 *  build/sbt "sql/test-only *benchmark.AggregateBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class AggregateBenchmark extends BenchmarkBase {

  ignore("aggregate without grouping") {
    val N = 500L << 22
    val benchmark = new Benchmark("agg without grouping", N)
    runBenchmark("agg w/o group", N) {
      sparkSession.range(N).selectExpr("sum(id)").collect()
    }
    /*
    agg w/o group:                           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    agg w/o group wholestage off                30136 / 31885         69.6          14.4       1.0X
    agg w/o group wholestage on                   1851 / 1860       1132.9           0.9      16.3X
     */
  }

  ignore("stat functions") {
    val N = 100L << 20

    runBenchmark("stddev", N) {
      sparkSession.range(N).groupBy().agg("id" -> "stddev").collect()
    }

    runBenchmark("kurtosis", N) {
      sparkSession.range(N).groupBy().agg("id" -> "kurtosis").collect()
    }

    /*
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
    val N = 20 << 22

    val benchmark = new Benchmark("Aggregate w keys", N)
    def f(): Unit = {
      sparkSession.range(N).selectExpr("(id & 65535) as k").groupBy("k").sum().collect()
    }

    benchmark.addCase(s"codegen = F", numIters = 2) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = F", numIters = 3) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "0")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T", numIters = 5) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "3")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    Aggregate w keys:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    codegen = F                                   6619 / 6780         12.7          78.9       1.0X
    codegen = T hashmap = F                       3935 / 4059         21.3          46.9       1.7X
    codegen = T hashmap = T                        897 /  971         93.5          10.7       7.4X
    */
  }

  ignore("aggregate with randomized keys") {
    val N = 20 << 22

    val benchmark = new Benchmark("Aggregate w keys", N)
    sparkSession.range(N).selectExpr("id", "floor(rand() * 10000) as k")
      .createOrReplaceTempView("test")

    def f(): Unit = sparkSession.sql("select k, k, sum(id) from test group by k, k").collect()

    benchmark.addCase(s"codegen = F", numIters = 2) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = false)
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = F", numIters = 3) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = true)
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", 0)
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T", numIters = 5) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = true)
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", 3)
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    Aggregate w keys:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    codegen = F                                   7445 / 7517         11.3          88.7       1.0X
    codegen = T hashmap = F                       4672 / 4703         18.0          55.7       1.6X
    codegen = T hashmap = T                       1764 / 1958         47.6          21.0       4.2X
    */
  }

  ignore("aggregate with string key") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w string key", N)
    def f(): Unit = sparkSession.range(N).selectExpr("id", "cast(id & 1023 as string) as k")
      .groupBy("k").count().collect()

    benchmark.addCase(s"codegen = F", numIters = 2) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = F", numIters = 3) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "0")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T", numIters = 5) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "3")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Aggregate w string key:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    codegen = F                              3307 / 3376          6.3         157.7       1.0X
    codegen = T hashmap = F                  2364 / 2471          8.9         112.7       1.4X
    codegen = T hashmap = T                  1740 / 1841         12.0          83.0       1.9X
    */
  }

  ignore("aggregate with decimal key") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w decimal key", N)
    def f(): Unit = sparkSession.range(N).selectExpr("id", "cast(id & 65535 as decimal) as k")
      .groupBy("k").count().collect()

    benchmark.addCase(s"codegen = F") { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = F") { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "0")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T") { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "3")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Aggregate w decimal key:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    codegen = F                              2756 / 2817          7.6         131.4       1.0X
    codegen = T hashmap = F                  1580 / 1647         13.3          75.4       1.7X
    codegen = T hashmap = T                   641 /  662         32.7          30.6       4.3X
    */
  }

  ignore("aggregate with multiple key types") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w multiple keys", N)
    def f(): Unit = sparkSession.range(N)
      .selectExpr(
        "id",
        "(id & 1023) as k1",
        "cast(id & 1023 as string) as k2",
        "cast(id & 1023 as int) as k3",
        "cast(id & 1023 as double) as k4",
        "cast(id & 1023 as float) as k5",
        "id > 1023 as k6")
      .groupBy("k1", "k2", "k3", "k4", "k5", "k6")
      .sum()
      .collect()

    benchmark.addCase(s"codegen = F") { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = F") { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "0")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T") { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "10")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Aggregate w decimal key:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    codegen = F                              5885 / 6091          3.6         280.6       1.0X
    codegen = T hashmap = F                  3625 / 4009          5.8         172.8       1.6X
    codegen = T hashmap = T                  3204 / 3271          6.5         152.8       1.8X
    */
  }


  ignore("cube") {
    val N = 5 << 20

    runBenchmark("cube", N) {
      sparkSession.range(N).selectExpr("id", "id % 1000 as k1", "id & 256 as k2")
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

    /*
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

}
