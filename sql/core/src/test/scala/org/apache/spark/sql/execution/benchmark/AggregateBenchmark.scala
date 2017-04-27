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
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T", numIters = 5) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.vectorized.enable", "true")
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
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T", numIters = 5) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = true)
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.vectorized.enable", "true")
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
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T", numIters = 5) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.vectorized.enable", "true")
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
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T") { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.vectorized.enable", "true")
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
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "false")
      f()
    }

    benchmark.addCase(s"codegen = T hashmap = T") { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.twolevel.enable", "true")
      sparkSession.conf.set("spark.sql.codegen.aggregate.map.vectorized.enable", "true")
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

    /*
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

  // This test does not do any benchmark, instead it produces generated code for vectorized
  // and row-based hashmaps.
  ignore("generated code comparison for vectorized vs. rowbased") {
    val N = 20 << 23

    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    sparkSession.range(N)
      .selectExpr(
        "id & 1023 as k1",
        "cast (id & 1023 as string) as k2")
      .createOrReplaceTempView("test")

    // dataframe/query
    val query = sparkSession.sql("select count(k1), sum(k1) from test group by k1, k2")

    // vectorized
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", "vectorized")
    query.queryExecution.debug.codegen()

    // row based
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", "rowbased")
    query.queryExecution.debug.codegen()
  }

  ignore("1 key field, 1 value field, distinct linear keys") {
    val N = 20 << 22;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 0
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Distinct Keys", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 15) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          sparkSession.range(N)
            .selectExpr(
              "id & " + ((1 << i) - 1) + " as k0")
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select sum(k0)" +
            " from test group by k0").collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", 1 << i, results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

      Num. Distinct Keys      No Fast Hashmap           Vectorized            Row-based
                       1                   21                   13                   11
                       2                   23                   14                   13
                       4                   23                   14                   14
                       8                   23                   14                   14
                      16                   23                   12                   13
                      32                   24                   12                   13
                      64                   24                   14                   16
                     128                   24                   14                   13
                     256                   25                   14                   14
                     512                   25                   16                   14
                    1024                   25                   16                   15
                    2048                   26                   12                   15
                    4096                   27                   15                   15
                    8192                   33                   16                   15
                   16384                   34                   15                   15
    Unit: ns/row
    */
  }

  ignore("1 key field, 1 value field, distinct random keys") {
    val N = 20 << 22;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 0
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Distinct Keys", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 15) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          sparkSession.range(N)
            .selectExpr(
              "cast(floor(rand() * " + (1 << i) + ") as long) as k0")
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select sum(k0)" +
            " from test group by k0").collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", 1 << i, results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

      Num. Distinct Keys      No Fast Hashmap           Vectorized            Row-based
                       1                   32                    9                   13
                       2                   39                   16                   22
                       4                   39                   14                   23
                       8                   39                   13                   22
                      16                   38                   13                   20
                      32                   38                   13                   20
                      64                   38                   13                   20
                     128                   37                   16                   21
                     256                   36                   17                   22
                     512                   38                   17                   21
                    1024                   39                   18                   21
                    2048                   41                   18                   21
                    4096                   44                   18                   22
                    8192                   49                   20                   23
                   16384                   52                   23                   25
    Unit: ns/row
    */
  }

  ignore("1 key field, varying value fields, 16 linear distinct keys") {
    val N = 20 << 22;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 1
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Value Fields", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 11) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          sparkSession.range(N)
            .selectExpr("id & " + 15  + " as k0")
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select " + List.range(0, i).map(x => "sum(k" + 0 + ")").mkString(",") +
            " from test group by k0").collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", i, results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

       Num. Value Fields      No Fast Hashmap           Vectorized            Row-based
                       1                   24                   15                   12
                       2                   25                   24                   14
                       3                   29                   25                   17
                       4                   31                   32                   22
                       5                   33                   40                   24
                       6                   36                   36                   27
                       7                   38                   44                   28
                       8                   47                   50                   32
                       9                   52                   55                   37
                      10                   59                   59                   45
    Unit: ns/row
    */
  }

  ignore("varying key fields, 1 value field, 16 linear distinct keys") {
    val N = 20 << 22;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 1
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Key Fields", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 11) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          val s = "id & " + 15 + " as k"
          sparkSession.range(N)
            .selectExpr(List.range(0, i).map(x => s + x): _*)
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select sum(k0)" +
            " from test group by " + List.range(0, i).map(x => "k" + x).mkString(",")).collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", i, results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

         Num. Key Fields      No Fast Hashmap           Vectorized            Row-based
                       1                   24                   15                   13
                       2                   31                   20                   14
                       3                   37                   22                   17
                       4                   46                   26                   18
                       5                   53                   27                   20
                       6                   61                   29                   23
                       7                   69                   36                   25
                       8                   78                   37                   27
                       9                   88                   43                   30
                      10                   92                   45                   33
    Unit: ns/row
    */
  }

  ignore("varying key fields, varying value field, 16 linear distinct keys") {
    val N = 20 << 22;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 1
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Total Fields", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 11) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          val s = "id & " + 15 + " as k"
          sparkSession.range(N)
            .selectExpr(List.range(0, i).map(x => s + x): _*)
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select " + List.range(0, i).map(x => "sum(k" + x + ")").mkString(",") +
            " from test group by " + List.range(0, i).map(x => "k" + x).mkString(",")).collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          // printf("nsPerRow i=%d j=%d mode=%10s %20s\n", i, j, mode, nsPerRow)
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", i * 2, results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

       Num. Total Fields      No Fast Hashmap           Vectorized            Row-based
                       2                   24                   14                   12
                       4                   32                   28                   17
                       6                   42                   29                   21
                       8                   53                   36                   24
                      10                   62                   44                   29
                      12                   77                   50                   34
                      14                   93                   61                   37
                      16                  109                   75                   41
                      18                  124                   88                   51
                      20                  145                   97                   70
    Unit: ns/row
    */
  }

  ignore("varying key fields, varying value field, 512 linear distinct keys") {
    val N = 20 << 22;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 1
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Total Fields", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 11) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          val s = "id & " + 511 + " as k"
          sparkSession.range(N)
            .selectExpr(List.range(0, i).map(x => s + x): _*)
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select " + List.range(0, i).map(x => "sum(k" + x + ")").mkString(",") +
            " from test group by " + List.range(0, i).map(x => "k" + x).mkString(",")).collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          // printf("nsPerRow i=%d j=%d mode=%10s %20s\n", i, j, mode, nsPerRow)
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", i * 2, results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

       Num. Total Fields      No Fast Hashmap           Vectorized            Row-based
                       2                   26                   16                   13
                       4                   36                   24                   17
                       6                   45                   30                   22
                       8                   54                   33                   27
                      10                   64                   38                   30
                      12                   74                   47                   35
                      14                   95                   54                   39
                      16                  114                   72                   44
                      18                  129                   70                   51
                      20                  150                   91                   72
    Unit: ns/row
    */
  }


  ignore("varying key fields, varying value field, varying linear distinct keys") {
    val N = 20 << 22;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 1
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Total Fields", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 11) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          val s = "id & " + (1 << (i-1) - 1) + " as k"
          sparkSession.range(N)
            .selectExpr(List.range(0, i).map(x => s + x): _*)
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select " + List.range(0, i).map(x => "sum(k" + x + ")").mkString(",") +
            " from test group by " + List.range(0, i).map(x => "k" + x).mkString(",")).collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          // printf("nsPerRow i=%d j=%d mode=%10s %20s\n", i, j, mode, nsPerRow)
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", i * 2, results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

       Num. Total Fields      No Fast Hashmap           Vectorized            Row-based
                       2                   24                   11                   10
                       4                   33                   25                   16
                       6                   42                   30                   21
                       8                   53                   44                   24
                      10                   65                   52                   27
                      12                   74                   47                   33
                      14                   92                   69                   35
                      16                  109                   77                   40
                      18                  127                   75                   49
                      20                  143                   80                   66
    Unit: ns/row

    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Linux 3.13.0-74-generic
    Intel(R) Xeon(R) CPU E5-2676 v3 @ 2.40GHz
       Num. Total Fields      No Fast Hashmap           Vectorized            Row-based
                       2                   38                   15                   15
                       4                   50                   25                   25
                       6                   65                   35                   30
                       8                   79                   42                   35
                      10                   93                   50                   43
                      12                  108                   58                   48
                      14                  120                   71                   57
                      16                  145                   79                   62
                      18                  166                   88                   77
                      20                  189                   96                   98
    Unit: ns/row
    */
  }

  ignore("4 key fields, 4 value field, varying linear distinct keys") {
    val N = 20 << 22;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 0
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Distinct Keys", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 17) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          val s = "id & " + ((1<<i)-1) + " as k"
          sparkSession.range(N)
            .selectExpr(List.range(0, 4).map(x => s + x): _*)
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select " + List.range(0, 4).map(x => "sum(k" + x + ")").mkString(",") +
            " from test group by " + List.range(0, 4).map(x => "k" + x).mkString(",")).collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          // printf("nsPerRow i=%d j=%d mode=%10s %20s\n", i, j, mode, nsPerRow)
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", (1<<i), results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

      Num. Distinct Keys      No Fast Hashmap           Vectorized            Row-based
                       1                   33                   38                   24
                       2                   58                   43                   30
                       4                   58                   42                   28
                       8                   57                   46                   28
                      16                   56                   41                   28
                      32                   55                   44                   27
                      64                   56                   48                   27
                     128                   58                   43                   27
                     256                   60                   43                   30
                     512                   61                   45                   31
                    1024                   62                   44                   31
                    2048                   64                   42                   38
                    4096                   66                   47                   38
                    8192                   70                   48                   38
                   16384                   72                   48                   42
                   32768                   77                   54                   47
                   65536                   96                   75                   61
                  131072                  115                  119                  130
                  262144                  137                  162                  185
    Unit: ns/row
    */
  }

  ignore("single key field, single value field, varying linear distinct keys") {
    val N = 20 << 21;

    var timeStart: Long = 0L
    var timeEnd: Long = 0L
    var nsPerRow: Long = 0L
    var i = 0
    sparkSession.conf.set("spark.sql.codegen.wholeStage", "true")
    sparkSession.conf.set("spark.sql.codegen.aggregate.map.columns.max", "30")

    // scalastyle:off
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%20s %20s %20s %20s\n", "Num. Distinct Keys", "No Fast Hashmap",
      "Vectorized", "Row-based")
    // scalastyle:on

    val modes = List("skip", "vectorized", "rowbased")

    while (i < 21) {
      val results = modes.map(mode => {
        sparkSession.conf.set("spark.sql.codegen.aggregate.map.enforce.impl", mode)
        var j = 0
        var minTime: Long = 1000
        while (j < 5) {
          System.gc()
          val s = "id & " + ((1 << i) - 1) + " as k"
          sparkSession.range(N)
            .selectExpr(List.range(0, 2).map(x => s + x): _*)
            .createOrReplaceTempView("test")
          timeStart = System.nanoTime
          sparkSession.sql("select sum(k1) from test group by k0").collect()
          timeEnd = System.nanoTime
          nsPerRow = (timeEnd - timeStart) / N
          // printf("nsPerRow i=%d j=%d mode=%10s %20s\n", i, j, mode, nsPerRow)
          if (j > 1 && minTime > nsPerRow) minTime = nsPerRow
          j += 1
        }
        minTime
      })
      printf("%20s %20s %20s %20s\n", (1 << i), results(0), results(1), results(2))
      i += 1
    }
    printf("Unit: ns/row\n")

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

    Partial results:

      Num. Distinct Keys      No Fast Hashmap           Vectorized            Row-based
                       1                   23                   14                   12
                       8                   25                   13                   14
                      64                   24                   13                   14
                     512                   27                   15                   14
                    4096                   29                   18                   15
                   32768                   39                   17                   16
                   65536                   46                   19                   15
                  131072                   65                   38                   35
                  262144                   91                   74                   86
                  524288                  119                   93                   95
    Unit: ns/row
    */
  }

  ignore("TPCDS mini-scale benchmark") {
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.5
    Intel(R) Core(TM) i7-4980HQ CPU @ 2.80GHz

    agg_doAggregateWithKey() runtime (ms) for TPCDS queries with scale factor = 1

    query   skip         vectorized          rowbased

    q46      293        298  (0.9x)       296  (0.9x)
    q47      758        925  (0.8x)       831  (0.9x)
    q44      353        330  (1.0x)       327  (1.0x)
    q45      55         54   (1.0x)       56   (0.9x)
    q42      132        133  (0.9x)       138  (0.9x)
    q43      214        249  (0.8x)       226  (0.9x)
    q40      173        165  (1.0x)       168  (1.0x)
    q41      11         6    (1.8x)       5    (2.2x)
    q49      413        416  (0.9x)       428  (0.9x)
    q1       83         69   (1.2x)       68   (1.2x)
    q3       149        144  (1.0x)       137  (1.0x)
    q2       419        434  (0.9x)       404  (1.0x)
    q5       522        550  (0.9x)       568  (0.9x)
    q4       1795       1817 (0.9x)       1867 (0.9x)
    q7       270        273  (0.9x)       284  (0.9x)
    q6       31         31   (1.0x)       32   (0.9x)
    q8       203        159  (1.2x)       155  (1.3x)
    q33      273        265  (1.0x)       262  (1.0x)
    q32      61         53   (1.1x)       53   (1.1x)
    q31      502        512  (0.9x)       512  (0.9x)
    q30      41         35   (1.1x)       33   (1.2x)
    q36      409        429  (0.9x)       418  (0.9x)
    q35      274        279  (0.9x)       268  (1.0x)
    q34      154        136  (1.1x)       150  (1.0x)
    q14b     3453       3412 (1.0x)       3499 (0.9x)
    q21      442        437  (1.0x)       435  (1.0x)
    q22      2911       3127 (0.9x)       3057 (0.9x)
    q14a     3360       3299 (1.0x)       3660 (0.9x)
    q25      622        626  (0.9x)       541  (1.1x)
    q26      107        110  (0.9x)       107  (1.0x)
    q27      250        253  (0.9x)       271  (0.9x)
    q28      762        785  (0.9x)       807  (0.9x)
    q29      493        482  (1.0x)       488  (1.0x)
    q98      154        159  (0.9x)       173  (0.8x)
    q95      1349       983  (1.3x)       1058 (1.2x)
    q94      1166       1101 (1.0x)       1310 (0.8x)
    q91      3          3    (1.0x)       3    (1.0x)
    q93      430        446  (0.9x)       446  (0.9x)
    q92      40         33   (1.2x)       33   (1.2x)
    q15      246        239  (1.0x)       242  (1.0x)
    q17      574        625  (0.9x)       603  (0.9x)
    q16      3024       2384 (1.2x)       2601 (1.1x)
    q11      967        942  (1.0x)       946  (1.0x)
    q10      168        94   (1.7x)       188  (0.8x)
    q12      42         41   (1.0x)       44   (0.9x)
    q19      204        203  (1.0x)       212  (0.9x)
    q39b     903        900  (1.0x)       881  (1.0x)
    q39a     887        924  (0.9x)       888  (0.9x)
    q79      276        269  (1.0x)       271  (1.0x)
    q78      970        1007 (0.9x)       1031 (0.9x)
    q77      347        339  (1.0x)       345  (1.0x)
    q76      218        219  (0.9x)       221  (0.9x)
    q75      75         68   (1.1x)       67   (1.1x)
    q74      605        686  (0.8x)       659  (0.9x)
    q73      144        142  (1.0x)       148  (0.9x)
    q72      11211      11182(1.0x)       11159(1.0x)
    q71      292        286  (1.0x)       287  (1.0x)
    q70      483        478  (1.0x)       507  (0.9x)
    q20      73         77   (0.9x)       75   (0.9x)
    q83      27         23   (1.1x)       21   (1.2x)
    q80      948        980  (0.9x)       998  (0.9x)
    q81      60         45   (1.3x)       45   (1.3x)
    q86      79         86   (0.9x)       82   (0.9x)
    q85      136        122  (1.1x)       130  (1.0x)
    q89      169        177  (0.9x)       175  (0.9x)
    q68      364        369  (0.9x)       415  (0.8x)
    q69      90         82   (1.0x)       83   (1.0x)
    q64      494        495  (0.9x)       469  (1.0x)
    q65      361        375  (0.9x)       389  (0.9x)
    q66      926        924  (1.0x)       910  (1.0x)
    q67      1650       1894 (0.8x)       1882 (0.8x)
    q60      310        302  (1.0x)       293  (1.0x)
    q62      96         88   (1.0x)       82   (1.1x)
    q63      174        164  (1.0x)       167  (1.0x)
    q51      254        306  (0.8x)       343  (0.7x)
    q50      483        484  (0.9x)       449  (1.0x)
    q53      173        163  (1.0x)       166  (1.0x)
    q52      134        150  (0.8x)       153  (0.8x)
    q55      152        151  (1.0x)       153  (0.9x)
    q54      1266       1162 (1.0x)       1182 (1.0x)
    q57      419        481  (0.8x)       409  (1.0x)
    q56      299        285  (1.0x)       284  (1.0x)
    q59      561        537  (1.0x)       675  (0.8x)
    q58      276        282  (0.9x)       330  (0.8x)
    q23a     3360       4675 (0.7x)       4762 (0.7x)
    q23b     3121       4355 (0.7x)       4357 (0.7x)
    q24a     953        967  (0.9x)       968  (0.9x)
    q24b     962        959  (1.0x)       981  (0.9x)
    */
  }
}
