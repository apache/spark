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
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.config._
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.joins.LongToUnsafeRowMap
import org.apache.spark.sql.execution.vectorized.AggregateHashMap
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.map.BytesToBytesMap

/**
 * Benchmark to measure performance for aggregate primitives.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/AggregateBenchmark-results.txt".
 * }}}
 */
object AggregateBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("aggregate without grouping") {
      val N = 500L << 22
      codegenBenchmark("agg w/o group", N) {
        spark.range(N).selectExpr("sum(id)").noop()
      }
    }

    runBenchmark("stat functions") {
      val N = 100L << 20

      codegenBenchmark("stddev", N) {
        spark.range(N).groupBy().agg("id" -> "stddev").noop()
      }

      codegenBenchmark("kurtosis", N) {
        spark.range(N).groupBy().agg("id" -> "kurtosis").noop()
      }
    }

    runBenchmark("aggregate with linear keys") {
      val N = 20 << 22

      val benchmark = new Benchmark("Aggregate w keys", N, output = output)

      def f(): Unit = {
        spark.range(N).selectExpr("(id & 65535) as k").groupBy("k").sum().noop()
      }

      benchmark.addCase("codegen = F", numIters = 2) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = F", numIters = 3) { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = T", numIters = 5) { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "true",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "true") {
          f()
        }
      }

      benchmark.run()
    }

    runBenchmark("aggregate with randomized keys") {
      val N = 20 << 22

      val benchmark = new Benchmark("Aggregate w keys", N, output = output)
      spark.range(N).selectExpr("id", "floor(rand() * 10000) as k")
        .createOrReplaceTempView("test")

      def f(): Unit = spark.sql("select k, k, sum(id) from test group by k, k").noop()

      benchmark.addCase("codegen = F", numIters = 2) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = F", numIters = 3) { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = T", numIters = 5) { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "true",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "true") {
          f()
        }
      }

      benchmark.run()
    }

    runBenchmark("aggregate with string key") {
      val N = 20 << 20

      val benchmark = new Benchmark("Aggregate w string key", N, output = output)

      def f(): Unit = spark.range(N).selectExpr("id", "cast(id & 1023 as string) as k")
        .groupBy("k").count().noop()

      benchmark.addCase("codegen = F", numIters = 2) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = F", numIters = 3) { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = T", numIters = 5) { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "true",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "true") {
          f()
        }
      }

      benchmark.run()
    }

    runBenchmark("aggregate with decimal key") {
      val N = 20 << 20

      val benchmark = new Benchmark("Aggregate w decimal key", N, output = output)

      def f(): Unit = spark.range(N).selectExpr("id", "cast(id & 65535 as decimal) as k")
        .groupBy("k").count().noop()

      benchmark.addCase("codegen = F") { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = F") { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = T") { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "true",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "true") {
          f()
        }
      }

      benchmark.run()
    }

    runBenchmark("aggregate with multiple key types") {
      val N = 20 << 20

      val benchmark = new Benchmark("Aggregate w multiple keys", N, output = output)

      def f(): Unit = spark.range(N)
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
        .noop()

      benchmark.addCase("codegen = F") { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = F") { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hashmap = T") { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "true",
          SQLConf.ENABLE_VECTORIZED_HASH_MAP.key -> "true") {
          f()
        }
      }

      benchmark.run()
    }

    runBenchmark("max function bytecode size of wholestagecodegen") {
      val N = 20 << 15

      val benchmark = new Benchmark("max function bytecode size", N, output = output)

      def f(): Unit = spark.range(N)
        .selectExpr(
          "id",
          "(id & 1023) as k1",
          "cast(id & 1023 as double) as k2",
          "cast(id & 1023 as int) as k3",
          "case when id > 100 and id <= 200 then 1 else 0 end as v1",
          "case when id > 200 and id <= 300 then 1 else 0 end as v2",
          "case when id > 300 and id <= 400 then 1 else 0 end as v3",
          "case when id > 400 and id <= 500 then 1 else 0 end as v4",
          "case when id > 500 and id <= 600 then 1 else 0 end as v5",
          "case when id > 600 and id <= 700 then 1 else 0 end as v6",
          "case when id > 700 and id <= 800 then 1 else 0 end as v7",
          "case when id > 800 and id <= 900 then 1 else 0 end as v8",
          "case when id > 900 and id <= 1000 then 1 else 0 end as v9",
          "case when id > 1000 and id <= 1100 then 1 else 0 end as v10",
          "case when id > 1100 and id <= 1200 then 1 else 0 end as v11",
          "case when id > 1200 and id <= 1300 then 1 else 0 end as v12",
          "case when id > 1300 and id <= 1400 then 1 else 0 end as v13",
          "case when id > 1400 and id <= 1500 then 1 else 0 end as v14",
          "case when id > 1500 and id <= 1600 then 1 else 0 end as v15",
          "case when id > 1600 and id <= 1700 then 1 else 0 end as v16",
          "case when id > 1700 and id <= 1800 then 1 else 0 end as v17",
          "case when id > 1800 and id <= 1900 then 1 else 0 end as v18")
        .groupBy("k1", "k2", "k3")
        .sum()
        .noop()

      benchmark.addCase("codegen = F") { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f()
        }
      }

      benchmark.addCase("codegen = T hugeMethodLimit = 10000") { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key -> "10000") {
          f()
        }
      }

      benchmark.addCase("codegen = T hugeMethodLimit = 1500") { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key -> "1500") {
          f()
        }
      }

      benchmark.run()
    }


    runBenchmark("cube") {
      val N = 5 << 20

      codegenBenchmark("cube", N) {
        spark.range(N).selectExpr("id", "id % 1000 as k1", "id & 256 as k2")
          .cube("k1", "k2").sum("id").noop()
      }
    }

    runBenchmark("hash and BytesToBytesMap") {
      val N = 20 << 20

      val benchmark = new Benchmark("BytesToBytesMap", N, output = output)

      benchmark.addCase("UnsafeRowhash") { _ =>
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

      benchmark.addCase("murmur3 hash") { _ =>
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

      benchmark.addCase("fast hash") { _ =>
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

      benchmark.addCase("arrayEqual") { _ =>
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

      benchmark.addCase("Java HashMap (Long)") { _ =>
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

      benchmark.addCase("Java HashMap (two ints) ") { _ =>
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

      benchmark.addCase("Java HashMap (UnsafeRow)") { _ =>
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
        benchmark.addCase(s"LongToUnsafeRowMap (opt=$optimized)") { _ =>
          var i = 0
          val valueBytes = new Array[Byte](16)
          val value = new UnsafeRow(1)
          value.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, 16)
          value.setInt(0, 555)
          val taskMemoryManager = new TaskMemoryManager(
            new UnifiedMemoryManager(
              new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
              Long.MaxValue,
              Long.MaxValue / 2,
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
        benchmark.addCase(s"BytesToBytesMap ($heap Heap)") { _ =>
          val taskMemoryManager = new TaskMemoryManager(
            new UnifiedMemoryManager(
              new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, s"${heap == "off"}")
                .set(MEMORY_OFFHEAP_SIZE.key, "102400000"),
              Long.MaxValue,
              Long.MaxValue / 2,
              1),
            0)
          val map = new BytesToBytesMap(taskMemoryManager, 1024, 64L << 20)
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

      benchmark.addCase("Aggregate HashMap") { _ =>
        var i = 0
        val numKeys = 65536
        val schema = new StructType()
          .add("key", LongType)
          .add("value", LongType)
        val map = new AggregateHashMap(schema)
        while (i < numKeys) {
          val row = map.findOrInsert(i.toLong)
          row.setLong(1, row.getLong(1) + 1)
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
      benchmark.run()
    }
  }
}
