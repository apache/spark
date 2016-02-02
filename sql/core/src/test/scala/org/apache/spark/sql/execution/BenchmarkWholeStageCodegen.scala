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

  def testWholeStage(values: Int): Unit = {
    val benchmark = new Benchmark("rang/filter/aggregate", values)

    benchmark.addCase("Without codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).filter("(id & 1) = 1").groupBy().sum().collect()
    }

    benchmark.addCase("With codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).filter("(id & 1) = 1").groupBy().sum().collect()
    }

    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      rang/filter/aggregate:             Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------------
      Without codegen                         5488.16            38.21         1.00 X
      With codegen                             531.08           394.88        10.33 X
    */
    benchmark.run()
  }

  def testAggregateWithKey(values: Int): Unit = {
    val benchmark = new Benchmark("Aggregate with keys", values)

    benchmark.addCase("Aggregate w/o codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).selectExpr("(id & 65535) as k").groupBy("k").sum().collect()
    }
    benchmark.addCase(s"Aggregate w codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).selectExpr("(id & 65535) as k").groupBy("k").sum().collect()
    }

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Aggregate with keys:               Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    Aggregate w/o codegen                   3739.83             5.61         1.00 X
    Aggregate w codegen                     1602.70            13.09         2.33 X
    */
    benchmark.run()
  }

  def testBytesToBytesMap(values: Int): Unit = {
    val benchmark = new Benchmark("BytesToBytesMap", values)

    benchmark.addCase("hash") { iter =>
      var i = 0
      val keyBytes = new Array[Byte](16)
      val key = new UnsafeRow(1)
      key.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, 16)
      var s = 0
      while (i < values) {
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
      while (i < values) {
        key.setInt(0, i % 1000)
        val h = i % 1000
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
      while (i < values) {
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
        while (i < values) {
          key.setInt(0, i % 65536)
          val loc = map.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes, i % 65536)
          if (loc.isDefined) {
            value.pointTo(loc.getValueBase, loc.getValueOffset,
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
    BytesToBytesMap:                   Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    hash                                     637.85            82.20         1.00 X
    fast hash                                148.80           352.35         4.29 X
    arrayEqual                               408.46           128.36         1.56 X
    BytesToBytesMap (off Heap)              1209.64            43.34         0.53 X
    BytesToBytesMap (on Heap)               1322.56            39.64         0.48 X
      */
    benchmark.run()
  }

  test("benchmark") {
    // testWholeStage(200 << 20)
    // testAggregateWithKey(20 << 20)
    // testBytesToBytesMap(50 << 20)
  }
}
