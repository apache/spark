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
    rang/filter/aggregate:             Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    rang/filter/aggregate codegen=false        12509.22            41.91         1.00 X
    rang/filter/aggregate codegen=true           846.38           619.45        14.78 X
    */
  }

  def testAggregateWithKey(values: Int): Unit = {

    runBenchmark("Aggregate w keys", values) {
      sqlContext.range(values).selectExpr("(id & 65535) as k").groupBy("k").sum().collect()
    }

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Aggregate with keys:               Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    Aggregate w keys codegen=false         2589.00             8.10         1.00 X
    Aggregate w keys codegen=true          1645.38            12.75         1.57 X
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
    BytesToBytesMap:                   Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    hash                                     603.61            86.86         1.00 X
    BytesToBytesMap (off Heap)              3297.39            15.90         0.18 X
    BytesToBytesMap (on Heap)               3607.09            14.53         0.17 X
      */
    benchmark.run()
  }

  test("benchmark") {
    // testWholeStage(500 << 20)
    // testAggregateWithKey(20 << 20)
    // testBytesToBytesMap(50 << 20)
  }
}
