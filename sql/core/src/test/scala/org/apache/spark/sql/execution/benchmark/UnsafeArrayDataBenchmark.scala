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

import scala.util.Random

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeArrayWriter}
import org.apache.spark.util.Benchmark

/**
 * Benchmark [[UnsafeArrayDataBenchmark]] for UnsafeArrayData
 * To run this:
 *  1. replace ignore(...) with test(...)
 *  2. build/sbt "sql/test-only *benchmark.UnsafeArrayDataBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class UnsafeArrayDataBenchmark extends BenchmarkBase {

  def calculateHeaderPortionInBytes(count: Int) : Int = {
    /* 4 + 4 * count // Use this expression for SPARK-15962 */
    UnsafeArrayData.calculateHeaderPortionInBytes(count)
  }

  def readUnsafeArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 16
    val rand = new Random(42)

    val intPrimitiveArray = Array.fill[Int](count) { rand.nextInt }
    val intEncoder = ExpressionEncoder[Array[Int]].resolveAndBind()
    val intUnsafeArray = intEncoder.toRow(intPrimitiveArray).getArray(0)
    val readIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = intUnsafeArray.numElements
        var sum = 0
        var i = 0
        while (i < len) {
          sum += intUnsafeArray.getInt(i)
          i += 1
        }
        n += 1
      }
    }

    val doublePrimitiveArray = Array.fill[Double](count) { rand.nextDouble }
    val doubleEncoder = ExpressionEncoder[Array[Double]].resolveAndBind()
    val doubleUnsafeArray = doubleEncoder.toRow(doublePrimitiveArray).getArray(0)
    val readDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = doubleUnsafeArray.numElements
        var sum = 0.0
        var i = 0
        while (i < len) {
          sum += doubleUnsafeArray.getDouble(i)
          i += 1
        }
        n += 1
      }
    }

    val benchmark = new Benchmark("Read UnsafeArrayData", count * iters)
    benchmark.addCase("Int")(readIntArray)
    benchmark.addCase("Double")(readDoubleArray)
    benchmark.run
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.10.4
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz

    Read UnsafeArrayData:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Int                                            279 /  294        600.4           1.7       1.0X
    Double                                         296 /  303        567.0           1.8       0.9X
    */
  }

  def writeUnsafeArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 16
    val rand = new Random(42)

    var intTotalLength: Int = 0
    val intPrimitiveArray = Array.fill[Int](count) { rand.nextInt }
    val intEncoder = ExpressionEncoder[Array[Int]].resolveAndBind()
    val writeIntArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += intEncoder.toRow(intPrimitiveArray).getArray(0).numElements()
        n += 1
      }
      intTotalLength = len
    }

    var doubleTotalLength: Int = 0
    val doublePrimitiveArray = Array.fill[Double](count) { rand.nextDouble }
    val doubleEncoder = ExpressionEncoder[Array[Double]].resolveAndBind()
    val writeDoubleArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += doubleEncoder.toRow(doublePrimitiveArray).getArray(0).numElements()
        n += 1
      }
      doubleTotalLength = len
    }

    val benchmark = new Benchmark("Write UnsafeArrayData", count * iters)
    benchmark.addCase("Int")(writeIntArray)
    benchmark.addCase("Double")(writeDoubleArray)
    benchmark.run
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.10.4
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz

    Write UnsafeArrayData:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Int                                            135 /  180        154.9           6.5       1.0X
    Double                                         195 /  300        107.8           9.3       0.7X
    */
  }

  def getPrimitiveArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 12
    val rand = new Random(42)

    var intTotalLength: Int = 0
    val intPrimitiveArray = Array.fill[Int](count) { rand.nextInt }
    val intEncoder = ExpressionEncoder[Array[Int]].resolveAndBind()
    val intUnsafeArray = intEncoder.toRow(intPrimitiveArray).getArray(0)
    val readIntArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += intUnsafeArray.toIntArray.length
        n += 1
      }
      intTotalLength = len
    }

    var doubleTotalLength: Int = 0
    val doublePrimitiveArray = Array.fill[Double](count) { rand.nextDouble }
    val doubleEncoder = ExpressionEncoder[Array[Double]].resolveAndBind()
    val doubleUnsafeArray = doubleEncoder.toRow(doublePrimitiveArray).getArray(0)
    val readDoubleArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += doubleUnsafeArray.toDoubleArray.length
        n += 1
      }
      doubleTotalLength = len
    }

    val benchmark = new Benchmark("Get primitive array from UnsafeArrayData", count * iters)
    benchmark.addCase("Int")(readIntArray)
    benchmark.addCase("Double")(readDoubleArray)
    benchmark.run
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.10.4
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz

    Get primitive array from UnsafeArrayData: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)  Relative
    ------------------------------------------------------------------------------------------------
    Int                                             80 /  151        783.4           1.3       1.0X
    Double                                         208 /  366        302.8           3.3       0.4X
    */
  }

  def putPrimitiveArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 12
    val rand = new Random(42)

    var intTotalLen: Int = 0
    val intPrimitiveArray = Array.fill[Int](count) { rand.nextInt }
    val createIntArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += UnsafeArrayData.fromPrimitiveArray(intPrimitiveArray).numElements
        n += 1
      }
      intTotalLen = len
    }

    var doubleTotalLen: Int = 0
    val doublePrimitiveArray = Array.fill[Double](count) { rand.nextDouble }
    val createDoubleArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += UnsafeArrayData.fromPrimitiveArray(doublePrimitiveArray).numElements
        n += 1
      }
      doubleTotalLen = len
    }

    val benchmark = new Benchmark("Create UnsafeArrayData from primitive array", count * iters)
    benchmark.addCase("Int")(createIntArray)
    benchmark.addCase("Double")(createDoubleArray)
    benchmark.run
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.10.4
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz

    Create UnsafeArrayData from primitive array: Best/Avg Time(ms)  Rate(M/s)  Per Row(ns)  Relative
    ------------------------------------------------------------------------------------------------
    Int                                             68 /  144        920.4           1.1       1.0X
    Double                                         240 /  302        261.7           3.8       0.3X
    */
  }

  ignore("Benchmark UnsafeArrayData") {
    readUnsafeArray(10)
    writeUnsafeArray(10)
    getPrimitiveArray(5)
    putPrimitiveArray(5)
  }
}
