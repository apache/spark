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
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read UnsafeArrayData:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Int                                            252 /  260        666.1           1.5       1.0X
    Double                                         281 /  292        597.7           1.7       0.9X
    */
  }

  def writeUnsafeArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 2
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
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Write UnsafeArrayData:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Int                                            196 /  249        107.0           9.3       1.0X
    Double                                         227 /  367         92.3          10.8       0.9X
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
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Get primitive array from UnsafeArrayData: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)  Relative
    ------------------------------------------------------------------------------------------------
    Int                                            151 /  198        415.8           2.4       1.0X
    Double                                         214 /  394        293.6           3.4       0.7X
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
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Create UnsafeArrayData from primitive array: Best/Avg Time(ms) Rate(M/s)   Per Row(ns)  Relative
    ------------------------------------------------------------------------------------------------
    Int                                            206 /  211        306.0           3.3       1.0X
    Double                                         232 /  406        271.6           3.7       0.9X
    */
  }

  ignore("Benchmark UnsafeArrayData") {
    readUnsafeArray(10)
    writeUnsafeArray(10)
    getPrimitiveArray(5)
    putPrimitiveArray(5)
  }
}
