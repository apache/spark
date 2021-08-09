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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData

/**
 * Benchmark [[UnsafeArrayDataBenchmark]] for UnsafeArrayData
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/UnsafeArrayDataBenchmark-results.txt".
 * }}}
 */
object UnsafeArrayDataBenchmark extends BenchmarkBase {

  def calculateHeaderPortionInBytes(count: Int) : Int = {
    /* 4 + 4 * count // Use this expression for SPARK-15962 */
    UnsafeArrayData.calculateHeaderPortionInBytes(count)
  }

  private lazy val intEncoder = ExpressionEncoder[Array[Int]]().resolveAndBind()

  private lazy val doubleEncoder = ExpressionEncoder[Array[Double]]().resolveAndBind()

  def readUnsafeArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 16
    val rand = new Random(42)
    val intArrayToRow = intEncoder.createSerializer()
    val intPrimitiveArray = Array.fill[Int](count) { rand.nextInt }
    val intUnsafeArray = intArrayToRow(intPrimitiveArray).getArray(0)
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
    val doubleArrayToRow = doubleEncoder.createSerializer()
    val doubleUnsafeArray = doubleArrayToRow(doublePrimitiveArray).getArray(0)
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

    val benchmark = new Benchmark("Read UnsafeArrayData", count * iters, output = output)
    benchmark.addCase("Int")(readIntArray)
    benchmark.addCase("Double")(readDoubleArray)
    benchmark.run
  }

  def writeUnsafeArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 2
    val rand = new Random(42)

    var intTotalLength: Int = 0
    val intPrimitiveArray = Array.fill[Int](count) { rand.nextInt }
    val intArrayToRow = intEncoder.createSerializer()
    val writeIntArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += intArrayToRow(intPrimitiveArray).getArray(0).numElements()
        n += 1
      }
      intTotalLength = len
    }

    var doubleTotalLength: Int = 0
    val doublePrimitiveArray = Array.fill[Double](count) { rand.nextDouble }
    val doubleArrayToRow = doubleEncoder.createSerializer()
    val writeDoubleArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += doubleArrayToRow(doublePrimitiveArray).getArray(0).numElements()
        n += 1
      }
      doubleTotalLength = len
    }

    val benchmark = new Benchmark("Write UnsafeArrayData", count * iters, output = output)
    benchmark.addCase("Int")(writeIntArray)
    benchmark.addCase("Double")(writeDoubleArray)
    benchmark.run
  }

  def getPrimitiveArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 12
    val rand = new Random(42)

    var intTotalLength: Int = 0
    val intPrimitiveArray = Array.fill[Int](count) { rand.nextInt }
    val intArrayToRow = intEncoder.createSerializer()
    val intUnsafeArray = intArrayToRow(intPrimitiveArray).getArray(0)
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
    val doubleArrayToRow = doubleEncoder.createSerializer()
    val doubleUnsafeArray = doubleArrayToRow(doublePrimitiveArray).getArray(0)
    val readDoubleArray = { i: Int =>
      var len = 0
      var n = 0
      while (n < iters) {
        len += doubleUnsafeArray.toDoubleArray.length
        n += 1
      }
      doubleTotalLength = len
    }

    val benchmark =
      new Benchmark("Get primitive array from UnsafeArrayData", count * iters, output = output)
    benchmark.addCase("Int")(readIntArray)
    benchmark.addCase("Double")(readDoubleArray)
    benchmark.run
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

    val benchmark =
      new Benchmark("Create UnsafeArrayData from primitive array", count * iters, output = output)
    benchmark.addCase("Int")(createIntArray)
    benchmark.addCase("Double")(createDoubleArray)
    benchmark.run
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Benchmark UnsafeArrayData") {
      readUnsafeArray(10)
      writeUnsafeArray(10)
      getPrimitiveArray(5)
      putPrimitiveArray(5)
    }
  }
}
