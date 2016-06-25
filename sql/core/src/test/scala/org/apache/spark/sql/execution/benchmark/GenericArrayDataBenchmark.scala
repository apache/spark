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

import scala.concurrent.duration._

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.util.Benchmark

/**
 * Benchmark [[GenericArrayData]] for specialized representation with primitive type
 * To run this:
 *  build/sbt "sql/test-only *benchmark.GenericArrayDataBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class GenericArrayDataBenchmark extends BenchmarkBase {

  def allocateGenericIntArray(iters: Int): Unit = {
    val count = 1024 * 1024
    var array: GenericArrayData = null

    val primitiveIntArray = new Array[Int](count)
    val specializedIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = GenericArrayData.allocate(primitiveIntArray)
        n += 1
      }
    }
    val genericIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = new GenericRefArrayData(primitiveIntArray)
        n += 1
      }
    }

    val benchmark = new Benchmark("Allocate GenericArrayData for int", count * iters,
      minNumIters = 10, minTime = 1.milliseconds)
    benchmark.addCase("Generic    ")(genericIntArray)
    benchmark.addCase("Specialized")(specializedIntArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Allocate GenericArrayData for int:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                         40 /   43        522.2           1.9       1.0X
    Specialized                                      0 /    0  209715200.0           0.0  401598.7X
    */
  }

  def allocateGenericDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024
    var array: GenericArrayData = null

    val primitiveDoubleArray = new Array[Int](count)
    val specializedDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = GenericArrayData.allocate(primitiveDoubleArray)
        n += 1
      }
    }
    val genericDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = new GenericRefArrayData(primitiveDoubleArray)
        n += 1
      }
    }

    val benchmark = new Benchmark("Allocate GenericArrayData for double", count * iters,
      minNumIters = 10, minTime = 1.milliseconds)
    benchmark.addCase("Generic    ")(genericDoubleArray)
    benchmark.addCase("Specialized")(specializedDoubleArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Allocate GenericArrayData for double:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                         40 /   44        523.2           1.9       1.0X
    Specialized                                      0 /    0  225500215.1           0.0  431013.0X
    */
  }

  def getPrimitiveIntArray(iters: Int): Unit = {
    val count = 1024 * 1024

    val intSparseArray: GenericArrayData = new GenericRefArrayData(new Array[Int](count))
    val intDenseArray: GenericArrayData = GenericArrayData.allocate(new Array[Int](count))
    var primitiveIntArray: Array[Int] = null
    val genericIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveIntArray = intSparseArray.toIntArray
        n += 1
      }
    }
    val denseIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveIntArray = intDenseArray.toIntArray
        n += 1
      }
    }

    val benchmark = new Benchmark("Get int primitive array", count * iters)
    benchmark.addCase("Generic    ")(genericIntArray)
    benchmark.addCase("Specialized")(denseIntArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Get int primitive array:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                         67 /   70        783.9           1.3       1.0X
    Specialized                                     41 /   43       1263.8           0.8       1.6X
    */
  }

  def getPrimitiveDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024

    val doubleSparseArray: GenericArrayData = new GenericRefArrayData(new Array[Double](count))
    val doubleDenseArray: GenericArrayData = GenericArrayData.allocate(new Array[Double](count))
    var primitiveDoubleArray: Array[Double] = null
    val genericDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveDoubleArray = doubleSparseArray.toDoubleArray
        n += 1
      }
    }
    val specializedDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveDoubleArray = doubleDenseArray.toDoubleArray
        n += 1
      }
    }

    val benchmark = new Benchmark("Get double primitive array", count * iters)
    benchmark.addCase("Generic    ")(genericDoubleArray)
    benchmark.addCase("Specialized")(specializedDoubleArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Get double primitive array:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                        211 /  217        248.6           4.0       1.0X
    Specialized                                     95 /  100        554.1           1.8       2.2X
    */
  }

  def readGenericIntArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 2
    var result: Int = 0

    val sparseArray = new GenericRefArrayData(new Array[Int](count))
    val genericIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = sparseArray.numElements
        var sum = 0
        var i = 0
        while (i < len) {
          sum += sparseArray.getInt(i)
          i += 1
        }
        result = sum
        n += 1
      }
    }

    val denseArray = GenericArrayData.allocate(new Array[Int](count))
    val denseIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = denseArray.numElements
        var sum = 0
        var i = 0
        while (i < len) {
          sum += denseArray.getInt(i)
          i += 1
        }
        result = sum
        n += 1
      }
    }

    val benchmark = new Benchmark("Read GenericArrayData Int", count * iters)
    benchmark.addCase("Sparse")(genericIntArray)
    benchmark.addCase("Dense ")(denseIntArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read GenericArrayData Int:               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                        160 /  163       1314.5           0.8       1.0X
    Specialized                                     68 /   69       3080.0           0.3       2.3X
    */
  }

  def readGenericDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 2
    var result: Double = 0

    val sparseArray = new GenericRefArrayData(new Array[Double](count))
    val genericDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = sparseArray.numElements
        var sum = 0.toDouble
        var i = 0
        while (i < len) {
          sum += sparseArray.getDouble(i)
          i += 1
        }
        result = sum
        n += 1
      }
    }

    val denseArray = GenericArrayData.allocate(new Array[Double](count))
    val specializedDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = denseArray.numElements
        var sum = 0.toDouble
        var i = 0
        while (i < len) {
          sum += denseArray.getDouble(i)
          i += 1
        }
        result = sum
        n += 1
      }
    }

    val benchmark = new Benchmark("Read GenericArrayData Double", count * iters)
    benchmark.addCase("Generic")(genericDoubleArray)
    benchmark.addCase("Specialized")(specializedDoubleArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read GenericArrayData Double:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                        611 /  613        343.3           2.9       1.0X
    Specialized                                    199 /  202       1051.5           1.0       3.1X
    */
  }

  test("allocate GenericArrayData") {
    allocateGenericIntArray(20)
    allocateGenericDoubleArray(20)
  }

  test("get primitive array") {
    getPrimitiveIntArray(50)
    getPrimitiveDoubleArray(50)
  }

  test("read elements in GenericArrayData") {
    readGenericIntArray(100)
    readGenericDoubleArray(100)
  }

  def main(args: Array[String]): Unit = {
    allocateGenericIntArray(20)
    allocateGenericDoubleArray(20)
    getPrimitiveIntArray(50)
    getPrimitiveDoubleArray(50)
    readGenericIntArray(20)
    readGenericDoubleArray(20)
  }
}
