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
 *  1. replace ignore(...) with test(...)
 *  2. build/sbt "sql/test-only *benchmark.GenericArrayDataBenchmark"
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
        array = new GenericArrayData(primitiveIntArray)
        n += 1
      }
    }
    val anyArray = primitiveIntArray.toArray[Any]
    val genericIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = new GenericArrayData(anyArray)
        n += 1
      }
    }

    val benchmark = new Benchmark("Allocate GenericArrayData for int", count * iters,
      minNumIters = 10, minTime = 1.milliseconds)
    benchmark.addCase("Generic    ")(genericIntArray)
    benchmark.addCase("Specialized")(specializedIntArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Allocate GenericArrayData for int:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                          0 /    0   46500044.3           0.0       1.0X
    Specialized                                      0 /    0  170500162.6           0.0       3.7X
    */
  }

  def allocateGenericDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024
    var array: GenericArrayData = null

    val primitiveDoubleArray = new Array[Int](count)
    val specializedDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = new GenericArrayData(primitiveDoubleArray)
        n += 1
      }
    }
    val anyArray = primitiveDoubleArray.toArray[Any]
    val genericDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = new GenericArrayData(anyArray)
        n += 1
      }
    }

    val benchmark = new Benchmark("Allocate GenericArrayData for double", count * iters,
      minNumIters = 10, minTime = 1.milliseconds)
    benchmark.addCase("Generic    ")(genericDoubleArray)
    benchmark.addCase("Specialized")(specializedDoubleArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Allocate GenericArrayData for double:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                          0 /    0   55627374.0           0.0       1.0X
    Specialized                                      0 /    0  177724745.8           0.0       3.2X
    */
  }

  def getPrimitiveIntArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 8

    val anyArray: GenericArrayData = new GenericArrayData(new Array[Int](count).toArray[Any])
    val intArray: GenericArrayData = new GenericArrayData(new Array[Int](count))
    var primitiveIntArray: Array[Int] = null
    val genericIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveIntArray = anyArray.toIntArray
        n += 1
      }
    }
    val specializedIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveIntArray = intArray.toIntArray
        n += 1
      }
    }

    val benchmark = new Benchmark("Get int primitive array", count * iters)
    benchmark.addCase("Generic")(genericIntArray)
    benchmark.addCase("Specialized")(specializedIntArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Get int primitive array:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                        334 /  382        502.4           2.0       1.0X
    Specialized                                    282 /  314        595.4           1.7       1.2X
    */
  }

  def getPrimitiveDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 8

    val anyArray: GenericArrayData = new GenericArrayData(new Array[Double](count).toArray[Any])
    val doubleArray: GenericArrayData = new GenericArrayData(new Array[Double](count))
    var primitiveDoubleArray: Array[Double] = null
    val genericDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveDoubleArray = anyArray.toDoubleArray
        n += 1
      }
    }
    val specializedDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveDoubleArray = doubleArray.toDoubleArray
        n += 1
      }
    }

    val benchmark = new Benchmark("Get double primitive array", count * iters)
    benchmark.addCase("Generic")(genericDoubleArray)
    benchmark.addCase("Specialized")(specializedDoubleArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Get double primitive array:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                       1720 / 1883         97.6          10.3       1.0X
    Specialized                                    703 / 1117        238.7           4.2       2.4X
    */
  }

  def readGenericIntArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 8
    var result: Int = 0

    val anyArray = new GenericArrayData(new Array[Int](count).toArray[Any])
    val genericIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = anyArray.numElements
        var sum = 0
        var i = 0
        while (i < len) {
          sum += anyArray.getInt(i)
          i += 1
        }
        result = sum
        n += 1
      }
    }

    val intArray = new GenericArrayData(new Array[Int](count))
    val specializedIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = intArray.numElements
        var sum = 0
        var i = 0
        while (i < len) {
          sum += intArray.getInt(i)
          i += 1
        }
        result = sum
        n += 1
      }
    }

    val benchmark = new Benchmark("Read GenericArrayData Int", count * iters)
    benchmark.addCase("Generic")(genericIntArray)
    benchmark.addCase("Specialized")(specializedIntArray)
    benchmark.run
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read GenericArrayData Int:               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                        206 /  212       1017.6           1.0       1.0X
    Specialized                                    161 /  167       1301.0           0.8       1.3X
    */
  }

  def readGenericDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 8
    var result: Double = 0

    val anyArray = new GenericArrayData(new Array[Double](count).toArray[Any])
    val genericDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = anyArray.numElements
        var sum = 0.toDouble
        var i = 0
        while (i < len) {
          sum += anyArray.getDouble(i)
          i += 1
        }
        result = sum
        n += 1
      }
    }

    val doubleArray = new GenericArrayData(new Array[Double](count))
    val specializedDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        val len = doubleArray.numElements
        var sum = 0.toDouble
        var i = 0
        while (i < len) {
          sum += doubleArray.getDouble(i)
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
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read GenericArrayData Double:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Generic                                        547 /  581        383.3           2.6       1.0X
    Specialized                                    237 /  260        884.0           1.1       2.3X
    */
  }

  ignore("allocate GenericArrayData") {
    allocateGenericIntArray(20)
    allocateGenericDoubleArray(20)
  }

  ignore("get primitive array") {
    getPrimitiveIntArray(20)
    getPrimitiveDoubleArray(20)
  }

  ignore("read elements in GenericArrayData") {
    readGenericIntArray(25)
    readGenericDoubleArray(25)
  }
}
