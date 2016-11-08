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
    Generic                                        277 /  366        605.0           1.7       1.0X
    Specialized                                    214 /  251        785.1           1.3       1.3X
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
    Generic                                       1976 / 1991         84.9          11.8       1.0X
    Specialized                                    589 / 1050        285.1           3.5       3.4X
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
    Generic                                        208 /  214       1008.3           1.0       1.0X
    Specialized                                    142 /  158       1471.7           0.7       1.5X
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
    Generic                                        621 /  683        337.7           3.0       1.0X
    Specialized                                    265 /  297        790.4           1.3       2.3X
    */
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
