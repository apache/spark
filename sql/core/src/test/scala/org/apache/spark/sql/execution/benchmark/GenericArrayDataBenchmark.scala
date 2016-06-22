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
 * Benchmark [[GenericArrayData]] for Dense and Sparse with primitive type
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
    val denseIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = GenericArrayData.allocate(primitiveIntArray)
        n += 1
      }
    }
    val sparseIntArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = new GenericRefArrayData(primitiveIntArray)
        n += 1
      }
    }

    val benchmark = new Benchmark("Allocate GenericArrayData for int", count * iters,
      minNumIters = 10, minTime = 1.milliseconds)
    benchmark.addCase("Sparse")(sparseIntArray)
    benchmark.addCase("Dense ")(denseIntArray)
    benchmark.run
  }

  def allocateGenericDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024
    var array: GenericArrayData = null

    val primitiveDoubleArray = new Array[Int](count)
    val denseDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = GenericArrayData.allocate(primitiveDoubleArray)
        n += 1
      }
    }
    val sparseDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        array = new GenericRefArrayData(primitiveDoubleArray)
        n += 1
      }
    }

    val benchmark = new Benchmark("Allocate GenericArrayData for double", count * iters,
      minNumIters = 10, minTime = 1.milliseconds)
    benchmark.addCase("Sparse")(sparseDoubleArray)
    benchmark.addCase("Dense ")(denseDoubleArray)
    benchmark.run
  }

  def getPrimitiveIntArray(iters: Int): Unit = {
    val count = 1024 * 1024

    val intSparseArray: GenericArrayData = new GenericRefArrayData(new Array[Int](count))
    val intDenseArray: GenericArrayData = GenericArrayData.allocate(new Array[Int](count))
    var primitiveIntArray: Array[Int] = null
    val sparseIntArray = { i: Int =>
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
    benchmark.addCase("Sparse int")(sparseIntArray)
    benchmark.addCase("Dense  int")(denseIntArray)
    benchmark.run
  }

  def getPrimitiveDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024

    val doubleSparseArray: GenericArrayData = new GenericRefArrayData(new Array[Double](count))
    val doubleDenseArray: GenericArrayData = GenericArrayData.allocate(new Array[Double](count))
    var primitiveDoubleArray: Array[Double] = null
    val sparseDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveDoubleArray = doubleSparseArray.toDoubleArray
        n += 1
      }
    }
    val denseDoubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        primitiveDoubleArray = doubleDenseArray.toDoubleArray
        n += 1
      }
    }

    val benchmark = new Benchmark("Get double primitive array", count * iters)
    benchmark.addCase("Sparse double")(sparseDoubleArray)
    benchmark.addCase("Dense  double")(denseDoubleArray)
    benchmark.run
  }

  def readGenericIntArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 2
    var result: Int = 0

    val sparseArray = new GenericRefArrayData(new Array[Int](count))
    val sparseIntArray = { i: Int =>
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
    benchmark.addCase("Sparse")(sparseIntArray)
    benchmark.addCase("Dense ")(denseIntArray)
    benchmark.run
  }

  def readGenericDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 2
    var result: Double = 0

    val sparseArray = new GenericRefArrayData(new Array[Double](count))
    val sparseDoubleArray = { i: Int =>
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
    val denseDoubleArray = { i: Int =>
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
    benchmark.addCase("Sparse")(sparseDoubleArray)
    benchmark.addCase("Dense ")(denseDoubleArray)
    benchmark.run
  }

  ignore("allocate GenericArrayData") {
    allocateGenericIntArray(20)
    allocateGenericDoubleArray(20)
  }

  ignore("get primitive array") {
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
