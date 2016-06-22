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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.util.Benchmark

/**
 * Benchmark [[GenericArrayData]] for Dense and Sparse with primitive type
 */
object GenericArrayDataBenchmark {
/*
  def allocateGenericIntArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 10
    var array: GenericArrayData = null

    val primitiveIntArray = new Array[Int](count)
    val denseIntArray = { i: Int =>
      for (n <- 0L until iters) {
        array = GenericArrayData.allocate(primitiveIntArray)
      }
    }
    val sparseIntArray = { i: Int =>
      for (n <- 0L until iters) {
        array = new GenericRefArrayData(primitiveIntArray)
      }
    }

    val benchmark = new Benchmark("Allocate GenericArrayData for int", count * iters)
    benchmark.addCase("Sparse")(sparseIntArray)
    benchmark.addCase("Dense ")(denseIntArray)
  }

  def allocateGenericDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 10
    var array: GenericArrayData = null

    val primitiveDoubleArray = new Array[Int](count)
    val denseDoubleArray = { i: Int =>
      for (n <- 0L until iters) {
        array = GenericArrayData.allocate(primitiveDoubleArray)
      }
    }
    val sparseDoubleArray = { i: Int =>
      for (n <- 0L until iters) {
        array = new GenericRefArrayData(primitiveDoubleArray)
      }
    }

    val benchmark = new Benchmark("Allocate GenericArrayData for double", count * iters)
    benchmark.addCase("Sparse")(sparseDoubleArray)
    benchmark.addCase("Dense ")(denseDoubleArray)
  }

  def getPrimitiveIntArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 10

    val intSparseArray: GenericArrayData = new GenericRefArrayData(new Array[Int](count))
    val intDenseArray: GenericArrayData = GenericArrayData.allocate(new Array[Int](count))
    var primitiveIntArray: Array[Int] = null
    val sparseIntArray = { i: Int =>
      for (n <- 0L until iters) {
        primitiveIntArray = intSparseArray.toIntArray
      }
    }
    val denseIntArray = { i: Int =>
      for (n <- 0L until iters) {
        primitiveIntArray = intDenseArray.toIntArray
      }
    }

    val benchmark = new Benchmark("Get int primitive array", count * iters)
    benchmark.addCase("Sparse int")(sparseIntArray)
    benchmark.addCase("Dense  int")(denseIntArray)
  }

  def getPrimitiveDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 10

    val doubleSparseArray: GenericArrayData = new GenericRefArrayData(new Array[Double](count))
    val doubleDenseArray: GenericArrayData = GenericArrayData.allocate(new Array[Double](count))
    var primitiveDoubleArray: Array[Double] = null
    val sparseDoubleArray = { i: Int =>
      for (n <- 0L until iters) {
        primitiveDoubleArray = doubleSparseArray.toDoubleArray
      }
    }
    val denseDoubleArray = { i: Int =>
      for (n <- 0L until iters) {
        primitiveDoubleArray = doubleDenseArray.toDoubleArray
      }
    }

    val benchmark = new Benchmark("Get double primitive array", count * iters)
    benchmark.addCase("Sparse double")(sparseDoubleArray)
    benchmark.addCase("Dense  double")(denseDoubleArray)
  }

  def readGenericIntArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 10
    var result: Int = 0

    val sparseArray = new GenericRefArrayData(new Array[Int](count))
    val sparseIntArray = { i: Int =>
      for (n <- 0L until iters) {
        val len = sparseArray.numElements
        var sum = 0
        for (i <- 0 until len - 1) {
          sum += sparseArray.getInt(i)
        }
        result = sum
      }
    }

    val denseArray = GenericArrayData.allocate(new Array[Int](count))
    val denseIntArray = { i: Int =>
      for (n <- 0L until iters) {
        val len = denseArray.numElements
        var sum = 0
        for (i <- 0 until len - 1) {
          sum += denseArray.getInt(i)
        }
        result = sum
      }
    }

    val benchmark = new Benchmark("Read GenericArrayData Int", count * iters)
    benchmark.addCase("Sparse")(sparseIntArray)
    benchmark.addCase("Dense ")(denseIntArray)
  }

  def readGenericDoubleArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 10
    var result: Int = 0

    val sparseArray = new GenericRefArrayData(new Array[Int](count))
    val sparseIntArray = { i: Int =>
      for (n <- 0L until iters) {
        val len = sparseArray.numElements
        var sum = 0
        for (i <- 0 until len - 1) {
          sum += sparseArray.getInt(i)
        }
        result = sum
      }
    }

    val denseArray = GenericArrayData.allocate(new Array[Int](count))
    val denseIntArray = { i: Int =>
      for (n <- 0L until iters) {
        val len = denseArray.numElements
        var sum = 0
        for (i <- 0 until len - 1) {
          sum += denseArray.getInt(i)
        }
        result = sum
      }
    }

    val benchmark = new Benchmark("Read GenericArrayData Double", count * iters)
    benchmark.addCase("Sparse")(sparseIntArray)
    benchmark.addCase("Dense ")(denseIntArray)
  }

  def main(args: Array[String]): Unit = {
    allocateGenericIntArray(1024)
    allocateGenericDoubleArray(1024)
    getPrimitiveIntArray(1024)
    getPrimitiveDoubleArray(1024)
    readGenericIntArray(512)
    readGenericDoubleArray(512)
  }
*/
}
