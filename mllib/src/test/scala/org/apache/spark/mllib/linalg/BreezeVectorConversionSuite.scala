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

package org.apache.spark.mllib.linalg

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}

import org.apache.spark.SparkFunSuite

/**
 * Test Breeze vector conversions.
 */
class BreezeVectorConversionSuite extends SparkFunSuite {

  val arr = Array(0.1, 0.2, 0.3, 0.4)
  val n = 20
  val indices = Array(0, 3, 5, 10, 13)
  val values = Array(0.1, 0.5, 0.3, -0.8, -1.0)

  test("dense to breeze") {
    val vec = Vectors.dense(arr)
    assert(vec.toBreeze === new BDV[Double](arr))
  }

  test("sparse to breeze") {
    val vec = Vectors.sparse(n, indices, values)
    assert(vec.toBreeze === new BSV[Double](indices, values, n))
  }

  test("dense breeze to vector") {
    val breeze = new BDV[Double](arr)
    val vec = Vectors.fromBreeze(breeze).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr), "should not copy data")
  }

  test("sparse breeze to vector") {
    val breeze = new BSV[Double](indices, values, n)
    val vec = Vectors.fromBreeze(breeze).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices.eq(indices), "should not copy data")
    assert(vec.values.eq(values), "should not copy data")
  }

  test("sparse breeze with partially-used arrays to vector") {
    val activeSize = 3
    val breeze = new BSV[Double](indices, values, activeSize, n)
    val vec = Vectors.fromBreeze(breeze).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices === indices.slice(0, activeSize))
    assert(vec.values === values.slice(0, activeSize))
  }
}
