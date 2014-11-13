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

package org.apache.spark.mllib.linalg.distributed

import org.scalatest.FunSuite

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.linalg.Vectors

class CoordinateMatrixSuite extends FunSuite with MLlibTestSparkContext {

  val m = 5
  val n = 4
  var mat: CoordinateMatrix = _

  override def beforeAll() {
    super.beforeAll()
    val entries = sc.parallelize(Seq(
      (0, 0, 1.0),
      (0, 1, 2.0),
      (1, 1, 3.0),
      (1, 2, 4.0),
      (2, 2, 5.0),
      (2, 3, 6.0),
      (3, 0, 7.0),
      (3, 3, 8.0),
      (4, 1, 9.0)), 3).map { case (i, j, value) =>
      MatrixEntry(i, j, value)
    }
    mat = new CoordinateMatrix(entries)
  }

  test("size") {
    assert(mat.numRows() === m)
    assert(mat.numCols() === n)
  }

  test("empty entries") {
    val entries = sc.parallelize(Seq[MatrixEntry](), 1)
    val emptyMat = new CoordinateMatrix(entries)
    intercept[RuntimeException] {
      emptyMat.numCols()
    }
    intercept[RuntimeException] {
      emptyMat.numRows()
    }
  }

  test("toBreeze") {
    val expected = BDM(
      (1.0, 2.0, 0.0, 0.0),
      (0.0, 3.0, 4.0, 0.0),
      (0.0, 0.0, 5.0, 6.0),
      (7.0, 0.0, 0.0, 8.0),
      (0.0, 9.0, 0.0, 0.0))
    assert(mat.toBreeze() === expected)
  }

  test("toIndexedRowMatrix") {
    val indexedRowMatrix = mat.toIndexedRowMatrix()
    val expected = BDM(
      (1.0, 2.0, 0.0, 0.0),
      (0.0, 3.0, 4.0, 0.0),
      (0.0, 0.0, 5.0, 6.0),
      (7.0, 0.0, 0.0, 8.0),
      (0.0, 9.0, 0.0, 0.0))
    assert(indexedRowMatrix.toBreeze() === expected)
  }

  test("toRowMatrix") {
    val rowMatrix = mat.toRowMatrix()
    val rows = rowMatrix.rows.collect().toSet
    val expected = Set(
      Vectors.dense(1.0, 2.0, 0.0, 0.0),
      Vectors.dense(0.0, 3.0, 4.0, 0.0),
      Vectors.dense(0.0, 0.0, 5.0, 6.0),
      Vectors.dense(7.0, 0.0, 0.0, 8.0),
      Vectors.dense(0.0, 9.0, 0.0, 0.0))
    assert(rows === expected)
  }
}
