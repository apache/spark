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

import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Matrix}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class BlockMatrixSuite extends FunSuite with MLlibTestSparkContext {

  val m = 5
  val n = 4
  val rowPerPart = 2
  val colPerPart = 2
  val numRowBlocks = 3
  val numColBlocks = 2
  var gridBasedMat: BlockMatrix = _
  type SubMatrix = ((Int, Int), Matrix)

  override def beforeAll() {
    super.beforeAll()

    val entries: Seq[SubMatrix] = Seq(
      new SubMatrix((0, 0), new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 2.0))),
      new SubMatrix((0, 1), new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 0.0))),
      new SubMatrix((1, 0), new DenseMatrix(2, 2, Array(3.0, 0.0, 1.5, 0.0))),
      new SubMatrix((1, 1), new DenseMatrix(2, 2, Array(1.0, 4.0, 0.0, 1.0))),
      new SubMatrix((2, 0), new DenseMatrix(1, 2, Array(1.0, 0.0))),
      new SubMatrix((2, 1), new DenseMatrix(1, 2, Array(1.0, 5.0))))

    gridBasedMat = new BlockMatrix(numRowBlocks, numColBlocks, sc.parallelize(entries, 2))
  }

  test("size") {
    assert(gridBasedMat.numRows() === m)
    assert(gridBasedMat.numCols() === n)
  }

  test("toBreeze and toLocalMatrix") {
    val expected = BDM(
      (1.0, 0.0, 0.0, 0.0),
      (0.0, 2.0, 1.0, 0.0),
      (3.0, 1.5, 1.0, 0.0),
      (0.0, 0.0, 4.0, 1.0),
      (1.0, 0.0, 1.0, 5.0))

    val dense = Matrices.fromBreeze(expected).asInstanceOf[DenseMatrix]
    assert(gridBasedMat.toBreeze() === expected)
    assert(gridBasedMat.toLocalMatrix() === dense)
  }
}
