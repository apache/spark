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

import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices}
import org.apache.spark.mllib.util.LocalSparkContext

class BlockMatrixSuite extends FunSuite with LocalSparkContext {

  val m = 5
  val n = 4
  val rowPerPart = 2
  val colPerPart = 2
  val numRowBlocks = 3
  val numColBlocks = 2
  var rowBasedMat: BlockMatrix = _
  var colBasedMat: BlockMatrix = _
  var gridBasedMat: BlockMatrix = _

  override def beforeAll() {
    super.beforeAll()
    val entries: Seq[SubMatrix] = Seq(
      new SubMatrix(0, 0, new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 2.0))),
      new SubMatrix(0, 1, new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 0.0))),
      new SubMatrix(1, 0, new DenseMatrix(2, 2, Array(3.0, 0.0, 1.5, 0.0))),
      new SubMatrix(1, 1, new DenseMatrix(2, 2, Array(1.0, 4.0, 0.0, 1.0))),
      new SubMatrix(2, 0, new DenseMatrix(1, 2, Array(1.0, 0.0))),
      new SubMatrix(2, 1, new DenseMatrix(1, 2, Array(1.0, 5.0))))

    val colPart = new ColumnBasedPartitioner(numColBlocks, rowPerPart, colPerPart)
    val rowPart = new RowBasedPartitioner(numRowBlocks, rowPerPart, colPerPart)
    val gridPart = new GridPartitioner(numRowBlocks, numColBlocks, rowPerPart, colPerPart)

    colBasedMat =
      new BlockMatrix(numRowBlocks, numColBlocks, sc.parallelize(entries, numColBlocks), colPart)
    rowBasedMat =
      new BlockMatrix(numRowBlocks, numColBlocks, sc.parallelize(entries, numRowBlocks), rowPart)
    gridBasedMat =
      new BlockMatrix(numRowBlocks, numColBlocks,
        sc.parallelize(entries, numRowBlocks * numColBlocks), gridPart)
  }

  test("size") {
    assert(colBasedMat.numRows() === m)
    assert(colBasedMat.numCols() === n)
    assert(rowBasedMat.numRows() === m)
    assert(rowBasedMat.numCols() === n)
    assert(gridBasedMat.numRows() === m)
    assert(gridBasedMat.numCols() === n)
  }

  test("toBreeze and collect") {
    val expected = BDM(
      (1.0, 0.0, 0.0, 0.0),
      (0.0, 2.0, 1.0, 0.0),
      (3.0, 1.5, 1.0, 0.0),
      (0.0, 0.0, 4.0, 1.0),
      (1.0, 0.0, 1.0, 5.0))

    val dense = Matrices.fromBreeze(expected).asInstanceOf[DenseMatrix]
    assert(colBasedMat.toBreeze() === expected)
    assert(rowBasedMat.toBreeze() === expected)
    assert(gridBasedMat.toBreeze() === expected)
    assert(colBasedMat.collect() === dense)
    assert(rowBasedMat.collect() === dense)
    assert(gridBasedMat.collect() === dense)
  }

  test("blockInfo") {
    val colMatInfo = colBasedMat.getBlockInfo
    val rowMatInfo = rowBasedMat.getBlockInfo
    val gridMatInfo = gridBasedMat.getBlockInfo

    assert(colMatInfo((0, 1)).numRows === 2)
    assert(colMatInfo((0, 1)).numCols === 2)
    assert(colMatInfo((0, 1)).startRow === 0)
    assert(colMatInfo((0, 1)).startCol === 2)
    assert(colMatInfo((2, 0)).numRows === 1)
    assert(colMatInfo((2, 0)).numCols === 2)
    assert(colMatInfo((2, 0)).startRow === 4)
    assert(colMatInfo((2, 0)).startCol === 0)

    assert(rowMatInfo((0, 1)).numRows === 2)
    assert(rowMatInfo((0, 1)).numCols === 2)
    assert(rowMatInfo((0, 1)).startRow === 0)
    assert(rowMatInfo((0, 1)).startCol === 2)
    assert(rowMatInfo((2, 0)).numRows === 1)
    assert(rowMatInfo((2, 0)).numCols === 2)
    assert(rowMatInfo((2, 0)).startRow === 4)
    assert(rowMatInfo((2, 0)).startCol === 0)

    assert(gridMatInfo((0, 1)).numRows === 2)
    assert(gridMatInfo((0, 1)).numCols === 2)
    assert(gridMatInfo((0, 1)).startRow === 0)
    assert(gridMatInfo((0, 1)).startCol === 2)
    assert(gridMatInfo((2, 0)).numRows === 1)
    assert(gridMatInfo((2, 0)).numCols === 2)
    assert(gridMatInfo((2, 0)).startRow === 4)
    assert(gridMatInfo((2, 0)).startCol === 0)
  }
}
