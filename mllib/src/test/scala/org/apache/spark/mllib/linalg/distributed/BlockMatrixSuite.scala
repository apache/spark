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

import scala.util.Random

import breeze.linalg.{DenseMatrix => BDM}
import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Matrix}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class BlockMatrixSuite extends FunSuite with MLlibTestSparkContext {

  val m = 5
  val n = 4
  val rowPerPart = 2
  val colPerPart = 2
  val numPartitions = 3
  var gridBasedMat: BlockMatrix = _

  override def beforeAll() {
    super.beforeAll()

    val blocks: Seq[((Int, Int), Matrix)] = Seq(
      ((0, 0), new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 2.0))),
      ((0, 1), new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 0.0))),
      ((1, 0), new DenseMatrix(2, 2, Array(3.0, 0.0, 1.0, 1.0))),
      ((1, 1), new DenseMatrix(2, 2, Array(1.0, 2.0, 0.0, 1.0))),
      ((2, 1), new DenseMatrix(1, 2, Array(1.0, 5.0))))

    gridBasedMat = new BlockMatrix(sc.parallelize(blocks, numPartitions), rowPerPart, colPerPart)
  }

  test("size") {
    assert(gridBasedMat.numRows() === m)
    assert(gridBasedMat.numCols() === n)
  }

  test("grid partitioner") {
    val random = new Random()
    // This should generate a 4x4 grid of 1x2 blocks.
    val part0 = GridPartitioner(4, 7, suggestedNumPartitions = 12)
    val expected0 = Array(
      Array(0, 0, 4, 4,  8,  8, 12),
      Array(1, 1, 5, 5,  9,  9, 13),
      Array(2, 2, 6, 6, 10, 10, 14),
      Array(3, 3, 7, 7, 11, 11, 15))
    for (i <- 0 until 4; j <- 0 until 7) {
      assert(part0.getPartition((i, j)) === expected0(i)(j))
      assert(part0.getPartition((i, j, random.nextInt())) === expected0(i)(j))
    }

    intercept[IllegalArgumentException] {
      part0.getPartition((-1, 0))
    }

    intercept[IllegalArgumentException] {
      part0.getPartition((4, 0))
    }

    intercept[IllegalArgumentException] {
      part0.getPartition((0, -1))
    }

    intercept[IllegalArgumentException] {
      part0.getPartition((0, 7))
    }

    val part1 = GridPartitioner(2, 2, suggestedNumPartitions = 5)
    val expected1 = Array(
      Array(0, 2),
      Array(1, 3))
    for (i <- 0 until 2; j <- 0 until 2) {
      assert(part1.getPartition((i, j)) === expected1(i)(j))
      assert(part1.getPartition((i, j, random.nextInt())) === expected1(i)(j))
    }

    val part2 = GridPartitioner(2, 2, suggestedNumPartitions = 5)
    assert(part0 !== part2)
    assert(part1 === part2)

    val part3 = new GridPartitioner(2, 3, rowsPerPart = 1, colsPerPart = 2)
    val expected3 = Array(
      Array(0, 0, 2),
      Array(1, 1, 3))
    for (i <- 0 until 2; j <- 0 until 3) {
      assert(part3.getPartition((i, j)) === expected3(i)(j))
      assert(part3.getPartition((i, j, random.nextInt())) === expected3(i)(j))
    }

    val part4 = GridPartitioner(2, 3, rowsPerPart = 1, colsPerPart = 2)
    assert(part3 === part4)

    intercept[IllegalArgumentException] {
      new GridPartitioner(2, 2, rowsPerPart = 0, colsPerPart = 1)
    }

    intercept[IllegalArgumentException] {
      GridPartitioner(2, 2, rowsPerPart = 1, colsPerPart = 0)
    }

    intercept[IllegalArgumentException] {
      GridPartitioner(2, 2, suggestedNumPartitions = 0)
    }
  }

  test("toCoordinateMatrix") {
    val coordMat = gridBasedMat.toCoordinateMatrix()
    assert(coordMat.numRows() === m)
    assert(coordMat.numCols() === n)
    assert(coordMat.toBreeze() === gridBasedMat.toBreeze())
  }

  test("toIndexedRowMatrix") {
    val rowMat = gridBasedMat.toIndexedRowMatrix()
    assert(rowMat.numRows() === m)
    assert(rowMat.numCols() === n)
    assert(rowMat.toBreeze() === gridBasedMat.toBreeze())
  }

  test("toBreeze and toLocalMatrix") {
    val expected = BDM(
      (1.0, 0.0, 0.0, 0.0),
      (0.0, 2.0, 1.0, 0.0),
      (3.0, 1.0, 1.0, 0.0),
      (0.0, 1.0, 2.0, 1.0),
      (0.0, 0.0, 1.0, 5.0))

    val dense = Matrices.fromBreeze(expected).asInstanceOf[DenseMatrix]
    assert(gridBasedMat.toLocalMatrix() === dense)
    assert(gridBasedMat.toBreeze() === expected)
  }
}
