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
  val numPartitions = 3
  var gridBasedMat: BlockMatrix = _
  type SubMatrix = ((Int, Int), Matrix)

  override def beforeAll() {
    super.beforeAll()

    val entries: Seq[SubMatrix] = Seq(
      new SubMatrix((0, 0), new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 2.0))),
      new SubMatrix((0, 1), new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 0.0))),
      new SubMatrix((1, 0), new DenseMatrix(2, 2, Array(3.0, 0.0, 1.0, 1.0))),
      new SubMatrix((1, 1), new DenseMatrix(2, 2, Array(1.0, 2.0, 0.0, 1.0))),
      new SubMatrix((2, 1), new DenseMatrix(1, 2, Array(1.0, 5.0))))

    gridBasedMat = new BlockMatrix(sc.parallelize(entries, numPartitions), rowPerPart, colPerPart)
  }

  test("size") {
    assert(gridBasedMat.numRows() === m)
    assert(gridBasedMat.numCols() === n)
  }

  test("grid partitioner partitioning") {
    val partitioner = gridBasedMat.partitioner
    assert(partitioner.getPartition((0, 0)) === 0)
    assert(partitioner.getPartition((0, 1)) === 0)
    assert(partitioner.getPartition((1, 0)) === 1)
    assert(partitioner.getPartition((1, 1)) === 1)
    assert(partitioner.getPartition((2, 0)) === 2)
    assert(partitioner.getPartition((2, 1)) === 2)
    assert(partitioner.getPartition((1, 0, 1)) === 1)
    assert(partitioner.getPartition((2, 0, 0)) === 2)

    val part2 = new GridPartitioner(10, 20, 10)
    assert(part2.getPartition((0, 0)) === 0)
    assert(part2.getPartition((0, 1)) === 0)
    assert(part2.getPartition((0, 6)) === 2)
    assert(part2.getPartition((3, 7)) === 2)
    assert(part2.getPartition((3, 8)) === 4)
    assert(part2.getPartition((3, 13)) === 6)
    assert(part2.getPartition((9, 14)) === 7)
    assert(part2.getPartition((9, 15)) === 7)
    assert(part2.getPartition((9, 19)) === 9)

    intercept[IllegalArgumentException] {
      part2.getPartition((-1, 0))
    }

    intercept[IllegalArgumentException] {
      part2.getPartition((10, 0))
    }

    intercept[IllegalArgumentException] {
      part2.getPartition((9, 20))
    }

    val part3 = new GridPartitioner(20, 10, 10)
    assert(part3.getPartition((0, 0)) === 0)
    assert(part3.getPartition((1, 0)) === 0)
    assert(part3.getPartition((6, 0)) === 1)
    assert(part3.getPartition((7, 3)) === 1)
    assert(part3.getPartition((8, 3)) === 2)
    assert(part3.getPartition((13, 3)) === 3)
    assert(part3.getPartition((14, 9)) === 8)
    assert(part3.getPartition((15, 9)) === 8)
    assert(part3.getPartition((19, 9)) === 9)
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
