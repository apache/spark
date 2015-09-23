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

import breeze.linalg.{diag => brzDiag, DenseMatrix => BDM, DenseVector => BDV}

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrices, Vectors}

class IndexedRowMatrixSuite extends SparkFunSuite with MLlibTestSparkContext {

  val m = 4
  val n = 3
  val data = Seq(
    (0L, Vectors.dense(0.0, 1.0, 2.0)),
    (1L, Vectors.dense(3.0, 4.0, 5.0)),
    (3L, Vectors.dense(9.0, 0.0, 1.0))
  ).map(x => IndexedRow(x._1, x._2))
  var indexedRows: RDD[IndexedRow] = _

  override def beforeAll() {
    super.beforeAll()
    indexedRows = sc.parallelize(data, 2)
  }

  test("size") {
    val mat1 = new IndexedRowMatrix(indexedRows)
    assert(mat1.numRows() === m)
    assert(mat1.numCols() === n)

    val mat2 = new IndexedRowMatrix(indexedRows, 5, 0)
    assert(mat2.numRows() === 5)
    assert(mat2.numCols() === n)
  }

  test("empty rows") {
    val rows = sc.parallelize(Seq[IndexedRow](), 1)
    val mat = new IndexedRowMatrix(rows)
    intercept[RuntimeException] {
      mat.numRows()
    }
    intercept[RuntimeException] {
      mat.numCols()
    }
  }

  test("toBreeze") {
    val mat = new IndexedRowMatrix(indexedRows)
    val expected = BDM(
      (0.0, 1.0, 2.0),
      (3.0, 4.0, 5.0),
      (0.0, 0.0, 0.0),
      (9.0, 0.0, 1.0))
    assert(mat.toBreeze() === expected)
  }

  test("toRowMatrix") {
    val idxRowMat = new IndexedRowMatrix(indexedRows)
    val rowMat = idxRowMat.toRowMatrix()
    assert(rowMat.numCols() === n)
    assert(rowMat.numRows() === 3, "should drop empty rows")
    assert(rowMat.rows.collect().toSeq === data.map(_.vector).toSeq)
  }

  test("toCoordinateMatrix") {
    val idxRowMat = new IndexedRowMatrix(indexedRows)
    val coordMat = idxRowMat.toCoordinateMatrix()
    assert(coordMat.numRows() === m)
    assert(coordMat.numCols() === n)
    assert(coordMat.toBreeze() === idxRowMat.toBreeze())
  }

  test("toBlockMatrix") {
    val idxRowMat = new IndexedRowMatrix(indexedRows)
    val blockMat = idxRowMat.toBlockMatrix(2, 2)
    assert(blockMat.numRows() === m)
    assert(blockMat.numCols() === n)
    assert(blockMat.toBreeze() === idxRowMat.toBreeze())

    intercept[IllegalArgumentException] {
      idxRowMat.toBlockMatrix(-1, 2)
    }
    intercept[IllegalArgumentException] {
      idxRowMat.toBlockMatrix(2, 0)
    }
  }

  test("multiply a local matrix") {
    val A = new IndexedRowMatrix(indexedRows)
    val B = Matrices.dense(3, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val C = A.multiply(B)
    val localA = A.toBreeze()
    val localC = C.toBreeze()
    val expected = localA * B.toBreeze.asInstanceOf[BDM[Double]]
    assert(localC === expected)
  }

  test("gram") {
    val A = new IndexedRowMatrix(indexedRows)
    val G = A.computeGramianMatrix()
    val expected = BDM(
      (90.0, 12.0, 24.0),
      (12.0, 17.0, 22.0),
      (24.0, 22.0, 30.0))
    assert(G.toBreeze === expected)
  }

  test("svd") {
    val A = new IndexedRowMatrix(indexedRows)
    val svd = A.computeSVD(n, computeU = true)
    assert(svd.U.isInstanceOf[IndexedRowMatrix])
    val localA = A.toBreeze()
    val U = svd.U.toBreeze()
    val s = svd.s.toBreeze.asInstanceOf[BDV[Double]]
    val V = svd.V.toBreeze.asInstanceOf[BDM[Double]]
    assert(closeToZero(U.t * U - BDM.eye[Double](n)))
    assert(closeToZero(V.t * V - BDM.eye[Double](n)))
    assert(closeToZero(U * brzDiag(s) * V.t - localA))
  }

  test("validate matrix sizes of svd") {
    val k = 2
    val A = new IndexedRowMatrix(indexedRows)
    val svd = A.computeSVD(k, computeU = true)
    assert(svd.U.numRows() === m)
    assert(svd.U.numCols() === k)
    assert(svd.s.size === k)
    assert(svd.V.numRows === n)
    assert(svd.V.numCols === k)
  }

  test("validate k in svd") {
    val A = new IndexedRowMatrix(indexedRows)
    intercept[IllegalArgumentException] {
      A.computeSVD(-1)
    }
  }

  def closeToZero(G: BDM[Double]): Boolean = {
    G.valuesIterator.map(math.abs).sum < 1e-6
  }
}

