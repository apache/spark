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
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD

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

  test("toBlockMatrix dense backing") {
    val idxRowMatDense = new IndexedRowMatrix(indexedRows)

    // Tests when n % colsPerBlock != 0
    val blockMat = idxRowMatDense.toBlockMatrix(2, 2)
    assert(blockMat.numRows() === m)
    assert(blockMat.numCols() === n)
    assert(blockMat.toBreeze() === idxRowMatDense.toBreeze())

    // Tests when m % rowsPerBlock != 0
    val blockMat2 = idxRowMatDense.toBlockMatrix(3, 1)
    assert(blockMat2.numRows() === m)
    assert(blockMat2.numCols() === n)
    assert(blockMat2.toBreeze() === idxRowMatDense.toBreeze())

    intercept[IllegalArgumentException] {
      idxRowMatDense.toBlockMatrix(-1, 2)
    }
    intercept[IllegalArgumentException] {
      idxRowMatDense.toBlockMatrix(2, 0)
    }

    assert(blockMat.blocks.map { case (_, matrix: Matrix) =>
      matrix.isInstanceOf[DenseMatrix]
    }.reduce(_ && _))
    assert(blockMat2.blocks.map { case (_, matrix: Matrix) =>
      matrix.isInstanceOf[DenseMatrix]
    }.reduce(_ && _))
  }

  test("toBlockMatrix sparse backing") {
    val sparseData = Seq(
      (15L, Vectors.sparse(12, Seq((0, 4.0))))
    ).map(x => IndexedRow(x._1, x._2))

    // Gonna make m and n larger here so the matrices can easily be completely sparse:
    val m = 16
    val n = 12

    val idxRowMatSparse = new IndexedRowMatrix(sc.parallelize(sparseData))

    // Tests when n % colsPerBlock != 0
    val blockMat = idxRowMatSparse.toBlockMatrix(8, 8)
    assert(blockMat.numRows() === m)
    assert(blockMat.numCols() === n)
    assert(blockMat.toBreeze() === idxRowMatSparse.toBreeze())

    // Tests when m % rowsPerBlock != 0
    val blockMat2 = idxRowMatSparse.toBlockMatrix(6, 6)
    assert(blockMat2.numRows() === m)
    assert(blockMat2.numCols() === n)
    assert(blockMat2.toBreeze() === idxRowMatSparse.toBreeze())

    assert(blockMat.blocks.collect().forall{ case (_, matrix: Matrix) =>
      matrix.isInstanceOf[SparseMatrix]
    })
    assert(blockMat2.blocks.collect().forall{ case (_, matrix: Matrix) =>
      matrix.isInstanceOf[SparseMatrix]
    })
  }

  test("toBlockMatrix mixed backing") {
    val m = 24
    val n = 18

    val mixedData = Seq(
      (0L, Vectors.dense((0 to 17).map(_.toDouble).toArray)),
      (1L, Vectors.dense((0 to 17).map(_.toDouble).toArray)),
      (23L, Vectors.sparse(18, Seq((0, 4.0)))))
      .map(x => IndexedRow(x._1, x._2))

    val idxRowMatMixed = new IndexedRowMatrix(
      sc.parallelize(mixedData))

    // Tests when n % colsPerBlock != 0
    val blockMat = idxRowMatMixed.toBlockMatrix(12, 12)
    assert(blockMat.numRows() === m)
    assert(blockMat.numCols() === n)
    assert(blockMat.toBreeze() === idxRowMatMixed.toBreeze())

    // Tests when m % rowsPerBlock != 0
    val blockMat2 = idxRowMatMixed.toBlockMatrix(18, 6)
    assert(blockMat2.numRows() === m)
    assert(blockMat2.numCols() === n)
    assert(blockMat2.toBreeze() === idxRowMatMixed.toBreeze())

    val blocks = blockMat.blocks.collect()

    assert(blocks.forall { case((row, col), matrix) =>
      if (row == 0) matrix.isInstanceOf[DenseMatrix] else matrix.isInstanceOf[SparseMatrix]})
  }

  test("multiply a local matrix") {
    val A = new IndexedRowMatrix(indexedRows)
    val B = Matrices.dense(3, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val C = A.multiply(B)
    val localA = A.toBreeze()
    val localC = C.toBreeze()
    val expected = localA * B.asBreeze.asInstanceOf[BDM[Double]]
    assert(localC === expected)
  }

  test("gram") {
    val A = new IndexedRowMatrix(indexedRows)
    val G = A.computeGramianMatrix()
    val expected = BDM(
      (90.0, 12.0, 24.0),
      (12.0, 17.0, 22.0),
      (24.0, 22.0, 30.0))
    assert(G.asBreeze === expected)
  }

  test("svd") {
    val A = new IndexedRowMatrix(indexedRows)
    val svd = A.computeSVD(n, computeU = true)
    assert(svd.U.isInstanceOf[IndexedRowMatrix])
    val localA = A.toBreeze()
    val U = svd.U.toBreeze()
    val s = svd.s.asBreeze.asInstanceOf[BDV[Double]]
    val V = svd.V.asBreeze.asInstanceOf[BDM[Double]]
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

  test("similar columns") {
    val A = new IndexedRowMatrix(indexedRows)
    val gram = A.computeGramianMatrix().asBreeze.toDenseMatrix

    val G = A.columnSimilarities().toBreeze()

    for (i <- 0 until n; j <- i + 1 until n) {
      val trueResult = gram(i, j) / scala.math.sqrt(gram(i, i) * gram(j, j))
      assert(math.abs(G(i, j) - trueResult) < 1e-6)
    }
  }

  def closeToZero(G: BDM[Double]): Boolean = {
    G.valuesIterator.map(math.abs).sum < 1e-6
  }
}

