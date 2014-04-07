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

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, diag => brzDiag, norm => brzNorm}

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.linalg.{Matrices, Vectors, Vector, Matrix}

class RowMatrixSuite extends FunSuite with LocalSparkContext {

  val m = 4
  val n = 3
  val arr = Array(0.0, 3.0, 6.0, 9.0, 1.0, 4.0, 7.0, 0.0, 2.0, 5.0, 8.0, 1.0)
  val denseData = Seq(
    Vectors.dense(0.0, 1.0, 2.0),
    Vectors.dense(3.0, 4.0, 5.0),
    Vectors.dense(6.0, 7.0, 8.0),
    Vectors.dense(9.0, 0.0, 1.0)
  )
  val sparseData = Seq(
    Vectors.sparse(3, Seq((1, 1.0), (2, 2.0))),
    Vectors.sparse(3, Seq((0, 3.0), (1, 4.0), (2, 5.0))),
    Vectors.sparse(3, Seq((0, 6.0), (1, 7.0), (2, 8.0))),
    Vectors.sparse(3, Seq((0, 9.0), (2, 1.0)))
  )

  val principalComponents = Matrices.dense(n, n,
    Array(0.0, math.sqrt(2.0) / 2.0, math.sqrt(2.0) / 2.0, 1.0, 0.0, 0.0,
      0.0, math.sqrt(2.0) / 2.0, - math.sqrt(2.0) / 2.0))

  var denseMat: RowMatrix = _
  var sparseMat: RowMatrix = _

  override def beforeAll() {
    super.beforeAll()
    denseMat = new RowMatrix(sc.parallelize(denseData, 2))
    sparseMat = new RowMatrix(sc.parallelize(sparseData, 2))
  }

  test("size") {
    assert(denseMat.numRows() === m)
    assert(denseMat.numCols() === n)
    assert(sparseMat.numRows() === m)
    assert(sparseMat.numCols() === n)
  }

  test("empty rows") {
    val rows = sc.parallelize(Seq[Vector](), 1)
    val emptyMat = new RowMatrix(rows)
    intercept[RuntimeException] {
      emptyMat.numCols()
    }
    intercept[RuntimeException] {
      emptyMat.numRows()
    }
  }

  test("gram") {
    val expected =
      Matrices.dense(n, n, Array(126.0, 54.0, 72.0, 54.0, 66.0, 78.0, 72.0, 78.0, 94.0))
    for (mat <- Seq(denseMat, sparseMat)) {
      val G = mat.computeGramianMatrix()
      assert(G.toBreeze === expected.toBreeze)
    }
  }

  test("svd") {
    val A = new BDM[Double](m, n, arr)
    for (mat <- Seq(denseMat, sparseMat)) {
      val svd = mat.computeSVD(n, computeU = true)
      val U = svd.U
      val brzSigma = svd.s.toBreeze.asInstanceOf[BDV[Double]]
      val brzV = svd.V.toBreeze.asInstanceOf[BDM[Double]]
      val rows = U.rows.collect()
      val brzUt = new BDM[Double](n, m, rows.flatMap(r => r.toArray))
      val UsVt = brzUt.t * brzDiag(brzSigma) * brzV.t
      assert(closeToZero(UsVt - A))
      val VtV: BDM[Double] = brzV.t * brzV
      assert(closeToZero(VtV - BDM.eye[Double](n)))
      val UtU = U.computeGramianMatrix().toBreeze.asInstanceOf[BDM[Double]]
      assert(closeToZero(UtU - BDM.eye[Double](n)))
    }
  }

  def closeToZero(G: BDM[Double]): Boolean = {
    G.valuesIterator.map(math.abs).sum < 1e-6
  }

  def closeToZero(v: BDV[Double]): Boolean = {
    brzNorm(v, 1.0) < 1e-6
  }

  def assertPrincipalComponentsEqual(a: Matrix, b: Matrix, k: Int) {
    val brzA = a.toBreeze.asInstanceOf[BDM[Double]]
    val brzB = b.toBreeze.asInstanceOf[BDM[Double]]
    assert(brzA.rows === brzB.rows)
    for (j <- 0 until k) {
      val aj = brzA(::, j)
      val bj = brzB(::, j)
      assert(closeToZero(aj - bj) || closeToZero(aj + bj),
        s"The $j-th components mismatch: $aj and $bj")
    }
  }

  test("pca") {
    for (mat <- Seq(denseMat, sparseMat); k <- 1 to n) {
      val pc = denseMat.computePrincipalComponents(k)
      assert(pc.numRows === n)
      assert(pc.numCols === k)
      assertPrincipalComponentsEqual(pc, principalComponents, k)
    }
  }

  test("multiply a local matrix") {
    val B = Matrices.dense(n, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    for (mat <- Seq(denseMat, sparseMat)) {
      val AB = mat.multiply(B)
      assert(AB.numRows() === m)
      assert(AB.numCols() === 2)
      assert(AB.rows.collect().toSeq === Seq(
        Vectors.dense(5.0, 14.0),
        Vectors.dense(14.0, 50.0),
        Vectors.dense(23.0, 86.0),
        Vectors.dense(2.0, 32.0)
      ))
    }
  }
}
