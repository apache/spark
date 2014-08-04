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

import org.scalatest.FunSuite

class MatricesSuite extends FunSuite {
  test("dense matrix construction") {
    val m = 3
    val n = 2
    val values = Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0)
    val mat = Matrices.dense(m, n, values).asInstanceOf[DenseMatrix]
    assert(mat.numRows === m)
    assert(mat.numCols === n)
    assert(mat.values.eq(values), "should not copy data")
    assert(mat.toArray.eq(values), "toArray should not copy data")
  }

  test("dense matrix construction with wrong dimension") {
    intercept[RuntimeException] {
      Matrices.dense(3, 2, Array(0.0, 1.0, 2.0))
    }
  }

  test("square matrix with DIMSUM") {
    val m = 10
    val n = 3
    val datarr = Array.tabulate(m,n){ (a, b) =>
      MatrixEntry(a, b, (a + 2).toDouble * (b + 1) / (1 + a + b)) }.flatten
    val data = sc.makeRDD(datarr, 3)

    val a = LAUtils.sparseToTallSkinnyDense(SparseMatrix(data, m, n))

    val decomposed = new SVD().setK(n).setComputeU(true).useDIMSUM(40.0).compute(a)
    val u = LAUtils.denseToSparse(decomposed.U)
    val v = decomposed.V
    val s = decomposed.S

    val denseA = getDenseMatrix(LAUtils.denseToSparse(a))
    val svd = Singular.sparseSVD(denseA)

    val retu = getDenseMatrix(u)
    val rets = DoubleMatrix.diag(new DoubleMatrix(s))
    val retv = new DoubleMatrix(v)

    // check individual decomposition
    assertMatrixApproximatelyEquals(retu, svd(0))
    assertMatrixApproximatelyEquals(rets, DoubleMatrix.diag(svd(1)))
    assertMatrixApproximatelyEquals(retv, svd(2))

    // check multiplication guarantee
    assertMatrixApproximatelyEquals(retu.mmul(rets).mmul(retv.transpose), denseA)
  }
}
