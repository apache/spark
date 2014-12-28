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

import java.util.Random

import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar._

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

  test("sparse matrix construction") {
    val m = 3
    val n = 2
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val mat = Matrices.sparse(m, n, colPtrs, rowIndices, values).asInstanceOf[SparseMatrix]
    assert(mat.numRows === m)
    assert(mat.numCols === n)
    assert(mat.values.eq(values), "should not copy data")
    assert(mat.colPtrs.eq(colPtrs), "should not copy data")
    assert(mat.rowIndices.eq(rowIndices), "should not copy data")
  }

  test("sparse matrix construction with wrong number of elements") {
    intercept[IllegalArgumentException] {
      Matrices.sparse(3, 2, Array(0, 1), Array(1, 2, 1), Array(0.0, 1.0, 2.0))
    }

    intercept[IllegalArgumentException] {
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(0.0, 1.0, 2.0))
    }
  }

  test("matrix copies are deep copies") {
    val m = 3
    val n = 2

    val denseMat = Matrices.dense(m, n, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val denseCopy = denseMat.copy

    assert(!denseMat.toArray.eq(denseCopy.toArray))

    val values = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val sparseMat = Matrices.sparse(m, n, colPtrs, rowIndices, values)
    val sparseCopy = sparseMat.copy

    assert(!sparseMat.toArray.eq(sparseCopy.toArray))
  }

  test("matrix indexing and updating") {
    val m = 3
    val n = 2
    val allValues = Array(0.0, 1.0, 2.0, 3.0, 4.0, 0.0)

    val denseMat = new DenseMatrix(m, n, allValues)

    assert(denseMat(0, 1) === 3.0)
    assert(denseMat(0, 1) === denseMat.values(3))
    assert(denseMat(0, 1) === denseMat(3))
    assert(denseMat(0, 0) === 0.0)

    denseMat.update(0, 0, 10.0)
    assert(denseMat(0, 0) === 10.0)
    assert(denseMat.values(0) === 10.0)

    val sparseValues = Array(1.0, 2.0, 3.0, 4.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 0, 1)
    val sparseMat = new SparseMatrix(m, n, colPtrs, rowIndices, sparseValues)

    assert(sparseMat(0, 1) === 3.0)
    assert(sparseMat(0, 1) === sparseMat.values(2))
    assert(sparseMat(0, 0) === 0.0)

    intercept[NoSuchElementException] {
      sparseMat.update(0, 0, 10.0)
    }

    sparseMat.update(0, 1, 10.0)
    assert(sparseMat(0, 1) === 10.0)
    assert(sparseMat.values(2) === 10.0)
  }

  test("zeros") {
    val mat = Matrices.zeros(2, 3).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 3)
    assert(mat.values.forall(_ == 0.0))
  }

  test("ones") {
    val mat = Matrices.ones(2, 3).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 3)
    assert(mat.values.forall(_ == 1.0))
  }

  test("eye") {
    val mat = Matrices.eye(2).asInstanceOf[DenseMatrix]
    assert(mat.numCols === 2)
    assert(mat.numCols === 2)
    assert(mat.values.toSeq === Seq(1.0, 0.0, 0.0, 1.0))
  }

  test("rand") {
    val rng = mock[Random]
    when(rng.nextDouble()).thenReturn(1.0, 2.0, 3.0, 4.0)
    val mat = Matrices.rand(2, 2, rng).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 2)
    assert(mat.values.toSeq === Seq(1.0, 2.0, 3.0, 4.0))
  }

  test("randn") {
    val rng = mock[Random]
    when(rng.nextGaussian()).thenReturn(1.0, 2.0, 3.0, 4.0)
    val mat = Matrices.randn(2, 2, rng).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 2)
    assert(mat.values.toSeq === Seq(1.0, 2.0, 3.0, 4.0))
  }

  test("diag") {
    val mat = Matrices.diag(Vectors.dense(1.0, 2.0)).asInstanceOf[DenseMatrix]
    assert(mat.numRows === 2)
    assert(mat.numCols === 2)
    assert(mat.values.toSeq === Seq(1.0, 0.0, 0.0, 2.0))
  }
}
