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

package org.apache.spark.ml.linalg

import java.util.Random

import scala.collection.mutable.{Map => MutableMap}

import breeze.linalg.{CSCMatrix, Matrix => BM}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark.ml.SparkMLFunSuite
import org.apache.spark.ml.util.TestingUtils._

class MatricesSuite extends SparkMLFunSuite {
  test("dense matrix construction") {
    val m = 3
    val n = 2
    val values = Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0)
    val mat = Matrices.dense(m, n, values).asInstanceOf[DenseMatrix]
    assert(mat.numRows === m)
    assert(mat.numCols === n)
    assert(mat.values.eq(values), "should not copy data")
  }

  test("dense matrix construction with wrong dimension") {
    intercept[RuntimeException] {
      Matrices.dense(3, 2, Array(0.0, 1.0, 2.0))
    }
  }

  test("sparse matrix construction") {
    val m = 3
    val n = 4
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 2, 4, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val mat = Matrices.sparse(m, n, colPtrs, rowIndices, values).asInstanceOf[SparseMatrix]
    assert(mat.numRows === m)
    assert(mat.numCols === n)
    assert(mat.values.eq(values), "should not copy data")
    assert(mat.colPtrs.eq(colPtrs), "should not copy data")
    assert(mat.rowIndices.eq(rowIndices), "should not copy data")

    val entries: Array[(Int, Int, Double)] = Array((2, 2, 3.0), (1, 0, 1.0), (2, 0, 2.0),
        (1, 2, 2.0), (2, 2, 2.0), (1, 2, 2.0), (0, 0, 0.0))

    val mat2 = SparseMatrix.fromCOO(m, n, entries)
    assert(mat.asBreeze === mat2.asBreeze)
    assert(mat2.values.length == 4)
  }

  test("sparse matrix construction with wrong number of elements") {
    intercept[IllegalArgumentException] {
      Matrices.sparse(3, 2, Array(0, 1), Array(1, 2, 1), Array(0.0, 1.0, 2.0))
    }

    intercept[IllegalArgumentException] {
      Matrices.sparse(3, 2, Array(0, 1, 2), Array(1, 2), Array(0.0, 1.0, 2.0))
    }
  }

  test("index in matrices incorrect input") {
    val sm = Matrices.sparse(3, 2, Array(0, 2, 3), Array(1, 2, 1), Array(0.0, 1.0, 2.0))
    val dm = Matrices.dense(3, 2, Array(0.0, 2.3, 1.4, 3.2, 1.0, 9.1))
    Array(sm, dm).foreach { mat =>
      intercept[IllegalArgumentException] { mat.index(4, 1) }
      intercept[IllegalArgumentException] { mat.index(1, 4) }
      intercept[IllegalArgumentException] { mat.index(-1, 2) }
      intercept[IllegalArgumentException] { mat.index(1, -2) }
    }
  }

  test("equals") {
    val dm1 = Matrices.dense(2, 2, Array(0.0, 1.0, 2.0, 3.0))
    assert(dm1 === dm1)
    assert(dm1 !== dm1.transpose)

    val dm2 = Matrices.dense(2, 2, Array(0.0, 2.0, 1.0, 3.0))
    assert(dm1 === dm2.transpose)

    val sm1 = dm1.asInstanceOf[DenseMatrix].toSparse
    assert(sm1 === sm1)
    assert(sm1 === dm1)
    assert(sm1 !== sm1.transpose)

    val sm2 = dm2.asInstanceOf[DenseMatrix].toSparse
    assert(sm1 === sm2.transpose)
    assert(sm1 === dm2.transpose)
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

    intercept[NoSuchElementException] {
      sparseMat.update(2, 1, 10.0)
    }

    sparseMat.update(0, 1, 10.0)
    assert(sparseMat(0, 1) === 10.0)
    assert(sparseMat.values(2) === 10.0)
  }

  test("dense to dense") {
    /*
      dm1 =  4.0 2.0 -8.0
            -1.0 7.0  4.0

      dm2 = 5.0 -9.0  4.0
            1.0 -3.0 -8.0
     */
    val dm1 = new DenseMatrix(2, 3, Array(4.0, -1.0, 2.0, 7.0, -8.0, 4.0))
    val dm2 = new DenseMatrix(2, 3, Array(5.0, -9.0, 4.0, 1.0, -3.0, -8.0), isTransposed = true)

    val dm8 = dm1.toDenseColMajor
    assert(dm8 === dm1)
    assert(dm8.isColMajor)
    assert(dm8.values.equals(dm1.values))

    val dm5 = dm2.toDenseColMajor
    assert(dm5 === dm2)
    assert(dm5.isColMajor)
    assert(dm5.values === Array(5.0, 1.0, -9.0, -3.0, 4.0, -8.0))

    val dm4 = dm1.toDenseRowMajor
    assert(dm4 === dm1)
    assert(dm4.isRowMajor)
    assert(dm4.values === Array(4.0, 2.0, -8.0, -1.0, 7.0, 4.0))

    val dm6 = dm2.toDenseRowMajor
    assert(dm6 === dm2)
    assert(dm6.isRowMajor)
    assert(dm6.values.equals(dm2.values))

    val dm3 = dm1.toDense
    assert(dm3 === dm1)
    assert(dm3.isColMajor)
    assert(dm3.values.equals(dm1.values))

    val dm9 = dm2.toDense
    assert(dm9 === dm2)
    assert(dm9.isRowMajor)
    assert(dm9.values.equals(dm2.values))
  }

  test("dense to sparse") {
    /*
      dm1 = 0.0 4.0 5.0
            0.0 2.0 0.0

      dm2 = 0.0 4.0 5.0
            0.0 2.0 0.0

      dm3 = 0.0 0.0 0.0
            0.0 0.0 0.0
     */
    val dm1 = new DenseMatrix(2, 3, Array(0.0, 0.0, 4.0, 2.0, 5.0, 0.0))
    val dm2 = new DenseMatrix(2, 3, Array(0.0, 4.0, 5.0, 0.0, 2.0, 0.0), isTransposed = true)
    val dm3 = new DenseMatrix(2, 3, Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0))

    val sm1 = dm1.toSparseColMajor
    assert(sm1 === dm1)
    assert(sm1.isColMajor)
    assert(sm1.values === Array(4.0, 2.0, 5.0))

    val sm3 = dm2.toSparseColMajor
    assert(sm3 === dm2)
    assert(sm3.isColMajor)
    assert(sm3.values === Array(4.0, 2.0, 5.0))

    val sm5 = dm3.toSparseColMajor
    assert(sm5 === dm3)
    assert(sm5.values === Array.empty[Double])
    assert(sm5.isColMajor)

    val sm2 = dm1.toSparseRowMajor
    assert(sm2 === dm1)
    assert(sm2.isRowMajor)
    assert(sm2.values === Array(4.0, 5.0, 2.0))

    val sm4 = dm2.toSparseRowMajor
    assert(sm4 === dm2)
    assert(sm4.isRowMajor)
    assert(sm4.values === Array(4.0, 5.0, 2.0))

    val sm6 = dm3.toSparseRowMajor
    assert(sm6 === dm3)
    assert(sm6.values === Array.empty[Double])
    assert(sm6.isRowMajor)

    val sm7 = dm1.toSparse
    assert(sm7 === dm1)
    assert(sm7.values === Array(4.0, 2.0, 5.0))
    assert(sm7.isColMajor)

    val sm10 = dm2.toSparse
    assert(sm10 === dm2)
    assert(sm10.values === Array(4.0, 5.0, 2.0))
    assert(sm10.isRowMajor)
  }

  test("sparse to sparse") {
    /*
      sm1 = sm2 = sm3 = sm4 = 0.0 4.0 5.0
                              0.0 2.0 0.0
      smZeros = 0.0 0.0 0.0
                0.0 0.0 0.0
     */
    val sm1 = new SparseMatrix(2, 3, Array(0, 0, 2, 3), Array(0, 1, 0), Array(4.0, 2.0, 5.0))
    val sm2 = new SparseMatrix(2, 3, Array(0, 2, 3), Array(1, 2, 1), Array(4.0, 5.0, 2.0),
      isTransposed = true)
    val sm3 = new SparseMatrix(2, 3, Array(0, 0, 2, 4), Array(0, 1, 0, 1),
      Array(4.0, 2.0, 5.0, 0.0))
    val sm4 = new SparseMatrix(2, 3, Array(0, 2, 4), Array(1, 2, 1, 2),
      Array(4.0, 5.0, 2.0, 0.0), isTransposed = true)
    val smZeros = new SparseMatrix(2, 3, Array(0, 2, 4, 6), Array(0, 1, 0, 1, 0, 1),
      Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0))

    val sm6 = sm1.toSparseColMajor
    assert(sm6 === sm1)
    assert(sm6.isColMajor)
    assert(sm6.values.equals(sm1.values))

    val sm7 = sm2.toSparseColMajor
    assert(sm7 === sm2)
    assert(sm7.isColMajor)
    assert(sm7.values === Array(4.0, 2.0, 5.0))

    val sm16 = sm3.toSparseColMajor
    assert(sm16 === sm3)
    assert(sm16.isColMajor)
    assert(sm16.values === Array(4.0, 2.0, 5.0))

    val sm14 = sm4.toSparseColMajor
    assert(sm14 === sm4)
    assert(sm14.values === Array(4.0, 2.0, 5.0))
    assert(sm14.isColMajor)

    val sm15 = smZeros.toSparseColMajor
    assert(sm15 === smZeros)
    assert(sm15.values === Array.empty[Double])
    assert(sm15.isColMajor)

    val sm5 = sm1.toSparseRowMajor
    assert(sm5 === sm1)
    assert(sm5.isRowMajor)
    assert(sm5.values === Array(4.0, 5.0, 2.0))

    val sm8 = sm2.toSparseRowMajor
    assert(sm8 === sm2)
    assert(sm8.isRowMajor)
    assert(sm8.values.equals(sm2.values))

    val sm10 = sm3.toSparseRowMajor
    assert(sm10 === sm3)
    assert(sm10.values === Array(4.0, 5.0, 2.0))
    assert(sm10.isRowMajor)

    val sm11 = sm4.toSparseRowMajor
    assert(sm11 === sm4)
    assert(sm11.values === Array(4.0, 5.0, 2.0))
    assert(sm11.isRowMajor)

    val sm17 = smZeros.toSparseRowMajor
    assert(sm17 === smZeros)
    assert(sm17.values === Array.empty[Double])
    assert(sm17.isRowMajor)

    val sm9 = sm3.toSparse
    assert(sm9 === sm3)
    assert(sm9.values === Array(4.0, 2.0, 5.0))
    assert(sm9.isColMajor)

    val sm12 = sm4.toSparse
    assert(sm12 === sm4)
    assert(sm12.values === Array(4.0, 5.0, 2.0))
    assert(sm12.isRowMajor)

    val sm13 = smZeros.toSparse
    assert(sm13 === smZeros)
    assert(sm13.values === Array.empty[Double])
    assert(sm13.isColMajor)
  }

  test("sparse to dense") {
    /*
      sm1 = sm2 = 0.0 4.0 5.0
                  0.0 2.0 0.0

      sm3 = 0.0 0.0 0.0
            0.0 0.0 0.0
     */
    val sm1 = new SparseMatrix(2, 3, Array(0, 0, 2, 3), Array(0, 1, 0), Array(4.0, 2.0, 5.0))
    val sm2 = new SparseMatrix(2, 3, Array(0, 2, 3), Array(1, 2, 1), Array(4.0, 5.0, 2.0),
      isTransposed = true)
    val sm3 = new SparseMatrix(2, 3, Array(0, 0, 0, 0), Array.empty[Int], Array.empty[Double])

    val dm6 = sm1.toDenseColMajor
    assert(dm6 === sm1)
    assert(dm6.isColMajor)
    assert(dm6.values === Array(0.0, 0.0, 4.0, 2.0, 5.0, 0.0))

    val dm7 = sm2.toDenseColMajor
    assert(dm7 === sm2)
    assert(dm7.isColMajor)
    assert(dm7.values === Array(0.0, 0.0, 4.0, 2.0, 5.0, 0.0))

    val dm2 = sm1.toDenseRowMajor
    assert(dm2 === sm1)
    assert(dm2.isRowMajor)
    assert(dm2.values === Array(0.0, 4.0, 5.0, 0.0, 2.0, 0.0))

    val dm4 = sm2.toDenseRowMajor
    assert(dm4 === sm2)
    assert(dm4.isRowMajor)
    assert(dm4.values === Array(0.0, 4.0, 5.0, 0.0, 2.0, 0.0))

    val dm1 = sm1.toDense
    assert(dm1 === sm1)
    assert(dm1.isColMajor)
    assert(dm1.values === Array(0.0, 0.0, 4.0, 2.0, 5.0, 0.0))

    val dm3 = sm2.toDense
    assert(dm3 === sm2)
    assert(dm3.isRowMajor)
    assert(dm3.values === Array(0.0, 4.0, 5.0, 0.0, 2.0, 0.0))

    val dm5 = sm3.toDense
    assert(dm5 === sm3)
    assert(dm5.isColMajor)
    assert(dm5.values === Array.fill(6)(0.0))
  }

  test("compressed dense") {
    /*
      dm1 = 1.0 0.0 0.0 0.0
            1.0 0.0 0.0 0.0
            0.0 0.0 0.0 0.0

      dm2 = 1.0 1.0 0.0 0.0
            0.0 0.0 0.0 0.0
            0.0 0.0 0.0 0.0
     */
    // this should compress to a sparse matrix
    val dm1 = new DenseMatrix(3, 4, Array.fill(2)(1.0) ++ Array.fill(10)(0.0))

    // optimal compression layout is row major since numRows < numCols
    val cm1 = dm1.compressed.asInstanceOf[SparseMatrix]
    assert(cm1 === dm1)
    assert(cm1.isRowMajor)
    assert(cm1.getSizeInBytes < dm1.getSizeInBytes)

    // force compressed column major
    val cm2 = dm1.compressedColMajor.asInstanceOf[SparseMatrix]
    assert(cm2 === dm1)
    assert(cm2.isColMajor)
    assert(cm2.getSizeInBytes < dm1.getSizeInBytes)

    // optimal compression layout for transpose is column major
    val dm2 = dm1.transpose
    val cm3 = dm2.compressed.asInstanceOf[SparseMatrix]
    assert(cm3 === dm2)
    assert(cm3.isColMajor)
    assert(cm3.getSizeInBytes < dm2.getSizeInBytes)

    /*
      dm3 = 1.0 1.0 1.0 0.0
            1.0 1.0 0.0 0.0
            1.0 1.0 0.0 0.0

      dm4 = 1.0 1.0 1.0 1.0
            1.0 1.0 1.0 0.0
            0.0 0.0 0.0 0.0
     */
    // this should compress to a dense matrix
    val dm3 = new DenseMatrix(3, 4, Array.fill(7)(1.0) ++ Array.fill(5)(0.0))
    val dm4 = new DenseMatrix(3, 4, Array.fill(7)(1.0) ++ Array.fill(5)(0.0), isTransposed = true)

    val cm4 = dm3.compressed.asInstanceOf[DenseMatrix]
    assert(cm4 === dm3)
    assert(cm4.isColMajor)
    assert(cm4.values.equals(dm3.values))
    assert(cm4.getSizeInBytes === dm3.getSizeInBytes)

    // force compressed row major
    val cm5 = dm3.compressedRowMajor.asInstanceOf[DenseMatrix]
    assert(cm5 === dm3)
    assert(cm5.isRowMajor)
    assert(cm5.getSizeInBytes === dm3.getSizeInBytes)

    val cm6 = dm4.compressed.asInstanceOf[DenseMatrix]
    assert(cm6 === dm4)
    assert(cm6.isRowMajor)
    assert(cm6.values.equals(dm4.values))
    assert(cm6.getSizeInBytes === dm4.getSizeInBytes)

    val cm7 = dm4.compressedColMajor.asInstanceOf[DenseMatrix]
    assert(cm7 === dm4)
    assert(cm7.isColMajor)
    assert(cm7.getSizeInBytes === dm4.getSizeInBytes)

    // this has the same size sparse or dense
    val dm5 = new DenseMatrix(4, 4, Array.fill(7)(1.0) ++ Array.fill(9)(0.0))
    // should choose dense to break ties
    val cm8 = dm5.compressed.asInstanceOf[DenseMatrix]
    assert(cm8.getSizeInBytes === dm5.toSparseColMajor.getSizeInBytes)
  }

  test("compressed sparse") {
    /*
       sm1 = 0.0 -1.0
             0.0  0.0
             0.0  0.0
             0.0  0.0

       sm2 = 0.0 0.0 0.0 0.0
            -1.0 0.0 0.0 0.0
     */
    // these should compress to sparse matrices
    val sm1 = new SparseMatrix(4, 2, Array(0, 0, 1), Array(0), Array(-1.0))
    val sm2 = sm1.transpose

    val cm1 = sm1.compressed.asInstanceOf[SparseMatrix]
    // optimal is column major
    assert(cm1 === sm1)
    assert(cm1.isColMajor)
    assert(cm1.values.equals(sm1.values))
    assert(cm1.getSizeInBytes === sm1.getSizeInBytes)

    val cm2 = sm1.compressedRowMajor.asInstanceOf[SparseMatrix]
    assert(cm2 === sm1)
    assert(cm2.isRowMajor)
    // forced to be row major, so we have increased the size
    assert(cm2.getSizeInBytes > sm1.getSizeInBytes)
    assert(cm2.getSizeInBytes < sm1.toDense.getSizeInBytes)

    val cm9 = sm1.compressedColMajor.asInstanceOf[SparseMatrix]
    assert(cm9 === sm1)
    assert(cm9.values.equals(sm1.values))
    assert(cm9.getSizeInBytes === sm1.getSizeInBytes)

    val cm3 = sm2.compressed.asInstanceOf[SparseMatrix]
    assert(cm3 === sm2)
    assert(cm3.isRowMajor)
    assert(cm3.values.equals(sm2.values))
    assert(cm3.getSizeInBytes === sm2.getSizeInBytes)

    val cm8 = sm2.compressedColMajor.asInstanceOf[SparseMatrix]
    assert(cm8 === sm2)
    assert(cm8.isColMajor)
    // forced to be col major, so we have increased the size
    assert(cm8.getSizeInBytes > sm2.getSizeInBytes)
    assert(cm8.getSizeInBytes < sm2.toDense.getSizeInBytes)

    val cm10 = sm2.compressedRowMajor.asInstanceOf[SparseMatrix]
    assert(cm10 === sm2)
    assert(cm10.isRowMajor)
    assert(cm10.values.equals(sm2.values))
    assert(cm10.getSizeInBytes === sm2.getSizeInBytes)


    /*
       sm3 = 0.0 -1.0
             2.0  3.0
            -4.0  9.0
     */
    // this should compress to a dense matrix
    val sm3 = new SparseMatrix(3, 2, Array(0, 2, 5), Array(1, 2, 0, 1, 2),
      Array(2.0, -4.0, -1.0, 3.0, 9.0))

    // dense is optimal, and maintains column major
    val cm4 = sm3.compressed.asInstanceOf[DenseMatrix]
    assert(cm4 === sm3)
    assert(cm4.isColMajor)
    assert(cm4.getSizeInBytes < sm3.getSizeInBytes)

    val cm5 = sm3.compressedRowMajor.asInstanceOf[DenseMatrix]
    assert(cm5 === sm3)
    assert(cm5.isRowMajor)
    assert(cm5.getSizeInBytes < sm3.getSizeInBytes)

    val cm11 = sm3.compressedColMajor.asInstanceOf[DenseMatrix]
    assert(cm11 === sm3)
    assert(cm11.isColMajor)
    assert(cm11.getSizeInBytes < sm3.getSizeInBytes)

    /*
      sm4 = 1.0 0.0 0.0 ...

      sm5 = 1.0
            0.0
            0.0
            ...
     */
    val sm4 = new SparseMatrix(Int.MaxValue, 1, Array(0, 1), Array(0), Array(1.0))
    val cm6 = sm4.compressed.asInstanceOf[SparseMatrix]
    assert(cm6 === sm4)
    assert(cm6.isColMajor)
    assert(cm6.getSizeInBytes <= sm4.getSizeInBytes)

    val sm5 = new SparseMatrix(1, Int.MaxValue, Array(0, 1), Array(0), Array(1.0),
      isTransposed = true)
    val cm7 = sm5.compressed.asInstanceOf[SparseMatrix]
    assert(cm7 === sm5)
    assert(cm7.isRowMajor)
    assert(cm7.getSizeInBytes <= sm5.getSizeInBytes)

    // this has the same size sparse or dense
    val sm6 = new SparseMatrix(4, 4, Array(0, 4, 7, 7, 7), Array(0, 1, 2, 3, 0, 1, 2),
      Array.fill(7)(1.0))
    // should choose dense to break ties
    val cm12 = sm6.compressed.asInstanceOf[DenseMatrix]
    assert(cm12.getSizeInBytes === sm6.getSizeInBytes)
  }

  test("map, update") {
    val m = 3
    val n = 2
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val allValues = Array(1.0, 2.0, 0.0, 0.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(0, 1, 1, 2)

    val spMat1 = new SparseMatrix(m, n, colPtrs, rowIndices, values)
    val deMat1 = new DenseMatrix(m, n, allValues)
    val deMat2 = deMat1.map(_ * 2)
    val spMat2 = spMat1.map(_ * 2)
    deMat1.update(_ * 2)
    spMat1.update(_ * 2)

    assert(spMat1.toArray === spMat2.toArray)
    assert(deMat1.toArray === deMat2.toArray)
  }

  test("transpose") {
    val dA =
      new DenseMatrix(4, 3, Array(0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0))
    val sA = new SparseMatrix(4, 3, Array(0, 1, 3, 4), Array(1, 0, 2, 3), Array(1.0, 2.0, 1.0, 3.0))

    val dAT = dA.transpose
    val sAT = sA.transpose
    val dATexpected =
      new DenseMatrix(3, 4, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0))
    val sATexpected =
      new SparseMatrix(3, 4, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0))

    assert(dAT.asBreeze === dATexpected.asBreeze)
    assert(sAT.asBreeze === sATexpected.asBreeze)
    assert(dA(1, 0) === dAT(0, 1))
    assert(dA(2, 1) === dAT(1, 2))
    assert(sA(1, 0) === sAT(0, 1))
    assert(sA(2, 1) === sAT(1, 2))

    assert(!dA.toArray.eq(dAT.toArray), "has to have a new array")
    assert(dA.values.eq(dAT.transpose.values), "should not copy array")

    assert(dAT.toSparse.asBreeze === sATexpected.asBreeze)
    assert(sAT.toDense.asBreeze === dATexpected.asBreeze)
  }

  test("foreachActive") {
    val m = 3
    val n = 2
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val allValues = Array(1.0, 2.0, 0.0, 0.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(0, 1, 1, 2)

    val sp = new SparseMatrix(m, n, colPtrs, rowIndices, values)
    val dn = new DenseMatrix(m, n, allValues)

    val dnMap = MutableMap[(Int, Int), Double]()
    dn.foreachActive { (i, j, value) =>
      dnMap.put((i, j), value)
    }
    assert(dnMap.size === 6)
    assert(dnMap((0, 0)) === 1.0)
    assert(dnMap((1, 0)) === 2.0)
    assert(dnMap((2, 0)) === 0.0)
    assert(dnMap((0, 1)) === 0.0)
    assert(dnMap((1, 1)) === 4.0)
    assert(dnMap((2, 1)) === 5.0)

    val spMap = MutableMap[(Int, Int), Double]()
    sp.foreachActive { (i, j, value) =>
      spMap.put((i, j), value)
    }
    assert(spMap.size === 4)
    assert(spMap((0, 0)) === 1.0)
    assert(spMap((1, 0)) === 2.0)
    assert(spMap((1, 1)) === 4.0)
    assert(spMap((2, 1)) === 5.0)
  }

  test("horzcat, vertcat, eye, speye") {
    val m = 3
    val n = 2
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val allValues = Array(1.0, 2.0, 0.0, 0.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(0, 1, 1, 2)
    // transposed versions
    val allValuesT = Array(1.0, 0.0, 2.0, 4.0, 0.0, 5.0)
    val colPtrsT = Array(0, 1, 3, 4)
    val rowIndicesT = Array(0, 0, 1, 1)

    val spMat1 = new SparseMatrix(m, n, colPtrs, rowIndices, values)
    val deMat1 = new DenseMatrix(m, n, allValues)
    val spMat1T = new SparseMatrix(n, m, colPtrsT, rowIndicesT, values)
    val deMat1T = new DenseMatrix(n, m, allValuesT)

    // should equal spMat1 & deMat1 respectively
    val spMat1TT = spMat1T.transpose
    val deMat1TT = deMat1T.transpose

    val deMat2 = Matrices.eye(3)
    val spMat2 = Matrices.speye(3)
    val deMat3 = Matrices.eye(2)
    val spMat3 = Matrices.speye(2)

    val spHorz = Matrices.horzcat(Array(spMat1, spMat2))
    val spHorz2 = Matrices.horzcat(Array(spMat1, deMat2))
    val spHorz3 = Matrices.horzcat(Array(deMat1, spMat2))
    val deHorz1 = Matrices.horzcat(Array(deMat1, deMat2))
    val deHorz2 = Matrices.horzcat(Array.empty[Matrix])

    assert(deHorz1.numRows === 3)
    assert(spHorz2.numRows === 3)
    assert(spHorz3.numRows === 3)
    assert(spHorz.numRows === 3)
    assert(deHorz1.numCols === 5)
    assert(spHorz2.numCols === 5)
    assert(spHorz3.numCols === 5)
    assert(spHorz.numCols === 5)
    assert(deHorz2.numRows === 0)
    assert(deHorz2.numCols === 0)
    assert(deHorz2.toArray.length === 0)

    assert(deHorz1 ~== spHorz2.asInstanceOf[SparseMatrix].toDense absTol 1e-15)
    assert(spHorz2 ~== spHorz3 absTol 1e-15)
    assert(spHorz(0, 0) === 1.0)
    assert(spHorz(2, 1) === 5.0)
    assert(spHorz(0, 2) === 1.0)
    assert(spHorz(1, 2) === 0.0)
    assert(spHorz(1, 3) === 1.0)
    assert(spHorz(2, 4) === 1.0)
    assert(spHorz(1, 4) === 0.0)
    assert(deHorz1(0, 0) === 1.0)
    assert(deHorz1(2, 1) === 5.0)
    assert(deHorz1(0, 2) === 1.0)
    assert(deHorz1(1, 2) == 0.0)
    assert(deHorz1(1, 3) === 1.0)
    assert(deHorz1(2, 4) === 1.0)
    assert(deHorz1(1, 4) === 0.0)

    // containing transposed matrices
    val spHorzT = Matrices.horzcat(Array(spMat1TT, spMat2))
    val spHorz2T = Matrices.horzcat(Array(spMat1TT, deMat2))
    val spHorz3T = Matrices.horzcat(Array(deMat1TT, spMat2))
    val deHorz1T = Matrices.horzcat(Array(deMat1TT, deMat2))

    assert(deHorz1T ~== deHorz1 absTol 1e-15)
    assert(spHorzT ~== spHorz absTol 1e-15)
    assert(spHorz2T ~== spHorz2 absTol 1e-15)
    assert(spHorz3T ~== spHorz3 absTol 1e-15)

    intercept[IllegalArgumentException] {
      Matrices.horzcat(Array(spMat1, spMat3))
    }

    intercept[IllegalArgumentException] {
      Matrices.horzcat(Array(deMat1, spMat3))
    }

    val spVert = Matrices.vertcat(Array(spMat1, spMat3))
    val deVert1 = Matrices.vertcat(Array(deMat1, deMat3))
    val spVert2 = Matrices.vertcat(Array(spMat1, deMat3))
    val spVert3 = Matrices.vertcat(Array(deMat1, spMat3))
    val deVert2 = Matrices.vertcat(Array.empty[Matrix])

    assert(deVert1.numRows === 5)
    assert(spVert2.numRows === 5)
    assert(spVert3.numRows === 5)
    assert(spVert.numRows === 5)
    assert(deVert1.numCols === 2)
    assert(spVert2.numCols === 2)
    assert(spVert3.numCols === 2)
    assert(spVert.numCols === 2)
    assert(deVert2.numRows === 0)
    assert(deVert2.numCols === 0)
    assert(deVert2.toArray.length === 0)

    assert(deVert1 ~== spVert2.asInstanceOf[SparseMatrix].toDense absTol 1e-15)
    assert(spVert2 ~== spVert3 absTol 1e-15)
    assert(spVert(0, 0) === 1.0)
    assert(spVert(2, 1) === 5.0)
    assert(spVert(3, 0) === 1.0)
    assert(spVert(3, 1) === 0.0)
    assert(spVert(4, 1) === 1.0)
    assert(deVert1(0, 0) === 1.0)
    assert(deVert1(2, 1) === 5.0)
    assert(deVert1(3, 0) === 1.0)
    assert(deVert1(3, 1) === 0.0)
    assert(deVert1(4, 1) === 1.0)

    // containing transposed matrices
    val spVertT = Matrices.vertcat(Array(spMat1TT, spMat3))
    val deVert1T = Matrices.vertcat(Array(deMat1TT, deMat3))
    val spVert2T = Matrices.vertcat(Array(spMat1TT, deMat3))
    val spVert3T = Matrices.vertcat(Array(deMat1TT, spMat3))

    assert(deVert1T ~== deVert1 absTol 1e-15)
    assert(spVertT ~== spVert absTol 1e-15)
    assert(spVert2T ~== spVert2 absTol 1e-15)
    assert(spVert3T ~== spVert3 absTol 1e-15)

    intercept[IllegalArgumentException] {
      Matrices.vertcat(Array(spMat1, spMat2))
    }

    intercept[IllegalArgumentException] {
      Matrices.vertcat(Array(deMat1, spMat2))
    }
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

  test("sprand") {
    val rng = mock[Random]
    when(rng.nextInt(4)).thenReturn(0, 1, 1, 3, 2, 2, 0, 1, 3, 0)
    when(rng.nextDouble()).thenReturn(1.0, 2.0, 3.0, 4.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
    val mat = SparseMatrix.sprand(4, 4, 0.25, rng)
    assert(mat.numRows === 4)
    assert(mat.numCols === 4)
    assert(mat.rowIndices.toSeq === Seq(3, 0, 2, 1))
    assert(mat.values.toSeq === Seq(1.0, 2.0, 3.0, 4.0))
    val mat2 = SparseMatrix.sprand(2, 3, 1.0, rng)
    assert(mat2.rowIndices.toSeq === Seq(0, 1, 0, 1, 0, 1))
    assert(mat2.colPtrs.toSeq === Seq(0, 2, 4, 6))
  }

  test("sprandn") {
    val rng = mock[Random]
    when(rng.nextInt(4)).thenReturn(0, 1, 1, 3, 2, 2, 0, 1, 3, 0)
    when(rng.nextGaussian()).thenReturn(1.0, 2.0, 3.0, 4.0)
    val mat = SparseMatrix.sprandn(4, 4, 0.25, rng)
    assert(mat.numRows === 4)
    assert(mat.numCols === 4)
    assert(mat.rowIndices.toSeq === Seq(3, 0, 2, 1))
    assert(mat.values.toSeq === Seq(1.0, 2.0, 3.0, 4.0))
  }

  test("toString") {
    val empty = Matrices.ones(0, 0)
    empty.toString(0, 0)

    val mat = Matrices.rand(5, 10, new Random())
    mat.toString(-1, -5)
    mat.toString(0, 0)
    mat.toString(Int.MinValue, Int.MinValue)
    mat.toString(Int.MaxValue, Int.MaxValue)
    var lines = mat.toString(6, 50).split('\n')
    assert(lines.length == 5 && lines.forall(_.length <= 50))

    lines = mat.toString(5, 100).split('\n')
    assert(lines.length == 5 && lines.forall(_.length <= 100))
  }

  test("numNonzeros and numActives") {
    val dm1 = Matrices.dense(3, 2, Array(0, 0, -1, 1, 0, 1))
    assert(dm1.numNonzeros === 3)
    assert(dm1.numActives === 6)

    val sm1 = Matrices.sparse(3, 2, Array(0, 2, 3), Array(0, 2, 1), Array(0.0, -1.2, 0.0))
    assert(sm1.numNonzeros === 1)
    assert(sm1.numActives === 3)
  }

  test("fromBreeze with sparse matrix") {
    // colPtr.last does NOT always equal to values.length in breeze SCSMatrix and
    // invocation of compact() may be necessary. Refer to SPARK-11507
    val bm1: BM[Double] = new CSCMatrix[Double](
      Array(1.0, 1, 1), 3, 3, Array(0, 1, 2, 3), Array(0, 1, 2))
    val bm2: BM[Double] = new CSCMatrix[Double](
      Array(1.0, 2, 2, 4), 3, 3, Array(0, 0, 2, 4), Array(1, 2, 1, 2))
    val sum = bm1 + bm2
    Matrices.fromBreeze(sum)
  }

  test("row/col iterator") {
    val dm = new DenseMatrix(3, 2, Array(0, 1, 2, 3, 4, 0))
    val sm = dm.toSparse
    val rows = Seq(Vectors.dense(0, 3), Vectors.dense(1, 4), Vectors.dense(2, 0))
    val cols = Seq(Vectors.dense(0, 1, 2), Vectors.dense(3, 4, 0))
    for (m <- Seq(dm, sm)) {
      assert(m.rowIter.toSeq === rows)
      assert(m.colIter.toSeq === cols)
      assert(m.transpose.rowIter.toSeq === cols)
      assert(m.transpose.colIter.toSeq === rows)
    }
  }
}
