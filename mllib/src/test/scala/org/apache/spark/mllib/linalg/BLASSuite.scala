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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.linalg.BLAS._

class BLASSuite extends SparkFunSuite {

  test("copy") {
    val sx = Vectors.sparse(4, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0, 0.0)
    val sy = Vectors.sparse(4, Array(0, 1, 3), Array(2.0, 1.0, 1.0))
    val dy = Array(2.0, 1.0, 0.0, 1.0)

    val dy1 = Vectors.dense(dy.clone())
    copy(sx, dy1)
    assert(dy1 ~== dx absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    copy(dx, dy2)
    assert(dy2 ~== dx absTol 1e-15)

    intercept[IllegalArgumentException] {
      copy(sx, sy)
    }

    intercept[IllegalArgumentException] {
      copy(dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        copy(sx, Vectors.dense(0.0, 1.0, 2.0))
      }
    }
  }

  test("scal") {
    val a = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)

    scal(a, sx)
    assert(sx ~== Vectors.sparse(3, Array(0, 2), Array(0.1, -0.2)) absTol 1e-15)

    scal(a, dx)
    assert(dx ~== Vectors.dense(0.1, 0.0, -0.2) absTol 1e-15)
  }

  test("axpy") {
    val alpha = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val dy = Array(2.0, 1.0, 0.0)
    val expected = Vectors.dense(2.1, 1.0, -0.2)

    val dy1 = Vectors.dense(dy.clone())
    axpy(alpha, sx, dy1)
    assert(dy1 ~== expected absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    axpy(alpha, dx, dy2)
    assert(dy2 ~== expected absTol 1e-15)

    val sy = Vectors.sparse(4, Array(0, 1), Array(2.0, 1.0))

    intercept[IllegalArgumentException] {
      axpy(alpha, sx, sy)
    }

    intercept[IllegalArgumentException] {
      axpy(alpha, dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        axpy(alpha, sx, Vectors.dense(1.0, 2.0))
      }
    }
  }

  test("dot") {
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val sy = Vectors.sparse(3, Array(0, 1), Array(2.0, 1.0))
    val dy = Vectors.dense(2.0, 1.0, 0.0)

    assert(dot(sx, sy) ~== 2.0 absTol 1e-15)
    assert(dot(sy, sx) ~== 2.0 absTol 1e-15)
    assert(dot(sx, dy) ~== 2.0 absTol 1e-15)
    assert(dot(dy, sx) ~== 2.0 absTol 1e-15)
    assert(dot(dx, dy) ~== 2.0 absTol 1e-15)
    assert(dot(dy, dx) ~== 2.0 absTol 1e-15)

    assert(dot(sx, sx) ~== 5.0 absTol 1e-15)
    assert(dot(dx, dx) ~== 5.0 absTol 1e-15)
    assert(dot(sx, dx) ~== 5.0 absTol 1e-15)
    assert(dot(dx, sx) ~== 5.0 absTol 1e-15)

    val sx1 = Vectors.sparse(10, Array(0, 3, 5, 7, 8), Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val sx2 = Vectors.sparse(10, Array(1, 3, 6, 7, 9), Array(1.0, 2.0, 3.0, 4.0, 5.0))
    assert(dot(sx1, sx2) ~== 20.0 absTol 1e-15)
    assert(dot(sx2, sx1) ~== 20.0 absTol 1e-15)

    withClue("vector sizes must match") {
      intercept[Exception] {
        dot(sx, Vectors.dense(2.0, 1.0))
      }
    }
  }

  test("syr") {
    val dA = new DenseMatrix(4, 4,
      Array(0.0, 1.2, 2.2, 3.1, 1.2, 3.2, 5.3, 4.6, 2.2, 5.3, 1.8, 3.0, 3.1, 4.6, 3.0, 0.8))
    val x = new DenseVector(Array(0.0, 2.7, 3.5, 2.1))
    val alpha = 0.15

    val expected = new DenseMatrix(4, 4,
      Array(0.0, 1.2, 2.2, 3.1, 1.2, 4.2935, 6.7175, 5.4505, 2.2, 6.7175, 3.6375, 4.1025, 3.1,
        5.4505, 4.1025, 1.4615))

    syr(alpha, x, dA)

    assert(dA ~== expected absTol 1e-15)

    val dB =
      new DenseMatrix(3, 4, Array(0.0, 1.2, 2.2, 3.1, 1.2, 3.2, 5.3, 4.6, 2.2, 5.3, 1.8, 3.0))

    withClue("Matrix A must be a symmetric Matrix") {
      intercept[Exception] {
        syr(alpha, x, dB)
      }
    }

    val dC =
      new DenseMatrix(3, 3, Array(0.0, 1.2, 2.2, 1.2, 3.2, 5.3, 2.2, 5.3, 1.8))

    withClue("Size of vector must match the rank of matrix") {
      intercept[Exception] {
        syr(alpha, x, dC)
      }
    }

    val y = new DenseVector(Array(0.0, 2.7, 3.5, 2.1, 1.5))

    withClue("Size of vector must match the rank of matrix") {
      intercept[Exception] {
        syr(alpha, y, dA)
      }
    }

    val xSparse = new SparseVector(4, Array(0, 2, 3), Array(1.0, 3.0, 4.0))
    val dD = new DenseMatrix(4, 4,
      Array(0.0, 1.2, 2.2, 3.1, 1.2, 3.2, 5.3, 4.6, 2.2, 5.3, 1.8, 3.0, 3.1, 4.6, 3.0, 0.8))
    syr(0.1, xSparse, dD)
    val expectedSparse = new DenseMatrix(4, 4,
      Array(0.1, 1.2, 2.5, 3.5, 1.2, 3.2, 5.3, 4.6, 2.5, 5.3, 2.7, 4.2, 3.5, 4.6, 4.2, 2.4))
    assert(dD ~== expectedSparse absTol 1e-15)
  }

  test("gemm") {
    val dA =
      new DenseMatrix(4, 3, Array(0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0))
    val sA = new SparseMatrix(4, 3, Array(0, 1, 3, 4), Array(1, 0, 2, 3), Array(1.0, 2.0, 1.0, 3.0))

    val B = new DenseMatrix(3, 2, Array(1.0, 0.0, 0.0, 0.0, 2.0, 1.0))
    val expected = new DenseMatrix(4, 2, Array(0.0, 1.0, 0.0, 0.0, 4.0, 0.0, 2.0, 3.0))
    val BTman = new DenseMatrix(2, 3, Array(1.0, 0.0, 0.0, 2.0, 0.0, 1.0))
    val BT = B.transpose

    assert(dA.multiply(B) ~== expected absTol 1e-15)
    assert(sA.multiply(B) ~== expected absTol 1e-15)

    val C1 = new DenseMatrix(4, 2, Array(1.0, 0.0, 2.0, 1.0, 0.0, 0.0, 1.0, 0.0))
    val C2 = C1.copy
    val C3 = C1.copy
    val C4 = C1.copy
    val C5 = C1.copy
    val C6 = C1.copy
    val C7 = C1.copy
    val C8 = C1.copy
    val C9 = C1.copy
    val C10 = C1.copy
    val C11 = C1.copy
    val C12 = C1.copy
    val C13 = C1.copy
    val C14 = C1.copy
    val C15 = C1.copy
    val C16 = C1.copy
    val expected2 = new DenseMatrix(4, 2, Array(2.0, 1.0, 4.0, 2.0, 4.0, 0.0, 4.0, 3.0))
    val expected3 = new DenseMatrix(4, 2, Array(2.0, 2.0, 4.0, 2.0, 8.0, 0.0, 6.0, 6.0))
    val expected4 = new DenseMatrix(4, 2, Array(5.0, 0.0, 10.0, 5.0, 0.0, 0.0, 5.0, 0.0))
    val expected5 = C1.copy

    gemm(1.0, dA, B, 2.0, C1)
    gemm(1.0, sA, B, 2.0, C2)
    gemm(2.0, dA, B, 2.0, C3)
    gemm(2.0, sA, B, 2.0, C4)
    assert(C1 ~== expected2 absTol 1e-15)
    assert(C2 ~== expected2 absTol 1e-15)
    assert(C3 ~== expected3 absTol 1e-15)
    assert(C4 ~== expected3 absTol 1e-15)

    withClue("columns of A don't match the rows of B") {
      intercept[Exception] {
        gemm(1.0, dA.transpose, B, 2.0, C1)
      }
    }

    val dATman =
      new DenseMatrix(3, 4, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0))
    val sATman =
      new SparseMatrix(3, 4, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0))

    val dATT = dATman.transpose
    val sATT = sATman.transpose
    val BTT = BTman.transpose.asInstanceOf[DenseMatrix]

    assert(dATT.multiply(B) ~== expected absTol 1e-15)
    assert(sATT.multiply(B) ~== expected absTol 1e-15)
    assert(dATT.multiply(BTT) ~== expected absTol 1e-15)
    assert(sATT.multiply(BTT) ~== expected absTol 1e-15)

    gemm(1.0, dATT, BTT, 2.0, C5)
    gemm(1.0, sATT, BTT, 2.0, C6)
    gemm(2.0, dATT, BTT, 2.0, C7)
    gemm(2.0, sATT, BTT, 2.0, C8)
    gemm(1.0, dA, BTT, 2.0, C9)
    gemm(1.0, sA, BTT, 2.0, C10)
    gemm(2.0, dA, BTT, 2.0, C11)
    gemm(2.0, sA, BTT, 2.0, C12)
    assert(C5 ~== expected2 absTol 1e-15)
    assert(C6 ~== expected2 absTol 1e-15)
    assert(C7 ~== expected3 absTol 1e-15)
    assert(C8 ~== expected3 absTol 1e-15)
    assert(C9 ~== expected2 absTol 1e-15)
    assert(C10 ~== expected2 absTol 1e-15)
    assert(C11 ~== expected3 absTol 1e-15)
    assert(C12 ~== expected3 absTol 1e-15)

    gemm(0, dA, B, 5, C13)
    gemm(0, sA, B, 5, C14)
    gemm(0, dA, B, 1, C15)
    gemm(0, sA, B, 1, C16)
    assert(C13 ~== expected4 absTol 1e-15)
    assert(C14 ~== expected4 absTol 1e-15)
    assert(C15 ~== expected5 absTol 1e-15)
    assert(C16 ~== expected5 absTol 1e-15)

  }

  test("gemv") {

    val dA =
      new DenseMatrix(4, 3, Array(0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0))
    val sA = new SparseMatrix(4, 3, Array(0, 1, 3, 4), Array(1, 0, 2, 3), Array(1.0, 2.0, 1.0, 3.0))

    val dA2 =
      new DenseMatrix(4, 3, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0), true)
    val sA2 =
      new SparseMatrix(4, 3, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0),
        true)

    val dx = new DenseVector(Array(1.0, 2.0, 3.0))
    val sx = dx.toSparse
    val expected = new DenseVector(Array(4.0, 1.0, 2.0, 9.0))

    assert(dA.multiply(dx) ~== expected absTol 1e-15)
    assert(sA.multiply(dx) ~== expected absTol 1e-15)
    assert(dA.multiply(sx) ~== expected absTol 1e-15)
    assert(sA.multiply(sx) ~== expected absTol 1e-15)

    val y1 = new DenseVector(Array(1.0, 3.0, 1.0, 0.0))
    val y2 = y1.copy
    val y3 = y1.copy
    val y4 = y1.copy
    val y5 = y1.copy
    val y6 = y1.copy
    val y7 = y1.copy
    val y8 = y1.copy
    val y9 = y1.copy
    val y10 = y1.copy
    val y11 = y1.copy
    val y12 = y1.copy
    val y13 = y1.copy
    val y14 = y1.copy
    val y15 = y1.copy
    val y16 = y1.copy

    val expected2 = new DenseVector(Array(6.0, 7.0, 4.0, 9.0))
    val expected3 = new DenseVector(Array(10.0, 8.0, 6.0, 18.0))

    gemv(1.0, dA, dx, 2.0, y1)
    gemv(1.0, sA, dx, 2.0, y2)
    gemv(1.0, dA, sx, 2.0, y3)
    gemv(1.0, sA, sx, 2.0, y4)

    gemv(1.0, dA2, dx, 2.0, y5)
    gemv(1.0, sA2, dx, 2.0, y6)
    gemv(1.0, dA2, sx, 2.0, y7)
    gemv(1.0, sA2, sx, 2.0, y8)

    gemv(2.0, dA, dx, 2.0, y9)
    gemv(2.0, sA, dx, 2.0, y10)
    gemv(2.0, dA, sx, 2.0, y11)
    gemv(2.0, sA, sx, 2.0, y12)

    gemv(2.0, dA2, dx, 2.0, y13)
    gemv(2.0, sA2, dx, 2.0, y14)
    gemv(2.0, dA2, sx, 2.0, y15)
    gemv(2.0, sA2, sx, 2.0, y16)

    assert(y1 ~== expected2 absTol 1e-15)
    assert(y2 ~== expected2 absTol 1e-15)
    assert(y3 ~== expected2 absTol 1e-15)
    assert(y4 ~== expected2 absTol 1e-15)

    assert(y5 ~== expected2 absTol 1e-15)
    assert(y6 ~== expected2 absTol 1e-15)
    assert(y7 ~== expected2 absTol 1e-15)
    assert(y8 ~== expected2 absTol 1e-15)

    assert(y9 ~== expected3 absTol 1e-15)
    assert(y10 ~== expected3 absTol 1e-15)
    assert(y11 ~== expected3 absTol 1e-15)
    assert(y12 ~== expected3 absTol 1e-15)

    assert(y13 ~== expected3 absTol 1e-15)
    assert(y14 ~== expected3 absTol 1e-15)
    assert(y15 ~== expected3 absTol 1e-15)
    assert(y16 ~== expected3 absTol 1e-15)

    withClue("columns of A don't match the rows of B") {
      intercept[Exception] {
        gemv(1.0, dA.transpose, dx, 2.0, y1)
      }
      intercept[Exception] {
        gemv(1.0, sA.transpose, dx, 2.0, y1)
      }
      intercept[Exception] {
        gemv(1.0, dA.transpose, sx, 2.0, y1)
      }
      intercept[Exception] {
        gemv(1.0, sA.transpose, sx, 2.0, y1)
      }
    }

    val dAT =
      new DenseMatrix(3, 4, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0))
    val sAT =
      new SparseMatrix(3, 4, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0))

    val dATT = dAT.transpose
    val sATT = sAT.transpose

    assert(dATT.multiply(dx) ~== expected absTol 1e-15)
    assert(sATT.multiply(dx) ~== expected absTol 1e-15)
    assert(dATT.multiply(sx) ~== expected absTol 1e-15)
    assert(sATT.multiply(sx) ~== expected absTol 1e-15)
  }
}
