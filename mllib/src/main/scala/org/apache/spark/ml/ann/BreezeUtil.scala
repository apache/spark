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

package org.apache.spark.ml.ann

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}

import org.apache.spark.ml.linalg.BLAS

/**
 * In-place DGEMM and DGEMV for Breeze
 */
private[ann] object BreezeUtil {

  // TODO: switch to MLlib BLAS interface
  private def transposeString(A: BDM[Double]): String = if (A.isTranspose) "T" else "N"

  /**
   * DGEMM: C := alpha * A * B + beta * C
   * @param alpha alpha
   * @param A A
   * @param B B
   * @param beta beta
   * @param C C
   */
  def dgemm(alpha: Double, A: BDM[Double], B: BDM[Double], beta: Double, C: BDM[Double]): Unit = {
    // TODO: add code if matrices isTranspose!!!
    require(A.cols == B.rows, "A & B Dimension mismatch!")
    require(A.rows == C.rows, "A & C Dimension mismatch!")
    require(B.cols == C.cols, "A & C Dimension mismatch!")
    BLAS.nativeBLAS.dgemm(transposeString(A), transposeString(B), C.rows, C.cols, A.cols,
      alpha, A.data, A.offset, A.majorStride, B.data, B.offset, B.majorStride,
      beta, C.data, C.offset, C.rows)
  }

  /**
   * DGEMV: y := alpha * A * x + beta * y
   * @param alpha alpha
   * @param A A
   * @param x x
   * @param beta beta
   * @param y y
   */
  def dgemv(alpha: Double, A: BDM[Double], x: BDV[Double], beta: Double, y: BDV[Double]): Unit = {
    require(A.cols == x.length, "A & x Dimension mismatch!")
    require(A.rows == y.length, "A & y Dimension mismatch!")
    BLAS.nativeBLAS.dgemv(transposeString(A), A.rows, A.cols,
      alpha, A.data, A.offset, A.majorStride, x.data, x.offset, x.stride,
      beta, y.data, y.offset, y.stride)
  }
}
