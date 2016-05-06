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
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}

/**
 * In-place DGEMM and DGEMV for Breeze
 */
private[ann] object BreezeUtil {

  // TODO: switch to MLlib BLAS interface
  private def transposeString(a: BDM[Double]): String = if (a.isTranspose) "T" else "N"

  /**
   * DGEMM: C := alpha * A * B + beta * C
   * @param alpha alpha
   * @param a A
   * @param b B
   * @param beta beta
   * @param c C
   */
  def dgemm(alpha: Double, a: BDM[Double], b: BDM[Double], beta: Double, c: BDM[Double]): Unit = {
    // TODO: add code if matrices isTranspose!!!
    require(a.cols == b.rows, "A & B Dimension mismatch!")
    require(a.rows == c.rows, "A & C Dimension mismatch!")
    require(b.cols == c.cols, "A & C Dimension mismatch!")
    NativeBLAS.dgemm(transposeString(a), transposeString(b), c.rows, c.cols, a.cols,
      alpha, a.data, a.offset, a.majorStride, b.data, b.offset, b.majorStride,
      beta, c.data, c.offset, c.rows)
  }

  /**
   * DGEMV: y := alpha * A * x + beta * y
   * @param alpha alpha
   * @param a A
   * @param x x
   * @param beta beta
   * @param y y
   */
  def dgemv(alpha: Double, a: BDM[Double], x: BDV[Double], beta: Double, y: BDV[Double]): Unit = {
    require(a.cols == x.length, "A & b Dimension mismatch!")
    NativeBLAS.dgemv(transposeString(a), a.rows, a.cols,
      alpha, a.data, a.offset, a.majorStride, x.data, x.offset, x.stride,
      beta, y.data, y.offset, y.stride)
  }
}
