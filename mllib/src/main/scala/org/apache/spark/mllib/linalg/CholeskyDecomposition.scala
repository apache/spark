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

import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

/**
 * Compute Cholesky decomposition.
 */
private[spark] object CholeskyDecomposition {

  /**
   * Solves a symmetric positive definite linear system via Cholesky factorization.
   * The input arguments are modified in-place to store the factorization and the solution.
   * @param A The upper triangular part of A.
   * @param bx Right-hand side.
   * @return The solution array and a status that identifies whether the decomposition success.
   *         A status value greater than 0 indicates singular matrix error; a status value less
   *         than 0 indicates illegal input; otherwise, success.
   */
  def solve(A: Array[Double], bx: Array[Double]): (Array[Double], Int) = {
    val k = bx.length
    val info = new intW(0)
    lapack.dppsv("U", k, 1, A, bx, k, info)
    (bx, info.`val`)
  }

  /**
   * Computes the inverse of a real symmetric positive definite matrix A
   * using the Cholesky factorization A = U**T*U.
   * The input arguments are modified in-place to store the inverse matrix.
   * @param UAi The upper triangular factor U from the Cholesky factorization A = U**T*U.
   * @param k The dimension of A.
   * @return The upper triangle of the (symmetric) inverse of A and a status that identifies
   *         whether the decomposition success. A status value greater than 0 indicates singular
   *         matrix error; a status value less than 0 indicates illegal input; otherwise, success.
   */
  def inverse(UAi: Array[Double], k: Int): (Array[Double], Int) = {
    val info = new intW(0)
    lapack.dpptri("U", k, UAi, info)
    (UAi, info.`val`)
  }
}
