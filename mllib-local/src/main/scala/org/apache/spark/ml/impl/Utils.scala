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

package org.apache.spark.ml.impl


private[spark] object Utils {

  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  /**
   * Convert an n * (n + 1) / 2 dimension array representing the upper triangular part of a matrix
   * into an n * n array representing the full symmetric matrix (column major).
   *
   * @param n The order of the n by n matrix.
   * @param triangularValues The upper triangular part of the matrix packed in an array
   *                         (column major).
   * @return A dense matrix which represents the symmetric matrix in column major.
   */
  def unpackUpperTriangular(
      n: Int,
      triangularValues: Array[Double]): Array[Double] = {
    val symmetricValues = new Array[Double](n * n)
    var r = 0
    var i = 0
    while (i < n) {
      var j = 0
      while (j <= i) {
        symmetricValues(i * n + j) = triangularValues(r)
        symmetricValues(j * n + i) = triangularValues(r)
        r += 1
        j += 1
      }
      i += 1
    }
    symmetricValues
  }

  /**
   * Indexing in an array representing the upper triangular part of a matrix
   * into an n * n array representing the full symmetric matrix (column major).
   *    val symmetricValues = unpackUpperTriangularMatrix(n, triangularValues)
   *    val matrix = new DenseMatrix(n, n, symmetricValues)
   *    val index = indexUpperTriangularMatrix(n, i, j)
   *    then: symmetricValues(index) == matrix(i, j)
   *
   * @param n The order of the n by n matrix.
   */
  def indexUpperTriangular(
      n: Int,
      i: Int,
      j: Int): Int = {
    require(i >= 0 && i < n, s"Expected 0 <= i < $n, got i = $i.")
    require(j >= 0 && j < n, s"Expected 0 <= j < $n, got j = $j.")
    if (i <= j) {
      j * (j + 1) / 2 + i
    } else {
      i * (i + 1) / 2 + j
    }
  }

  /**
   * When `x` is positive and large, computing `math.log(1 + math.exp(x))` will lead to arithmetic
   * overflow. This will happen when `x &gt; 709.78` which is not a very large number.
   * It can be addressed by rewriting the formula into `x + math.log1p(math.exp(-x))`
   * when `x` is positive.
   * @param x a floating-point value as input.
   * @return the result of `math.log(1 + math.exp(x))`.
   */
  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}
