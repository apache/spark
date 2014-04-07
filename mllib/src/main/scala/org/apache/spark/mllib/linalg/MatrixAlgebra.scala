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

import org.apache.spark.rdd.RDD

import org.jblas.DoubleMatrix

/**
 * Efficient matrix operations.
 */
object MatrixAlgebra {
  /**
   * Given matrix, compute squares of column magnitudes
   * @param matrix given row-by-row, each row an array
   * @return an array of squared column magnitudes
   */
  def columnMagnitudes(matrix: RDD[Array[Double]]):
  Array[Double] = {
    val n = matrix.first.size
    matrix.map {
      x =>
        val a = new DoubleMatrix(x)
        a.mul(a).data
    }.fold(Array.ofDim[Double](n)) {
      (a, b) =>
        val am = new DoubleMatrix(a)
        val bm = new DoubleMatrix(b)
        am.addi(bm)
        a
    }
  }

  /**
   * Given a matrix A, compute A^T A using the sampling procedure
   * described in http://arxiv.org/abs/1304.1467
   * @param matrix given row-by-row, each row an array
   * @param colMags Euclidean column magnitudes squared
   * @param gamma The oversampling parameter, should be set to greater than 1,
   *              guideline is 2 log(n)
   * @return Computed A^T A
   */
  def squareWithDIMSUM(matrix: RDD[Array[Double]], colMags: Array[Double], gamma: Double):
  Array[Array[Double]] = {
    val n = matrix.first.size

    if (gamma < 1) {
      throw new IllegalArgumentException("Oversampling should be greater than 1: $gamma")
    }

    // Compute A^T A
    val fullATA = matrix.mapPartitions {
      iter =>
        val localATA = Array.ofDim[Double](n, n)
        while (iter.hasNext) {
          val row = iter.next()
          var i = 0
          while (i < n) {
            if (Math.random < gamma / colMags(i)) {
              var j = i + 1
              while (j < n) {
                val mult = row(i) * row(j)
                localATA(i)(j) += mult
                localATA(j)(i) += mult
                j += 1
              }
            }
            i += 1
          }
        }
        Iterator(localATA)
    }.fold(Array.ofDim[Double](n, n)) {
      (a, b) =>
        var i = 0
        while (i < n) {
          var j = 0
          while (j < n) {
            a(i)(j) += b(i)(j)
            j += 1
          }
          i += 1
        }
        a
    }

    // undo normalization
    for (i <- 0 until n) for (j <- i until n) {
      fullATA(i)(j) = if (i == j) colMags(i)
      else if (gamma / colMags(i) > 1) fullATA(i)(j)
      else fullATA(i)(j) * colMags(i) / gamma
      fullATA(j)(i) = fullATA(i)(j)
    }

    fullATA
  }

  /**
   * Given a matrix A, compute A^T A
   * @param matrix given row-by-row, each row an array
   * @return Computed A^T A
   */
  def square(matrix: RDD[Array[Double]]): Array[Array[Double]] = {
    val n = matrix.first.size

    // Compute A^T A
    val fullATA = matrix.mapPartitions {
      iter =>
        val localATA = Array.ofDim[Double](n, n)
        while (iter.hasNext) {
          val row = iter.next()
          var i = 0
          while (i < n) {
            var j = 0
            while (j < n) {
              localATA(i)(j) += row(i) * row(j)
              j += 1
            }
            i += 1
          }
        }
        Iterator(localATA)
    }.fold(Array.ofDim[Double](n, n)) {
      (a, b) =>
        var i = 0
        while (i < n) {
          var j = 0
          while (j < n) {
            a(i)(j) += b(i)(j)
            j += 1
          }
          i += 1
        }
        a
    }
    fullATA
  }
}
