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

package org.apache.spark.mllib.linalg.rdd

import java.util

import scala.util.control.Breaks._

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, svd => brzSvd}
import breeze.numerics.{sqrt => brzSqrt}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

/**
 * Represents a row-oriented RDDMatrix with no meaningful row indices.
 *
 * @param rows rows stored as an RDD[Vector]
 * @param m number of rows
 * @param n number of columns
 */
class RowRDDMatrix(
    val rows: RDD[Vector],
    m: Long = -1L,
    n: Long = -1) extends RDDMatrix {

  private var _m = m
  private var _n = n

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (_n < 0) {
      _n = rows.first().size
    }
    _n
  }

  /** Gets or computes the number of rows. */
  override def numRows(): Long = {
    if (_m < 0) {
      _m = rows.count()
    }
    _m
  }

  /**
   * Computes the gram matrix `A^T A`.
   */
  def gram(): Matrix = {
    val n = numCols().toInt
    val nt: Int = n * (n + 1) / 2

    // Compute the upper triangular part of the gram matrix.
    val GU = rows.aggregate(new BDV[Double](new Array[Double](nt)))(
      seqOp = (U, v) => {
        RowRDDMatrix.dspr(1.0, v, U.data)
        U
      },
      combOp = (U1, U2) => U1 += U2
    )

    RowRDDMatrix.triuToFull(n, GU.data)
  }


  /**
   * Singular Value Decomposition for Tall and Skinny matrices.
   * Given an m x n matrix A, this will compute matrices U, S, V such that
   * A = U * S * V'
   *
   * There is no restriction on m, but we require n^2 doubles to fit in memory.
   * Further, n should be less than m.
   *
   * The decomposition is computed by first computing A'A = V S^2 V',
   * computing svd locally on that (since n x n is small),
   * from which we recover S and V.
   * Then we compute U via easy matrix multiplication
   * as U =  A * V * S^-1
   *
   * Only the k largest singular values and associated vectors are found.
   * If there are k such values, then the dimensions of the return will be:
   *
   * S is k x k and diagonal, holding the singular values on diagonal
   * U is m x k and satisfies U'U = eye(k)
   * V is n x k and satisfies V'V = eye(k)
   *
   * The return values are as lean as possible: an RDD of rows for U,
   * a simple array for sigma, and a dense 2d matrix array for V
   *
   * @param matrix dense matrix to factorize
   * @return Three matrices: U, S, V such that A = USV^T
   */
  def computeSVD(
      k: Int,
      computeU: Boolean = false,
      rCond: Double = 1e-9): SingularValueDecomposition[RowRDDMatrix, Matrix] = {

    val n = numCols().toInt

    require(k >= 0 && k <= n, s"Request up to n singular values k=$k n=$n.")

    val G = gram()

    // TODO: Use sparse SVD instead.
    val (u: BDM[Double], sigmaSquares: BDV[Double], v: BDM[Double]) =
      brzSvd(G.toBreeze.asInstanceOf[BDM[Double]])
    val sigmas: BDV[Double] = brzSqrt(sigmaSquares)

    // Determine effective rank.
    val sigma0 = sigmas(0)
    val threshold = rCond * sigma0
    var i = 0
    breakable {
      while (i < k) {
        if (sigmas(i) < threshold) {
          break()
        }
        i += 1
      }
    }
    val sk = i

    val s = Vectors.dense(util.Arrays.copyOfRange(sigmas.data, 0, sk))
    val V = Matrices.dense(n, sk, util.Arrays.copyOfRange(u.data, 0, n * sk))

    if (computeU) {
      val N = new BDM[Double](sk, n, util.Arrays.copyOfRange(v.data, 0, sk * n))
      var i = 0
      var j = 0
      while (i < sk) {
        j = 0
        val sigma = sigmas(i)
        while (j < n) {
          N(i, j) /= sigma
          j += 1
        }
        i += 1
      }
      val Nb = rows.context.broadcast(N)
      val rowsU = rows.map { row =>
        Vectors.fromBreeze(Nb.value * row.toBreeze)
      }
      SingularValueDecomposition(new RowRDDMatrix(rowsU, _m, n), s, V)
    } else {
      SingularValueDecomposition(null, s, V)
    }
  }
}

object RowRDDMatrix {

  /**
   * Adds alpha * x * x.t to a matrix in-place. This is the same as BLAS's DSPR.
   *
   * @param U the upper triangular part of the matrix packed in an array (column major)
   */
  private def dspr(alpha: Double, v: Vector, U: Array[Double]): Unit = {
    // TODO: Find a better home (breeze?) for this method.
    val n = v.size
    v match {
      case dv: DenseVector =>
        blas.dspr("U", n, 1.0, dv.values, 1, U)
      case sv: SparseVector =>
        val indices = sv.indices
        val values = sv.values
        val nnz = indices.length
        var colStartIdx = 0
        var prevCol = 0
        var col = 0
        var j = 0
        var i = 0
        var av = 0.0
        while (j < nnz) {
          col = indices(j)
          // Skip empty columns.
          colStartIdx += (col - prevCol) * (col + prevCol + 1) / 2
          col = indices(j)
          av = alpha * values(j)
          i = 0
          while (i <= j) {
            U(colStartIdx + indices(i)) += av * values(i)
            i += 1
          }
          j += 1
          prevCol = col
        }
    }
  }

  /**
   * Fills a full square matrix from its upper triangular part.
   */
  private def triuToFull(n: Int, U: Array[Double]): Matrix = {
    val G = new BDM[Double](n, n)

    var row = 0
    var col = 0
    var idx = 0
    var value = 0.0
    while (col < n) {
      row = 0
      while (row < col) {
        value = U(idx)
        G(row, col) = value
        G(col, row) = value
        idx += 1
        row += 1
      }
      G(col, col) = U(idx)
      idx += 1
      col +=1
    }

    Matrices.dense(n, n, G.data)
  }
}
