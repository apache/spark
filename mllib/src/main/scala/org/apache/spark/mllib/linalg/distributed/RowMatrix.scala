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

package org.apache.spark.mllib.linalg.distributed

import java.util

import breeze.linalg.{Vector => BV, DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import breeze.linalg.{svd => brzSvd, axpy => brzAxpy}
import breeze.numerics.{sqrt => brzSqrt}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

/**
 * Column statistics aggregator implementing
 * [[org.apache.spark.mllib.stat.MultivariateStatisticalSummary]]
 * together with add() and merge() function.
 * A numerically stable algorithm is implemented to compute sample mean and variance:
  *[[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance variance-wiki]].
 * Zero elements (including explicit zero values) are skipped when calling add() and merge(),
 * to have time complexity O(nnz) instead of O(n) for each column.
 */
private class ColumnStatisticsAggregator(private val n: Int)
    extends MultivariateStatisticalSummary with Serializable {

  private val currMean: BDV[Double] = BDV.zeros[Double](n)
  private val currM2n: BDV[Double] = BDV.zeros[Double](n)
  private var totalCnt = 0.0
  private val nnz: BDV[Double] = BDV.zeros[Double](n)
  private val currMax: BDV[Double] = BDV.fill(n)(Double.MinValue)
  private val currMin: BDV[Double] = BDV.fill(n)(Double.MaxValue)

  override def mean: Vector = {
    val realMean = BDV.zeros[Double](n)
    var i = 0
    while (i < n) {
      realMean(i) = currMean(i) * nnz(i) / totalCnt
      i += 1
    }
    Vectors.fromBreeze(realMean)
  }

  override def variance: Vector = {
    val realVariance = BDV.zeros[Double](n)

    val denominator = totalCnt - 1.0

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean
      var i = 0
      while (i < currM2n.size) {
        realVariance(i) =
          currM2n(i) + deltaMean(i) * deltaMean(i) * nnz(i) * (totalCnt - nnz(i)) / totalCnt
        realVariance(i) /= denominator
        i += 1
      }
    }

    Vectors.fromBreeze(realVariance)
  }

  override def count: Long = totalCnt.toLong

  override def numNonzeros: Vector = Vectors.fromBreeze(nnz)

  override def max: Vector = {
    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0
      i += 1
    }
    Vectors.fromBreeze(currMax)
  }

  override def min: Vector = {
    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0
      i += 1
    }
    Vectors.fromBreeze(currMin)
  }

  /**
   * Aggregates a row.
   */
  def add(currData: BV[Double]): this.type = {
    currData.activeIterator.foreach {
      case (_, 0.0) => // Skip explicit zero elements.
      case (i, value) =>
        if (currMax(i) < value) {
          currMax(i) = value
        }
        if (currMin(i) > value) {
          currMin(i) = value
        }

        val tmpPrevMean = currMean(i)
        currMean(i) = (currMean(i) * nnz(i) + value) / (nnz(i) + 1.0)
        currM2n(i) += (value - currMean(i)) * (value - tmpPrevMean)

        nnz(i) += 1.0
    }

    totalCnt += 1.0
    this
  }

  /**
   * Merges another aggregator.
   */
  def merge(other: ColumnStatisticsAggregator): this.type = {
    require(n == other.n, s"Dimensions mismatch. Expecting $n but got ${other.n}.")

    totalCnt += other.totalCnt
    val deltaMean = currMean - other.currMean

    var i = 0
    while (i < n) {
      // merge mean together
      if (other.currMean(i) != 0.0) {
        currMean(i) = (currMean(i) * nnz(i) + other.currMean(i) * other.nnz(i)) /
          (nnz(i) + other.nnz(i))
      }
      // merge m2n together
      if (nnz(i) + other.nnz(i) != 0.0) {
        currM2n(i) += other.currM2n(i) + deltaMean(i) * deltaMean(i) * nnz(i) * other.nnz(i) /
          (nnz(i) + other.nnz(i))
      }
      if (currMax(i) < other.currMax(i)) {
        currMax(i) = other.currMax(i)
      }
      if (currMin(i) > other.currMin(i)) {
        currMin(i) = other.currMin(i)
      }
      i += 1
    }

    nnz += other.nnz
    this
  }
}

/**
 * :: Experimental ::
 * Represents a row-oriented distributed Matrix with no meaningful row indices.
 *
 * @param rows rows stored as an RDD[Vector]
 * @param nRows number of rows. A non-positive value means unknown, and then the number of rows will
 *              be determined by the number of records in the RDD `rows`.
 * @param nCols number of columns. A non-positive value means unknown, and then the number of
 *              columns will be determined by the size of the first row.
 */
@Experimental
class RowMatrix(
    val rows: RDD[Vector],
    private var nRows: Long,
    private var nCols: Int) extends DistributedMatrix with Logging {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(rows: RDD[Vector]) = this(rows, 0L, 0)

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (nCols <= 0) {
      // Calling `first` will throw an exception if `rows` is empty.
      nCols = rows.first().size
    }
    nCols
  }

  /** Gets or computes the number of rows. */
  override def numRows(): Long = {
    if (nRows <= 0L) {
      nRows = rows.count()
      if (nRows == 0L) {
        sys.error("Cannot determine the number of rows because it is not specified in the " +
          "constructor and the rows RDD is empty.")
      }
    }
    nRows
  }

  /**
   * Multiply the Gramian matrix `A^T A` by a DenseVector on the right.
   *
   * @param v a local DenseVector whose length must match the number of columns of this matrix.
   * @return a local DenseVector representing the product.
   */
  private[mllib] def multiplyGramianMatrixBy(v: DenseVector): DenseVector = {
    val n = numCols().toInt
    val vbr = rows.context.broadcast(v.toBreeze)

    val bv = rows.aggregate(BDV.zeros[Double](n))(
      seqOp = (U, r) => {
        val rBrz = r.toBreeze
        val a = rBrz.dot(vbr.value)
        rBrz match {
          case _: BDV[_] => brzAxpy(a, rBrz.asInstanceOf[BDV[Double]], U)
          case _: BSV[_] => brzAxpy(a, rBrz.asInstanceOf[BSV[Double]], U)
          case _ =>
            throw new UnsupportedOperationException(
              s"Do not support vector operation from type ${rBrz.getClass.getName}.")
        }
        U
      },
      combOp = (U1, U2) => U1 += U2
    )

    Vectors.fromBreeze(bv).asInstanceOf[DenseVector]
  }

  /**
   * Computes the Gramian matrix `A^T A`.
   */
  def computeGramianMatrix(): Matrix = {
    val n = numCols().toInt
    val nt: Int = n * (n + 1) / 2

    // Compute the upper triangular part of the gram matrix.
    val GU = rows.aggregate(new BDV[Double](new Array[Double](nt)))(
      seqOp = (U, v) => {
        RowMatrix.dspr(1.0, v, U.data)
        U
      },
      combOp = (U1, U2) => U1 += U2
    )

    RowMatrix.triuToFull(n, GU.data)
  }

  /**
   * Computes singular value decomposition of this matrix using dense implementation.
   * Denote this matrix by A (m x n), this will compute matrices U, S, V such that A ~= U * S * V',
   * where S contains the leading singular values, U and V contain the corresponding singular
   * vectors.
   *
   * This approach requires `n^2` doubles to fit in memory and `O(n^3)` time on the master node.
   * Further, n should be less than m. For problems with small n (e.g. n < 100 and n << m), the
   * dense implementation might be faster than the sparse implementation.
   *
   * At most k largest non-zero singular values and associated vectors are returned.
   * If there are k such values, then the dimensions of the return will be:
   *
   * U is a RowMatrix of size m x k that satisfies U'U = eye(k),
   * s is a Vector of size k, holding the singular values in descending order,
   * and V is a Matrix of size n x k that satisfies V'V = eye(k).
   *
   * @param k number of leading singular values to keep (0 < k <= n). We might return less than
   *          k if there are numerically zero singular values. See rCond.
   * @param computeU whether to compute U.
   * @param rCond the reciprocal condition number. All singular values smaller than rCond * sigma(0)
   *              are treated as zero, where sigma(0) is the largest singular value.
   * @return SingularValueDecomposition(U, s, V)
   */
  def computeSVD(
      k: Int,
      computeU: Boolean,
      rCond: Double): SingularValueDecomposition[RowMatrix, Matrix] = {
    val n = numCols().toInt
    require(k > 0 && k <= n, s"Request up to n singular values k=$k n=$n.")
    val G = computeGramianMatrix()
    val (uFull: BDM[Double], sigmaSquaresFull: BDV[Double], vFull: BDM[Double]) =
      brzSvd(G.toBreeze.asInstanceOf[BDM[Double]])
    computeSVDEffectiveRank(k, n, computeU, rCond, sigmaSquaresFull, uFull)
  }

  /**
   * Computes singular value decomposition of this matrix using dense implementation with default
   * reciprocal condition number (1e-9). See computeSVD for more details.
   *
   * @param k number of leading singular values to keep (0 < k <= n). We might return less than
   *          k if there are numerically zero singular values.
   * @param computeU whether to compute U.
   * @return SingularValueDecomposition(U, s, V)
   */
  def computeSVD(
      k: Int,
      computeU: Boolean = false): SingularValueDecomposition[RowMatrix, Matrix] = {
    computeSVD(k, computeU, 1e-9)
  }

  /**
   * Computes singular value decomposition of this matrix using sparse implementation.
   * Denote this matrix by A (m x n), this will compute matrices U, S, V such that A ~= U * S * V',
   * where S contains the leading singular values, U and V contain the corresponding singular
   * vectors.
   *
   * The decomposition is computed by providing a function that multiples a vector with A'A to
   * ARPACK, and iteratively invoking ARPACK-dsaupd on master node, from which we recover S and V.
   * Then we compute U via easy matrix multiplication as U =  A * (V * S^{-1}).
   * Note that this approach requires approximately `O(k * nnz(A))` time.
   *
   * There is no restriction on m, but we require `n*(6*k+4)` doubles to fit in memory on the master
   * node. Further, n should be less than m, and ARPACK requires k to be strictly less than n. If
   * the requested k = n, please use the dense implementation computeSVD.
   *
   * At most k largest non-zero singular values and associated vectors are returned.
   * If there are k such values, then the dimensions of the return will be:
   *
   * U is a RowMatrix of size m x k that satisfies U'U = eye(k),
   * s is a Vector of size k, holding the singular values in descending order,
   * and V is a Matrix of size n x k that satisfies V'V = eye(k).
   *
   * @param k number of leading singular values to keep (0 < k < n). We might return less than
   *          k if there are numerically zero singular values. See rCond.
   * @param computeU whether to compute U.
   * @param rCond the reciprocal condition number. All singular values smaller than rCond * sigma(0)
   *              are treated as zero, where sigma(0) is the largest singular value.
   * @param tol the numerical tolerance of SVD computation. Larger tolerance means fewer iterations,
   *            but less accurate result.
   * @param maxIterations the maximum number of Arnoldi update iterations.
   * @return SingularValueDecomposition(U, s, V)
   */
  def computeSparseSVD(
      k: Int,
      computeU: Boolean,
      rCond: Double,
      tol: Double,
      maxIterations: Int): SingularValueDecomposition[RowMatrix, Matrix] = {
    val n = numCols().toInt
    require(k > 0 && k < n, s"Request up to n - 1 singular values k=$k n=$n. " +
        s"For full SVD (i.e. k = n), please use dense implementation computeSVD.")
    val (sigmaSquares: BDV[Double], u: BDM[Double]) =
      EigenValueDecomposition.symmetricEigs(multiplyGramianMatrixBy, n, k, tol, maxIterations)
    computeSVDEffectiveRank(k, n, computeU, rCond, sigmaSquares, u)
  }

  /**
   * Computes singular value decomposition of this matrix using sparse implementation with default
   * reciprocal condition number (1e-9), tolerance (1e-10), and maximum number of Arnoldi iterations
   * (300). See computeSparseSVD for more details.
   *
   * @param k number of leading singular values to keep (0 < k < n). We might return less than
   *          k if there are numerically zero singular values.
   * @param computeU whether to compute U.
   * @return SingularValueDecomposition(U, s, V)
   */
  def computeSparseSVD(
      k: Int,
      computeU: Boolean = false): SingularValueDecomposition[RowMatrix, Matrix] = {
    computeSparseSVD(k, computeU, 1e-9, 1e-10, 300)
  }

  /**
   * Determine effective rank of SVD result and compute left singular vectors if required.
   */
  private def computeSVDEffectiveRank(
      k: Int,
      n: Int,
      computeU: Boolean,
      rCond: Double,
      sigmaSquares: BDV[Double],
      u: BDM[Double]): SingularValueDecomposition[RowMatrix, Matrix] = {
    val sigmas: BDV[Double] = brzSqrt(sigmaSquares)

    // Determine effective rank.
    val sigma0 = sigmas(0)
    val threshold = rCond * sigma0
    var i = 0
    // sigmas might have a length smaller than k, if some Ritz values do not satisfy the
    // convergence criterion specified by tol after maxIterations.
    // Thus use i < min(k, sigmas.length) instead of i < k
    if (sigmas.length < k) {
      logWarning(s"Requested $k singular values but only found ${sigmas.length} converged.")
    }
    while (i < math.min(k, sigmas.length) && sigmas(i) >= threshold) {
      i += 1
    }
    val sk = i

    if (sk < k) {
      logWarning(s"Requested $k singular values but only found $sk nonzeros.")
    }

    val s = Vectors.dense(util.Arrays.copyOfRange(sigmas.data, 0, sk))
    val V = Matrices.dense(n, sk, util.Arrays.copyOfRange(u.data, 0, n * sk))

    if (computeU) {
      // N = Vk * Sk^{-1}
      val N = new BDM[Double](n, sk, util.Arrays.copyOfRange(u.data, 0, n * sk))
      var i = 0
      var j = 0
      while (j < sk) {
        i = 0
        val sigma = sigmas(j)
        while (i < n) {
          N(i, j) /= sigma
          i += 1
        }
        j += 1
      }
      val U = this.multiply(Matrices.fromBreeze(N))
      SingularValueDecomposition(U, s, V)
    } else {
      SingularValueDecomposition(null, s, V)
    }
  }

  /**
   * Computes the covariance matrix, treating each row as an observation.
   * @return a local dense matrix of size n x n
   */
  def computeCovariance(): Matrix = {
    val n = numCols().toInt

    if (n > 10000) {
      val mem = n * n * java.lang.Double.SIZE / java.lang.Byte.SIZE
      logWarning(s"The number of columns $n is greater than 10000! " +
        s"We need at least $mem bytes of memory.")
    }

    val (m, mean) = rows.aggregate[(Long, BDV[Double])]((0L, BDV.zeros[Double](n)))(
      seqOp = (s: (Long, BDV[Double]), v: Vector) => (s._1 + 1L, s._2 += v.toBreeze),
      combOp = (s1: (Long, BDV[Double]), s2: (Long, BDV[Double])) => (s1._1 + s2._1, s1._2 += s2._2)
    )

    updateNumRows(m)

    mean :/= m.toDouble

    // We use the formula Cov(X, Y) = E[X * Y] - E[X] E[Y], which is not accurate if E[X * Y] is
    // large but Cov(X, Y) is small, but it is good for sparse computation.
    // TODO: find a fast and stable way for sparse data.

    val G = computeGramianMatrix().toBreeze.asInstanceOf[BDM[Double]]

    var i = 0
    var j = 0
    val m1 = m - 1.0
    var alpha = 0.0
    while (i < n) {
      alpha = m / m1 * mean(i)
      j = 0
      while (j < n) {
        G(i, j) = G(i, j) / m1 - alpha * mean(j)
        j += 1
      }
      i += 1
    }

    Matrices.fromBreeze(G)
  }

  /**
   * Computes the top k principal components.
   * Rows correspond to observations and columns correspond to variables.
   * The principal components are stored a local matrix of size n-by-k.
   * Each column corresponds for one principal component,
   * and the columns are in descending order of component variance.
   *
   * @param k number of top principal components.
   * @return a matrix of size n-by-k, whose columns are principal components
   */
  def computePrincipalComponents(k: Int): Matrix = {
    val n = numCols().toInt
    require(k > 0 && k <= n, s"k = $k out of range (0, n = $n]")

    val Cov = computeCovariance().toBreeze.asInstanceOf[BDM[Double]]

    val (u: BDM[Double], _, _) = brzSvd(Cov)

    if (k == n) {
      Matrices.dense(n, k, u.data)
    } else {
      Matrices.dense(n, k, util.Arrays.copyOfRange(u.data, 0, n * k))
    }
  }

  /**
   * Computes column-wise summary statistics.
   */
  def computeColumnSummaryStatistics(): MultivariateStatisticalSummary = {
    val zeroValue = new ColumnStatisticsAggregator(numCols().toInt)
    val summary = rows.map(_.toBreeze).aggregate[ColumnStatisticsAggregator](zeroValue)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
    )
    updateNumRows(summary.count)
    summary
  }

  /**
   * Multiply this matrix by a local matrix on the right.
   *
   * @param B a local matrix whose number of rows must match the number of columns of this matrix
   * @return a [[org.apache.spark.mllib.linalg.distributed.RowMatrix]] representing the product,
   *         which preserves partitioning
   */
  def multiply(B: Matrix): RowMatrix = {
    val n = numCols().toInt
    require(n == B.numRows, s"Dimension mismatch: $n vs ${B.numRows}")

    require(B.isInstanceOf[DenseMatrix],
      s"Only support dense matrix at this time but found ${B.getClass.getName}.")

    val Bb = rows.context.broadcast(B)
    val AB = rows.mapPartitions({ iter =>
      val Bi = Bb.value.toBreeze.asInstanceOf[BDM[Double]]
      iter.map(v => Vectors.fromBreeze(Bi.t * v.toBreeze))
    }, preservesPartitioning = true)

    new RowMatrix(AB, nRows, B.numCols)
  }

  private[mllib] override def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    var i = 0
    rows.collect().foreach { v =>
      v.toBreeze.activeIterator.foreach { case (j, v) =>
        mat(i, j) = v
      }
      i += 1
    }
    mat
  }

  /** Updates or verfires the number of rows. */
  private def updateNumRows(m: Long) {
    if (nRows <= 0) {
      nRows == m
    } else {
      require(nRows == m,
        s"The number of rows $m is different from what specified or previously computed: ${nRows}.")
    }
  }
}

@Experimental
object RowMatrix {

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
        blas.dspr("U", n, alpha, dv.values, 1, U)
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
