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

import java.util.Arrays

import scala.collection.mutable.ListBuffer

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, axpy => brzAxpy,
  svd => brzSvd}
import breeze.numerics.{sqrt => brzSqrt}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.mllib.stat.{MultivariateOnlineSummarizer, MultivariateStatisticalSummary}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.storage.StorageLevel

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
      try {
        // Calling `first` will throw an exception if `rows` is empty.
        nCols = rows.first().size
      } catch {
        case err: UnsupportedOperationException =>
          sys.error("Cannot determine the number of cols because it is not specified in the " +
            "constructor and the rows RDD is empty.")
      }
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
   * Multiplies the Gramian matrix `A^T A` by a dense vector on the right without computing `A^T A`.
   *
   * @param v a dense vector whose length must match the number of columns of this matrix
   * @return a dense vector representing the product
   */
  private[mllib] def multiplyGramianMatrixBy(v: BDV[Double]): BDV[Double] = {
    val n = numCols().toInt
    val vbr = rows.context.broadcast(v)
    rows.treeAggregate(BDV.zeros[Double](n))(
      seqOp = (U, r) => {
        val rBrz = r.toBreeze
        val a = rBrz.dot(vbr.value)
        rBrz match {
          // use specialized axpy for better performance
          case _: BDV[_] => brzAxpy(a, rBrz.asInstanceOf[BDV[Double]], U)
          case _: BSV[_] => brzAxpy(a, rBrz.asInstanceOf[BSV[Double]], U)
          case _ => throw new UnsupportedOperationException(
            s"Do not support vector operation from type ${rBrz.getClass.getName}.")
        }
        U
      }, combOp = (U1, U2) => U1 += U2)
  }

  /**
   * Computes the Gramian matrix `A^T A`.
   */
  def computeGramianMatrix(): Matrix = {
    val n = numCols().toInt
    checkNumColumns(n)
    // Computes n*(n+1)/2, avoiding overflow in the multiplication.
    // This succeeds when n <= 65535, which is checked above
    val nt: Int = if (n % 2 == 0) ((n / 2) * (n + 1)) else (n * ((n + 1) / 2))

    // Compute the upper triangular part of the gram matrix.
    val GU = rows.treeAggregate(new BDV[Double](new Array[Double](nt)))(
      seqOp = (U, v) => {
        RowMatrix.dspr(1.0, v, U.data)
        U
      }, combOp = (U1, U2) => U1 += U2)

    RowMatrix.triuToFull(n, GU.data)
  }

  private def checkNumColumns(cols: Int): Unit = {
    if (cols > 65535) {
      throw new IllegalArgumentException(s"Argument with more than 65535 cols: $cols")
    }
    if (cols > 10000) {
      val mem = cols * cols * 8
      logWarning(s"$cols columns will require at least $mem bytes of memory!")
    }
  }

  /**
   * Computes singular value decomposition of this matrix. Denote this matrix by A (m x n). This
   * will compute matrices U, S, V such that A ~= U * S * V', where S contains the leading k
   * singular values, U and V contain the corresponding singular vectors.
   *
   * At most k largest non-zero singular values and associated vectors are returned. If there are k
   * such values, then the dimensions of the return will be:
   *  - U is a RowMatrix of size m x k that satisfies U' * U = eye(k),
   *  - s is a Vector of size k, holding the singular values in descending order,
   *  - V is a Matrix of size n x k that satisfies V' * V = eye(k).
   *
   * We assume n is smaller than m. The singular values and the right singular vectors are derived
   * from the eigenvalues and the eigenvectors of the Gramian matrix A' * A. U, the matrix
   * storing the right singular vectors, is computed via matrix multiplication as
   * U = A * (V * S^-1^), if requested by user. The actual method to use is determined
   * automatically based on the cost:
   *  - If n is small (n &lt; 100) or k is large compared with n (k > n / 2), we compute the Gramian
   *    matrix first and then compute its top eigenvalues and eigenvectors locally on the driver.
   *    This requires a single pass with O(n^2^) storage on each executor and on the driver, and
   *    O(n^2^ k) time on the driver.
   *  - Otherwise, we compute (A' * A) * v in a distributive way and send it to ARPACK's DSAUPD to
   *    compute (A' * A)'s top eigenvalues and eigenvectors on the driver node. This requires O(k)
   *    passes, O(n) storage on each executor, and O(n k) storage on the driver.
   *
   * Several internal parameters are set to default values. The reciprocal condition number rCond
   * is set to 1e-9. All singular values smaller than rCond * sigma(0) are treated as zeros, where
   * sigma(0) is the largest singular value. The maximum number of Arnoldi update iterations for
   * ARPACK is set to 300 or k * 3, whichever is larger. The numerical tolerance for ARPACK's
   * eigen-decomposition is set to 1e-10.
   *
   * @note The conditions that decide which method to use internally and the default parameters are
   *       subject to change.
   *
   * @param k number of leading singular values to keep (0 &lt; k &lt;= n).
   *          It might return less than k if
   *          there are numerically zero singular values or there are not enough Ritz values
   *          converged before the maximum number of Arnoldi update iterations is reached (in case
   *          that matrix A is ill-conditioned).
   * @param computeU whether to compute U
   * @param rCond the reciprocal condition number. All singular values smaller than rCond * sigma(0)
   *              are treated as zero, where sigma(0) is the largest singular value.
   * @return SingularValueDecomposition(U, s, V). U = null if computeU = false.
   */
  def computeSVD(
      k: Int,
      computeU: Boolean = false,
      rCond: Double = 1e-9): SingularValueDecomposition[RowMatrix, Matrix] = {
    // maximum number of Arnoldi update iterations for invoking ARPACK
    val maxIter = math.max(300, k * 3)
    // numerical tolerance for invoking ARPACK
    val tol = 1e-10
    computeSVD(k, computeU, rCond, maxIter, tol, "auto")
  }

  /**
   * The actual SVD implementation, visible for testing.
   *
   * @param k number of leading singular values to keep (0 &lt; k &lt;= n)
   * @param computeU whether to compute U
   * @param rCond the reciprocal condition number
   * @param maxIter max number of iterations (if ARPACK is used)
   * @param tol termination tolerance (if ARPACK is used)
   * @param mode computation mode (auto: determine automatically which mode to use,
   *             local-svd: compute gram matrix and computes its full SVD locally,
   *             local-eigs: compute gram matrix and computes its top eigenvalues locally,
   *             dist-eigs: compute the top eigenvalues of the gram matrix distributively)
   * @return SingularValueDecomposition(U, s, V). U = null if computeU = false.
   */
  private[mllib] def computeSVD(
      k: Int,
      computeU: Boolean,
      rCond: Double,
      maxIter: Int,
      tol: Double,
      mode: String): SingularValueDecomposition[RowMatrix, Matrix] = {
    val n = numCols().toInt
    require(k > 0 && k <= n, s"Request up to n singular values but got k=$k and n=$n.")

    object SVDMode extends Enumeration {
      val LocalARPACK, LocalLAPACK, DistARPACK = Value
    }

    val computeMode = mode match {
      case "auto" =>
        // TODO: The conditions below are not fully tested.
        if (n < 100 || k > n / 2) {
          // If n is small or k is large compared with n, we better compute the Gramian matrix first
          // and then compute its eigenvalues locally, instead of making multiple passes.
          if (k < n / 3) {
            SVDMode.LocalARPACK
          } else {
            SVDMode.LocalLAPACK
          }
        } else {
          // If k is small compared with n, we use ARPACK with distributed multiplication.
          SVDMode.DistARPACK
        }
      case "local-svd" => SVDMode.LocalLAPACK
      case "local-eigs" => SVDMode.LocalARPACK
      case "dist-eigs" => SVDMode.DistARPACK
      case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
    }

    // Compute the eigen-decomposition of A' * A.
    val (sigmaSquares: BDV[Double], u: BDM[Double]) = computeMode match {
      case SVDMode.LocalARPACK =>
        require(k < n, s"k must be smaller than n in local-eigs mode but got k=$k and n=$n.")
        val G = computeGramianMatrix().toBreeze.asInstanceOf[BDM[Double]]
        EigenValueDecomposition.symmetricEigs(v => G * v, n, k, tol, maxIter)
      case SVDMode.LocalLAPACK =>
        val G = computeGramianMatrix().toBreeze.asInstanceOf[BDM[Double]]
        val brzSvd.SVD(uFull: BDM[Double], sigmaSquaresFull: BDV[Double], _) = brzSvd(G)
        (sigmaSquaresFull, uFull)
      case SVDMode.DistARPACK =>
        if (rows.getStorageLevel == StorageLevel.NONE) {
          logWarning("The input data is not directly cached, which may hurt performance if its"
            + " parent RDDs are also uncached.")
        }
        require(k < n, s"k must be smaller than n in dist-eigs mode but got k=$k and n=$n.")
        EigenValueDecomposition.symmetricEigs(multiplyGramianMatrixBy, n, k, tol, maxIter)
    }

    val sigmas: BDV[Double] = brzSqrt(sigmaSquares)

    // Determine the effective rank.
    val sigma0 = sigmas(0)
    val threshold = rCond * sigma0
    var i = 0
    // sigmas might have a length smaller than k, if some Ritz values do not satisfy the convergence
    // criterion specified by tol after max number of iterations.
    // Thus use i < min(k, sigmas.length) instead of i < k.
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

    // Warn at the end of the run as well, for increased visibility.
    if (computeMode == SVDMode.DistARPACK && rows.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    val s = Vectors.dense(Arrays.copyOfRange(sigmas.data, 0, sk))
    val V = Matrices.dense(n, sk, Arrays.copyOfRange(u.data, 0, n * sk))

    if (computeU) {
      // N = Vk * Sk^{-1}
      val N = new BDM[Double](n, sk, Arrays.copyOfRange(u.data, 0, n * sk))
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
    checkNumColumns(n)

    val (m, mean) = rows.treeAggregate[(Long, BDV[Double])]((0L, BDV.zeros[Double](n)))(
      seqOp = (s: (Long, BDV[Double]), v: Vector) => (s._1 + 1L, s._2 += v.toBreeze),
      combOp = (s1: (Long, BDV[Double]), s2: (Long, BDV[Double])) =>
        (s1._1 + s2._1, s1._2 += s2._2)
    )

    if (m <= 1) {
      sys.error(s"RowMatrix.computeCovariance called on matrix with only $m rows." +
        "  Cannot compute the covariance of a RowMatrix with <= 1 row.")
    }
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
   * The row data do not need to be "centered" first; it is not necessary for
   * the mean of each column to be 0.
   *
   * @param k number of top principal components.
   * @return a matrix of size n-by-k, whose columns are principal components
   */
  def computePrincipalComponents(k: Int): Matrix = {
    val n = numCols().toInt
    require(k > 0 && k <= n, s"k = $k out of range (0, n = $n]")

    val Cov = computeCovariance().toBreeze.asInstanceOf[BDM[Double]]

    val brzSvd.SVD(u: BDM[Double], _, _) = brzSvd(Cov)

    if (k == n) {
      Matrices.dense(n, k, u.data)
    } else {
      Matrices.dense(n, k, Arrays.copyOfRange(u.data, 0, n * k))
    }
  }

  /**
   * Computes column-wise summary statistics.
   */
  def computeColumnSummaryStatistics(): MultivariateStatisticalSummary = {
    val summary = rows.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
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
    val k = B.numCols
    require(n == B.numRows, s"Dimension mismatch: $n vs ${B.numRows}")

    require(B.isInstanceOf[DenseMatrix],
      s"Only support dense matrix at this time but found ${B.getClass.getName}.")

    val Bb = rows.context.broadcast(B.toBreeze.asInstanceOf[BDM[Double]].toDenseVector.toArray)
    val AB = rows.mapPartitions { iter =>
      val Bi = Bb.value
      iter.map { row =>
        val v = BDV.zeros[Double](k)
        var i = 0
        while (i < k) {
          v(i) = row.toBreeze.dot(new BDV(Bi, i * n, 1, n))
          i += 1
        }
        Vectors.fromBreeze(v)
      }
    }

    new RowMatrix(AB, nRows, B.numCols)
  }

  /**
   * Compute all cosine similarities between columns of this matrix using the brute-force
   * approach of computing normalized dot products.
   *
   * @return An n x n sparse upper-triangular matrix of cosine similarities between
   *         columns of this matrix.
   */
  def columnSimilarities(): CoordinateMatrix = {
    columnSimilarities(0.0)
  }

  /**
   * Compute similarities between columns of this matrix using a sampling approach.
   *
   * The threshold parameter is a trade-off knob between estimate quality and computational cost.
   *
   * Setting a threshold of 0 guarantees deterministic correct results, but comes at exactly
   * the same cost as the brute-force approach. Setting the threshold to positive values
   * incurs strictly less computational cost than the brute-force approach, however the
   * similarities computed will be estimates.
   *
   * The sampling guarantees relative-error correctness for those pairs of columns that have
   * similarity greater than the given similarity threshold.
   *
   * To describe the guarantee, we set some notation:
   * Let A be the smallest in magnitude non-zero element of this matrix.
   * Let B be the largest  in magnitude non-zero element of this matrix.
   * Let L be the maximum number of non-zeros per row.
   *
   * For example, for {0,1} matrices: A=B=1.
   * Another example, for the Netflix matrix: A=1, B=5
   *
   * For those column pairs that are above the threshold,
   * the computed similarity is correct to within 20% relative error with probability
   * at least 1 - (0.981)^10/B^
   *
   * The shuffle size is bounded by the *smaller* of the following two expressions:
   *
   * O(n log(n) L / (threshold * A))
   * O(m L^2^)
   *
   * The latter is the cost of the brute-force approach, so for non-zero thresholds,
   * the cost is always cheaper than the brute-force approach.
   *
   * @param threshold Set to 0 for deterministic guaranteed correctness.
   *                  Similarities above this threshold are estimated
   *                  with the cost vs estimate quality trade-off described above.
   * @return An n x n sparse upper-triangular matrix of cosine similarities
   *         between columns of this matrix.
   */
  def columnSimilarities(threshold: Double): CoordinateMatrix = {
    require(threshold >= 0, s"Threshold cannot be negative: $threshold")

    if (threshold > 1) {
      logWarning(s"Threshold is greater than 1: $threshold " +
      "Computation will be more efficient with promoted sparsity, " +
      " however there is no correctness guarantee.")
    }

    val gamma = if (threshold < 1e-6) {
      Double.PositiveInfinity
    } else {
      10 * math.log(numCols()) / threshold
    }

    columnSimilaritiesDIMSUM(computeColumnSummaryStatistics().normL2.toArray, gamma)
  }

  /**
   * Find all similar columns using the DIMSUM sampling algorithm, described in two papers
   *
   * http://arxiv.org/abs/1206.2082
   * http://arxiv.org/abs/1304.1467
   *
   * @param colMags A vector of column magnitudes
   * @param gamma The oversampling parameter. For provable results, set to 10 * log(n) / s,
   *              where s is the smallest similarity score to be estimated,
   *              and n is the number of columns
   * @return An n x n sparse upper-triangular matrix of cosine similarities
   *         between columns of this matrix.
   */
  private[mllib] def columnSimilaritiesDIMSUM(
      colMags: Array[Double],
      gamma: Double): CoordinateMatrix = {
    require(gamma > 1.0, s"Oversampling should be greater than 1: $gamma")
    require(colMags.size == this.numCols(), "Number of magnitudes didn't match column dimension")
    val sg = math.sqrt(gamma) // sqrt(gamma) used many times

    // Don't divide by zero for those columns with zero magnitude
    val colMagsCorrected = colMags.map(x => if (x == 0) 1.0 else x)

    val sc = rows.context
    val pBV = sc.broadcast(colMagsCorrected.map(c => sg / c))
    val qBV = sc.broadcast(colMagsCorrected.map(c => math.min(sg, c)))

    val sims = rows.mapPartitionsWithIndex { (indx, iter) =>
      val p = pBV.value
      val q = qBV.value

      val rand = new XORShiftRandom(indx)
      val scaled = new Array[Double](p.size)
      iter.flatMap { row =>
        val buf = new ListBuffer[((Int, Int), Double)]()
        row match {
          case sv: SparseVector =>
            val nnz = sv.indices.size
            var k = 0
            while (k < nnz) {
              scaled(k) = sv.values(k) / q(sv.indices(k))
              k += 1
            }
            k = 0
            while (k < nnz) {
              val i = sv.indices(k)
              val iVal = scaled(k)
              if (iVal != 0 && rand.nextDouble() < p(i)) {
                var l = k + 1
                while (l < nnz) {
                  val j = sv.indices(l)
                  val jVal = scaled(l)
                  if (jVal != 0 && rand.nextDouble() < p(j)) {
                    buf += (((i, j), iVal * jVal))
                  }
                  l += 1
                }
              }
              k += 1
            }
          case dv: DenseVector =>
            val n = dv.values.size
            var i = 0
            while (i < n) {
              scaled(i) = dv.values(i) / q(i)
              i += 1
            }
            i = 0
            while (i < n) {
              val iVal = scaled(i)
              if (iVal != 0 && rand.nextDouble() < p(i)) {
                var j = i + 1
                while (j < n) {
                  val jVal = scaled(j)
                  if (jVal != 0 && rand.nextDouble() < p(j)) {
                    buf += (((i, j), iVal * jVal))
                  }
                  j += 1
                }
              }
              i += 1
            }
        }
        buf
      }
    }.reduceByKey(_ + _).map { case ((i, j), sim) =>
      MatrixEntry(i.toLong, j.toLong, sim)
    }
    new CoordinateMatrix(sims, numCols(), numCols())
  }

  private[mllib] override def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    var i = 0
    rows.collect().foreach { vector =>
      vector.foreachActive { case (j, v) =>
        mat(i, j) = v
      }
      i += 1
    }
    mat
  }

  /** Updates or verifies the number of rows. */
  private def updateNumRows(m: Long) {
    if (nRows <= 0) {
      nRows = m
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
