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

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.util.Utils.random

// Updater that will be used to update W and H iteratively.
@Since("2.0.0")
@Experimental
private[mllib] trait NMFUpdater {
  // Update Matrix H. When called with (A^T^, H, W), it can be used to update Matrix W.
  def update(A: CoordinateMatrix,
      W: IndexedRowMatrix,
      H: IndexedRowMatrix): IndexedRowMatrix
}

/**
 * Updater for Gaussian NMF. The corresponding objective function is Squared Loss :
 * ||A - W*H^T^ ||^2^.
 * @see [[http://research.microsoft.com/pubs/119077/DNMF.pdf
 *     Chao Liu, Hung-chih Yang, Jinliang Fan, Li-Wei He and Yi-Min Wang, WWW, 2010.]]
 * @see [[http://papers.nips.cc/paper/1861-algorithms-for-non-negative-matrix-factorization.pdf
 *     Daniel D. Lee and H. Sebastian Seung, NIPS, 2000.]]
 */
@Since("2.0.0")
@Experimental
private[mllib] class GaussianNMFUpdater extends NMFUpdater {
  override def update(A: CoordinateMatrix,
      W: IndexedRowMatrix,
      H: IndexedRowMatrix): IndexedRowMatrix = {

    val sc = A.entries.sparkContext

    val m = A.numRows()
    val n = A.numCols()
    val k = W.numCols().toInt

    val a = A.entries.map(e => (e.i, (e.j, e.value)))
    val w = W.rows.map(r => (r.index, r.vector.toBreeze.toDenseVector))
    val h = H.rows.map(r => (r.index, r.vector.toBreeze.toDenseVector))

    // Compute X = W^T^ * A.
    val x = a.join(w).map {
      case (i, ((j, v), wi)) =>
        (j, wi * v)
    }.reduceByKey {
      case (v1, v2) =>
        v1 += v2
        v1
    }

    // Compute W^T^ * W.
    val wTw = w.map {
      case (i, wi) =>
        val m = new BDM[Double](k, 1, wi.toArray)
        m * m.t
    }.reduce {
      case (m1, m2) =>
        m1 += m2
        m1
    }

    val bcwTw = sc.broadcast(wTw)

    // Compute Y = W^T^ * W * H.
    val y = h.map {
      case (j, hj) =>
        (j, bcwTw.value * hj)
    }

    // Update H_new = H_old .* X ./ Y.
    val hNew = x.join(y).join(h).map {
      case (j, ((xj, yj), hj)) =>
        val r = hj :* xj :/ yj
        IndexedRow(j, Vectors.fromBreeze(r))
    }

    new IndexedRowMatrix(hNew, n, k)
  }
}

/**
 * Compute Non-Negative Matrix Factorization. Find two non-negative matrices (W, H) whose product
 * W * H^T^ approximates the non-negative matrix A. This factorization can be used for example for
 * dimensionality reduction, source separation or topic extraction.
 */
@Since("2.0.0")
@Experimental
object NMF extends Enumeration with Logging {
  // Todo: Add Poisson and Exponential types in the future, if needed
  val Gaussian = Value

  /**
   * Computes the Non-Negative Matrix Factorization of non-negative matrix A.
   * @param A the non-negative matrix to be factorized.
   * @param k the number of components.
   * @param numIterations number of iterations.
   * @param initW the initial matrix W, must be non-negative.
   * @param initH the initial matrix W, must be non-negative.
   * @param updater the NMFUpdater to update W and H.
   * @return the NMF result of A.
   */
  private def solve(A: CoordinateMatrix,
      k: Int,
      numIterations: Int,
      initW: IndexedRowMatrix,
      initH: IndexedRowMatrix,
      updater: NMFUpdater): NMFDecomposition[IndexedRowMatrix, IndexedRowMatrix] = {
    require(k > 0, s"Number of components must be greater than 0, " +
      s"but got ${k}")
    require(numIterations > 0, s"Number of iterations must be greater than 0, " +
      s"but got ${numIterations}")

    var W = initW
    var H = initH

    var i = 1
    while (i <= numIterations) {
      log.info(s"Iteration ${i}, updating Matrix H")
      H = updater.update(A, W, H)
      log.info(s"Iteration ${i}, updating Matrix W")
      W = updater.update(A.transpose(), H, W)
      i += 1
    }

    NMFDecomposition(W, H)
  }

  /**
   * Computes the Non-Negative Matrix Factorization of non-negative matrix A.
   * @param A the non-negative matrix to be factorized.
   * @param k the number of components.
   * @param numIterations number of iterations.
   * @param dist the type of NMF, now only Gaussian is supported.
   * @param initW the initial matrix W, must be non-negative.
   * @param initH the initial matrix W, must be non-negative.
   * @return the NMF result of A.
   */
  def solve(A: CoordinateMatrix,
      k: Int,
      numIterations: Int,
      dist: NMF.Value,
      initW: IndexedRowMatrix,
      initH: IndexedRowMatrix): NMFDecomposition[IndexedRowMatrix, IndexedRowMatrix] = {

    require(k > 0, s"Number of components must be greater than 0, " +
      s"but got ${k}")
    require(numIterations > 0, s"Number of iterations must be greater than 0, " +
      s"but got ${numIterations}")

    val m = A.numRows()
    val n = A.numCols()
    require(initW.numRows() == m && initW.numCols() == k, s"Shape of Matrix W should be " +
      s"[${m}, ${k}], but got [${initW.numRows()}, ${initW.numCols()}]")
    require(initH.numRows() == n && initH.numCols() == k, s"Shape of Matrix H should be " +
      s"[${n}, ${k}], but got [${initH.numRows()}, ${initH.numCols()}]")

    val updater = dist match {
      case NMF.Gaussian =>
        new GaussianNMFUpdater
      case _ =>
        throw new IllegalArgumentException(
          s"NMFUpdater only supports Gaussian, but got type ${dist}.")
    }

    A.entries.foreach {
      case MatrixEntry(i, j, value) =>
        require(value >= 0, s"Elements in Matrix A must be no less than 0, " +
          s"but got ${value}")
    }

    initH.rows.foreach {
      case IndexedRow(i, vector) =>
        vector.foreachActive {
          case (index, value) =>
            require(value >= 0, s"Elements in Matrix H must be no less than 0, " +
              s"but got H[${i}, ${index}] ${value}")
        }
    }

    initW.rows.foreach {
      case IndexedRow(i, vector) =>
        vector.foreachActive {
          case (index, value) =>
            require(value >= 0, s"Elements in Matrix W must be no less than 0, " +
              s"but got W[${i}, ${index}] ${value}")
        }
    }

    solve(A, k, numIterations, initW, initH, updater)
  }

  /**
   * Computes the Non-Negative Matrix Factorization of non-negative matrix A.
   * @param A the non-negative matrix to be factorized.
   * @param k the number of components.
   * @param numIterations number of iterations.
   * @param dist the type of NMF, now only Gaussian is supported.
   * @param seed the seed to initialize matrix W and H.
   * @return the NMF result of A.
   */
  def solve(A: CoordinateMatrix,
      k: Int,
      numIterations: Int,
      dist: NMF.Value = NMF.Gaussian,
      seed: Long = random.nextLong()): NMFDecomposition[IndexedRowMatrix, IndexedRowMatrix] = {

    require(k > 0, s"Number of components must be greater than 0, " +
      s"but got ${k}")
    require(numIterations > 0, s"Number of iterations must be greater than 0, " +
      s"but got ${numIterations}")

    val updater = dist match {
      case NMF.Gaussian =>
        new GaussianNMFUpdater
      case _ =>
        throw new IllegalArgumentException(
          s"NMFUpdater only supports Gaussian, but got type ${dist}.")
    }

    val sc = A.entries.sparkContext

    val m = A.numRows()
    val n = A.numCols()
    val p = A.entries.getNumPartitions

    val w = normalVectorRDD(sc, m, k, p, seed).zipWithIndex().map {
      case (vec, i) =>
        val arr = vec.toArray.map(math.abs)
        IndexedRow(i, Vectors.dense(arr))
    }

    val h = normalVectorRDD(sc, n, k, p, seed).zipWithIndex().map {
      case (vec, i) =>
        val arr = vec.toArray.map(math.abs)
        IndexedRow(i, Vectors.dense(arr))
    }

    val initW: IndexedRowMatrix = new IndexedRowMatrix(w, m, k)

    val initH: IndexedRowMatrix = new IndexedRowMatrix(w, n, k)

    solve(A, k, numIterations, initW, initH, updater)
  }
}
