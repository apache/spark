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

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.SingularValueDecomposition

/**
 * :: Experimental ::
 * Represents a row of [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]].
 */
@Since("1.0.0")
@Experimental
case class IndexedRow(index: Long, vector: Vector)

/**
 * :: Experimental ::
 * Represents a row-oriented [[org.apache.spark.mllib.linalg.distributed.DistributedMatrix]] with
 * indexed rows.
 *
 * @param rows indexed rows of this matrix
 * @param nRows number of rows. A non-positive value means unknown, and then the number of rows will
 *              be determined by the max row index plus one.
 * @param nCols number of columns. A non-positive value means unknown, and then the number of
 *              columns will be determined by the size of the first row.
 */
@Since("1.0.0")
@Experimental
class IndexedRowMatrix @Since("1.0.0") (
    @Since("1.0.0") val rows: RDD[IndexedRow],
    private var nRows: Long,
    private var nCols: Int) extends DistributedMatrix {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  @Since("1.0.0")
  def this(rows: RDD[IndexedRow]) = this(rows, 0L, 0)

  @Since("1.0.0")
  override def numCols(): Long = {
    if (nCols <= 0) {
      // Calling `first` will throw an exception if `rows` is empty.
      nCols = rows.first().vector.size
    }
    nCols
  }

  @Since("1.0.0")
  override def numRows(): Long = {
    if (nRows <= 0L) {
      // Reduce will throw an exception if `rows` is empty.
      nRows = rows.map(_.index).reduce(math.max) + 1L
    }
    nRows
  }

  /**
   * Drops row indices and converts this matrix to a
   * [[org.apache.spark.mllib.linalg.distributed.RowMatrix]].
   */
  @Since("1.0.0")
  def toRowMatrix(): RowMatrix = {
    new RowMatrix(rows.map(_.vector), 0L, nCols)
  }

  /** Converts to BlockMatrix. Creates blocks of [[SparseMatrix]] with size 1024 x 1024. */
  @Since("1.3.0")
  def toBlockMatrix(): BlockMatrix = {
    toBlockMatrix(1024, 1024)
  }

  /**
   * Converts to BlockMatrix. Creates blocks of [[SparseMatrix]].
   * @param rowsPerBlock The number of rows of each block. The blocks at the bottom edge may have
   *                     a smaller value. Must be an integer value greater than 0.
   * @param colsPerBlock The number of columns of each block. The blocks at the right edge may have
   *                     a smaller value. Must be an integer value greater than 0.
   * @return a [[BlockMatrix]]
   */
  @Since("1.3.0")
  def toBlockMatrix(rowsPerBlock: Int, colsPerBlock: Int): BlockMatrix = {
    // TODO: This implementation may be optimized
    toCoordinateMatrix().toBlockMatrix(rowsPerBlock, colsPerBlock)
  }

  /**
   * Converts this matrix to a
   * [[org.apache.spark.mllib.linalg.distributed.CoordinateMatrix]].
   */
  @Since("1.3.0")
  def toCoordinateMatrix(): CoordinateMatrix = {
    val entries = rows.flatMap { row =>
      val rowIndex = row.index
      row.vector match {
        case SparseVector(size, indices, values) =>
          Iterator.tabulate(indices.size)(i => MatrixEntry(rowIndex, indices(i), values(i)))
        case DenseVector(values) =>
          Iterator.tabulate(values.size)(i => MatrixEntry(rowIndex, i, values(i)))
      }
    }
    new CoordinateMatrix(entries, numRows(), numCols())
  }

  /**
   * Computes the singular value decomposition of this IndexedRowMatrix.
   * Denote this matrix by A (m x n), this will compute matrices U, S, V such that A = U * S * V'.
   *
   * The cost and implementation of this method is identical to that in
   * [[org.apache.spark.mllib.linalg.distributed.RowMatrix]]
   * With the addition of indices.
   *
   * At most k largest non-zero singular values and associated vectors are returned.
   * If there are k such values, then the dimensions of the return will be:
   *
   * U is an [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]] of size m x k that
   * satisfies U'U = eye(k),
   * s is a Vector of size k, holding the singular values in descending order,
   * and V is a local Matrix of size n x k that satisfies V'V = eye(k).
   *
   * @param k number of singular values to keep. We might return less than k if there are
   *          numerically zero singular values. See rCond.
   * @param computeU whether to compute U
   * @param rCond the reciprocal condition number. All singular values smaller than rCond * sigma(0)
   *              are treated as zero, where sigma(0) is the largest singular value.
   * @return SingularValueDecomposition(U, s, V)
   */
  @Since("1.0.0")
  def computeSVD(
      k: Int,
      computeU: Boolean = false,
      rCond: Double = 1e-9): SingularValueDecomposition[IndexedRowMatrix, Matrix] = {

    val n = numCols().toInt
    require(k > 0 && k <= n, s"Requested k singular values but got k=$k and numCols=$n.")
    val indices = rows.map(_.index)
    val svd = toRowMatrix().computeSVD(k, computeU, rCond)
    val U = if (computeU) {
      val indexedRows = indices.zip(svd.U.rows).map { case (i, v) =>
        IndexedRow(i, v)
      }
      new IndexedRowMatrix(indexedRows, nRows, svd.U.numCols().toInt)
    } else {
      null
    }
    SingularValueDecomposition(U, svd.s, svd.V)
  }

  /**
   * Multiply this matrix by a local matrix on the right.
   *
   * @param B a local matrix whose number of rows must match the number of columns of this matrix
   * @return an IndexedRowMatrix representing the product, which preserves partitioning
   */
  @Since("1.0.0")
  def multiply(B: Matrix): IndexedRowMatrix = {
    val mat = toRowMatrix().multiply(B)
    val indexedRows = rows.map(_.index).zip(mat.rows).map { case (i, v) =>
      IndexedRow(i, v)
    }
    new IndexedRowMatrix(indexedRows, nRows, B.numCols)
  }

  /**
   * Computes the Gramian matrix `A^T A`.
   */
  @Since("1.0.0")
  def computeGramianMatrix(): Matrix = {
    toRowMatrix().computeGramianMatrix()
  }

  private[mllib] override def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    rows.collect().foreach { case IndexedRow(rowIndex, vector) =>
      val i = rowIndex.toInt
      vector.foreachActive { case (j, v) =>
        mat(i, j) = v
      }
    }
    mat
  }
}
