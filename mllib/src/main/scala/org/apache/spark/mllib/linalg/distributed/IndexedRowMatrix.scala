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

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.rdd.RDD

/**
 * Represents a row of [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]].
 */
@Since("1.0.0")
case class IndexedRow(index: Long, vector: Vector)

/**
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
   * Compute all cosine similarities between columns of this matrix using the brute-force
   * approach of computing normalized dot products.
   *
   * @return An n x n sparse upper-triangular matrix of cosine similarities between
   *         columns of this matrix.
   */
  @Since("1.6.0")
  def columnSimilarities(): CoordinateMatrix = {
    toRowMatrix().columnSimilarities()
  }

  /**
   * Drops row indices and converts this matrix to a
   * [[org.apache.spark.mllib.linalg.distributed.RowMatrix]].
   */
  @Since("1.0.0")
  def toRowMatrix(): RowMatrix = {
    new RowMatrix(rows.map(_.vector), 0L, nCols)
  }

  /**
   * Converts to BlockMatrix. Creates blocks with size 1024 x 1024.
   */
  @Since("1.3.0")
  def toBlockMatrix(): BlockMatrix = {
    toBlockMatrix(1024, 1024)
  }

  /**
   * Converts to BlockMatrix. Blocks may be sparse or dense depending on the sparsity of the rows.
   * @param rowsPerBlock The number of rows of each block. The blocks at the bottom edge may have
   *                     a smaller value. Must be an integer value greater than 0.
   * @param colsPerBlock The number of columns of each block. The blocks at the right edge may have
   *                     a smaller value. Must be an integer value greater than 0.
   * @return a [[BlockMatrix]]
   */
  @Since("1.3.0")
  def toBlockMatrix(rowsPerBlock: Int, colsPerBlock: Int): BlockMatrix = {
    require(rowsPerBlock > 0,
      s"rowsPerBlock needs to be greater than 0. rowsPerBlock: $rowsPerBlock")
    require(colsPerBlock > 0,
      s"colsPerBlock needs to be greater than 0. colsPerBlock: $colsPerBlock")

    val m = numRows()
    val n = numCols()

    // Since block matrices require an integer row index
    require(math.ceil(m.toDouble / rowsPerBlock) <= Int.MaxValue,
      "Number of rows divided by rowsPerBlock cannot exceed maximum integer.")

    // The remainder calculations only matter when m % rowsPerBlock != 0 or n % colsPerBlock != 0
    val remainderRowBlockIndex = m / rowsPerBlock
    val remainderColBlockIndex = n / colsPerBlock
    val remainderRowBlockSize = (m % rowsPerBlock).toInt
    val remainderColBlockSize = (n % colsPerBlock).toInt
    val numRowBlocks = math.ceil(m.toDouble / rowsPerBlock).toInt
    val numColBlocks = math.ceil(n.toDouble / colsPerBlock).toInt

    val blocks = rows.flatMap { ir: IndexedRow =>
      val blockRow = ir.index / rowsPerBlock
      val rowInBlock = ir.index % rowsPerBlock

      ir.vector match {
        case SparseVector(_, indices, values) =>
          indices.zip(values).map { case (index, value) =>
            val blockColumn = index / colsPerBlock
            val columnInBlock = index % colsPerBlock
            ((blockRow.toInt, blockColumn), (rowInBlock.toInt, Array((value, columnInBlock))))
          }
        case DenseVector(values) =>
          values.grouped(colsPerBlock)
            .zipWithIndex
            .map { case (values, blockColumn) =>
              ((blockRow.toInt, blockColumn), (rowInBlock.toInt, values.zipWithIndex))
            }
        case v =>
          throw new IllegalArgumentException(s"Unknown vector type ${v.getClass}.")
      }
    }.groupByKey(GridPartitioner(numRowBlocks, numColBlocks, rows.getNumPartitions)).map {
      case ((blockRow, blockColumn), itr) =>
        val actualNumRows =
          if (blockRow == remainderRowBlockIndex) remainderRowBlockSize else rowsPerBlock
        val actualNumColumns =
          if (blockColumn == remainderColBlockIndex) remainderColBlockSize else colsPerBlock

        val arraySize = actualNumRows * actualNumColumns
        val matrixAsArray = new Array[Double](arraySize)
        var countForValues = 0
        itr.foreach { case (rowWithinBlock, valuesWithColumns) =>
          valuesWithColumns.foreach { case (value, columnWithinBlock) =>
            matrixAsArray.update(columnWithinBlock * actualNumRows + rowWithinBlock, value)
            countForValues += 1
          }
        }
        val denseMatrix = new DenseMatrix(actualNumRows, actualNumColumns, matrixAsArray)
        val finalMatrix = if (countForValues / arraySize.toDouble >= 0.1) {
          denseMatrix
        } else {
          denseMatrix.toSparse
        }

        ((blockRow, blockColumn), finalMatrix)
    }
    new BlockMatrix(blocks, rowsPerBlock, colsPerBlock, m, n)
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
          Iterator.tabulate(indices.length)(i => MatrixEntry(rowIndex, indices(i), values(i)))
        case DenseVector(values) =>
          Iterator.tabulate(values.length)(i => MatrixEntry(rowIndex, i, values(i)))
        case v =>
          throw new IllegalArgumentException(s"Unknown vector type ${v.getClass}.")
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
   *
   * @note This cannot be computed on matrices with more than 65535 columns.
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
      vector.foreachNonZero { case (j, v) =>
        mat(i, j) = v
      }
    }
    mat
  }
}
