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
import org.apache.spark.HashPartitioner

import scala.collection.Map
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import scala.collection.mutable
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import KernelType._

/**
 * :: Experimental ::
 * Represents a row of [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]].
 */
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
@Experimental
class IndexedRowMatrix(
    val rows: RDD[IndexedRow],
    private var nRows: Long,
    private var nCols: Int) extends DistributedMatrix {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(rows: RDD[IndexedRow]) = this(rows, 0L, 0)

  override def numCols(): Long = {
    if (nCols <= 0) {
      // Calling `first` will throw an exception if `rows` is empty.
      nCols = rows.first().vector.size
    }
    nCols
  }

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
  def toRowMatrix(): RowMatrix = {
    new RowMatrix(rows.map(_.vector), 0L, nCols)
  }

  /** Converts to BlockMatrix. Creates blocks of [[SparseMatrix]] with size 1024 x 1024. */
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
  def toBlockMatrix(rowsPerBlock: Int, colsPerBlock: Int): BlockMatrix = {
    // TODO: This implementation may be optimized
    toCoordinateMatrix().toBlockMatrix(rowsPerBlock, colsPerBlock)
  }

  /**
   * Converts this matrix to a
   * [[org.apache.spark.mllib.linalg.distributed.CoordinateMatrix]].
   */
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
      new IndexedRowMatrix(indexedRows, nRows, nCols)
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

  /**
   * Computes the row norm of each entry from IndexedRowMatrix
   * @param norm L1/L2 norm of each row
   * @return Map of row index and norm
   */
  def rowMagnitudes(norm: Int): Map[Long, Double] = {
    rows.map { indexedRow =>
      (indexedRow.index, Vectors.norm(indexedRow.vector, norm))
    }.collectAsMap()
  }

  /**
   * row similarity calculation with user defined kernel
   * @param kernel vector based kernel computation
   * @param topk topk similar rows to each row
   * @return CoordinateMatrix of rows similar to every row
   */
  def rowSimilarities(
      kernel: Kernel,
      topk: Int): CoordinateMatrix = {
    multiply(rows, kernel, topk)
  }

  /**
   * row similarity calculation with cosine kernel
   * @param topk topk similar rows to each row
   * @param threshold cosine similarity threshold
   * @return CoordinateMatrix of rows similar to every row
   */
  def rowSimilarities(
      topk: Int = nRows.toInt,
      threshold: Double = 1e-4): CoordinateMatrix = {
    val rowNorms = rowMagnitudes(2)
    val kernel = CosineKernel(rowNorms, threshold)
    rowSimilarities(kernel, topk)
  }

  private def blockify(features: RDD[IndexedRow], blockSize: Int): RDD[(Int, Array[IndexedRow])] = {
    val featurePartitioner = new HashPartitioner(blockSize)
    val blockedFeatures = features.map { row =>
      (featurePartitioner.getPartition(row.index), row)
    }.groupByKey(blockSize).map {
      case (index, rows) => (index, rows.toArray)
    }
    blockedFeatures.count()
    blockedFeatures
  }

  private def multiply(query: RDD[IndexedRow],
      kernel: Kernel,
      topk: Int): CoordinateMatrix = {
    val ord = Ordering[(Float, Long)].on[(Long, Double)](x => (x._2.toFloat, x._1))
    val defaultParallelism = rows.sparkContext.defaultParallelism

    val queryBlocks = math.max(query.sparkContext.defaultParallelism, query.partitions.size) / 2
    val dictionaryBlocks = math.max(defaultParallelism, rows.partitions.size) / 2

    val blockedSmall = blockify(query, queryBlocks)
    val blockedBig = blockify(rows, dictionaryBlocks)

    blockedSmall.setName("blockedSmallMatrix")
    blockedBig.setName("blockedBigMatrix")

    blockedBig.cache()

    val topkSims = blockedBig.cartesian(blockedSmall).flatMap {
      case ((bigBlockIndex, bigRows), (smallBlockIndex, smallRows)) =>
        val buf = mutable.ArrayBuilder.make[(Long, (Long, Double))]
        for (i <- 0 until bigRows.size; j <- 0 until smallRows.size) {
          val bigIndex = bigRows(i).index
          val bigRow = bigRows(i).vector
          val smallIndex = smallRows(j).index
          val smallRow = smallRows(j).vector
          val kernelVal = kernel.compute(smallRow, smallIndex, bigRow, bigIndex)
          if (kernelVal != 0.0) {
            val entry = (bigIndex, (smallIndex, kernelVal))
            buf += entry
          }
        }
        buf.result()
    }.topByKey(topk)(ord).flatMap { case (i, value) =>
      value.map { sim =>
        MatrixEntry(i, sim._1, sim._2)
      }
    }.repartition(defaultParallelism)

    blockedBig.unpersist()

    // Materialize the cartesian RDD
    topkSims.count()
    new CoordinateMatrix(topkSims)
  }
}
