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

import org.apache.spark._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * A grid partitioner, which stores every block in a separate partition.
 *
 * @param numRowBlocks Number of blocks that form the rows of the matrix.
 * @param numColBlocks Number of blocks that form the columns of the matrix.
 * @param rowsPerBlock Number of rows that make up each block.
 * @param colsPerBlock Number of columns that make up each block.
 */
private[mllib] class GridPartitioner(
    val numRowBlocks: Int,
    val numColBlocks: Int,
    val rowsPerBlock: Int,
    val colsPerBlock: Int,
    override val numPartitions: Int) extends Partitioner {

  /**
   * Returns the index of the partition the SubMatrix belongs to. Tries to achieve block wise
   * partitioning.
   *
   * @param key The key for the SubMatrix. Can be its position in the grid (its column major index)
   *            or a tuple of three integers that are the final row index after the multiplication,
   *            the index of the block to multiply with, and the final column index after the
   *            multiplication.
   * @return The index of the partition, which the SubMatrix belongs to.
   */
  override def getPartition(key: Any): Int = {
    val sqrtPartition = math.round(math.sqrt(numPartitions)).toInt
    // numPartitions may not be the square of a number, it can even be a prime number
    
    key match {
      case (blockRowIndex: Int, blockColIndex: Int) =>
        Utils.nonNegativeMod(blockRowIndex + blockColIndex * numRowBlocks, numPartitions)
      case (blockRowIndex: Int, innerIndex: Int, blockColIndex: Int) =>
        Utils.nonNegativeMod(blockRowIndex + blockColIndex * numRowBlocks, numPartitions)
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized key. key: $key")
    }
  }

  /** Checks whether the partitioners have the same characteristics */
  override def equals(obj: Any): Boolean = {
    obj match {
      case r: GridPartitioner =>
        (this.numRowBlocks == r.numRowBlocks) && (this.numColBlocks == r.numColBlocks)
          (this.rowsPerBlock == r.rowsPerBlock) && (this.colsPerBlock == r.colsPerBlock)
      case _ =>
        false
    }
  }
}

/**
 * Represents a distributed matrix in blocks of local matrices.
 *
 * @param numRowBlocks Number of blocks that form the rows of this matrix
 * @param numColBlocks Number of blocks that form the columns of this matrix
 * @param rdd The RDD of SubMatrices (local matrices) that form this matrix
 */
class BlockMatrix(
    val numRowBlocks: Int,
    val numColBlocks: Int,
    val rdd: RDD[((Int, Int), Matrix)]) extends DistributedMatrix with Logging {

  private type SubMatrix = ((Int, Int), Matrix) // ((blockRowIndex, blockColIndex), matrix)

  /**
   * Alternate constructor for BlockMatrix without the input of a partitioner. Will use a Grid
   * Partitioner by default.
   *
   * @param numRowBlocks Number of blocks that form the rows of this matrix
   * @param numColBlocks Number of blocks that form the columns of this matrix
   * @param rdd The RDD of SubMatrices (local matrices) that form this matrix
   * @param rowsPerBlock Number of rows that make up each block.
   * @param colsPerBlock Number of columns that make up each block.
   */
  def this(
      numRowBlocks: Int,
      numColBlocks: Int,
      rdd: RDD[((Int, Int), Matrix)],
      rowsPerBlock: Int,
      colsPerBlock: Int) = {
    this(numRowBlocks, numColBlocks, rdd)
    val part = new GridPartitioner(numRowBlocks, numColBlocks, rowsPerBlock,
      colsPerBlock, rdd.partitions.length)
    partitioner = part
  }

  private[mllib] var partitioner: GridPartitioner = {
    val firstSubMatrix = rdd.first()._2
    new GridPartitioner(numRowBlocks, numColBlocks,
      firstSubMatrix.numRows, firstSubMatrix.numCols, rdd.partitions.length)
  }

  private lazy val dims: (Long, Long) = getDim

  override def numRows(): Long = dims._1
  override def numCols(): Long = dims._2

  /** Returns the dimensions of the matrix. */
  private def getDim: (Long, Long) = {
    case class MatrixMetaData(var rowIndex: Int, var colIndex: Int,
        var numRows: Int, var numCols: Int)
    // picks the sizes of the matrix with the maximum indices
    def pickSizeByGreaterIndex(example: MatrixMetaData, base: MatrixMetaData): MatrixMetaData = {
      if (example.rowIndex > base.rowIndex) {
        base.rowIndex = example.rowIndex
        base.numRows = example.numRows
      }
      if (example.colIndex > base.colIndex) {
        base.colIndex = example.colIndex
        base.numCols = example.numCols
      }
      base
    }

    val lastRowCol = rdd.treeAggregate(new MatrixMetaData(0, 0, 0, 0))(
      seqOp = (c, v) => (c, v) match { case (base, ((blockXInd, blockYInd), mat)) =>
        pickSizeByGreaterIndex(
          new MatrixMetaData(blockXInd, blockYInd, mat.numRows, mat.numCols), base)
      },
      combOp = (c1, c2) => (c1, c2) match {
        case (res1, res2) =>
          pickSizeByGreaterIndex(res1, res2)
      })
    // We add the size of the edge matrices, because they can be less than the specified
    // rowsPerBlock or colsPerBlock.
    (lastRowCol.rowIndex.toLong * partitioner.rowsPerBlock + lastRowCol.numRows,
      lastRowCol.colIndex.toLong * partitioner.colsPerBlock + lastRowCol.numCols)
  }

  /** Returns the Frobenius Norm of the matrix */
  def normFro(): Double = {
    math.sqrt(rdd.map { mat => mat._2 match {
      case sparse: SparseMatrix =>
        sparse.values.map(x => math.pow(x, 2)).sum
      case dense: DenseMatrix =>
        dense.values.map(x => math.pow(x, 2)).sum
      }
    }.reduce(_ + _))
  }

  /** Cache the underlying RDD. */
  def cache(): BlockMatrix = {
    rdd.cache()
    this
  }

  /** Set the storage level for the underlying RDD. */
  def persist(storageLevel: StorageLevel): BlockMatrix = {
    rdd.persist(storageLevel)
    this
  }

  /** Collect the distributed matrix on the driver as a `DenseMatrix`. */
  def toLocalMatrix(): Matrix = {
    require(numRows() < Int.MaxValue, "The number of rows of this matrix should be less than " +
      s"Int.MaxValue. Currently numRows: ${numRows()}")
    require(numCols() < Int.MaxValue, "The number of columns of this matrix should be less than " +
      s"Int.MaxValue. Currently numCols: ${numCols()}")
    val nRows = numRows().toInt
    val nCols = numCols().toInt
    val mem = nRows * nCols * 8 / 1000000
    if (mem > 500) logWarning(s"Storing this matrix will require $mem MB of memory!")

    val parts = rdd.collect().sortBy(x => (x._1._2, x._1._1))
    val values = new Array[Double](nRows * nCols)
    parts.foreach { case ((rowIndex, colIndex), block) =>
      val rowOffset = rowIndex * partitioner.rowsPerBlock
      val colOffset = colIndex * partitioner.colsPerBlock
      var j = 0
      val mat = block.toArray
      while (j < block.numCols) {
        var i = 0
        val indStart = (j + colOffset) * nRows + rowOffset
        val matStart = j * block.numRows
        while (i < block.numRows) {
          values(indStart + i) = mat(matStart + i)
          i += 1
        }
        j += 1
      }
    }
    new DenseMatrix(nRows, nCols, values)
  }

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  private[mllib] def toBreeze(): BDM[Double] = {
    val localMat = toLocalMatrix()
    new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
  }
}
