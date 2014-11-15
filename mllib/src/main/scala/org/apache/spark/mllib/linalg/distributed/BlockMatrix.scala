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
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * Represents a local matrix that makes up one block of a distributed BlockMatrix
 *
 * @param blockRowIndex The row index of this block. Must be zero based.
 * @param blockColIndex The column index of this block. Must be zero based.
 * @param mat The underlying local matrix
 */
case class SubMatrix(blockRowIndex: Int, blockColIndex: Int, mat: DenseMatrix) extends Serializable

/**
 * A partitioner that decides how the matrix is distributed in the cluster
 *
 * @param numPartitions Number of partitions
 * @param rowPerBlock Number of rows that make up each block.
 * @param colPerBlock Number of columns that make up each block.
 */
private[mllib] abstract class BlockMatrixPartitioner(
    override val numPartitions: Int,
    val rowPerBlock: Int,
    val colPerBlock: Int) extends Partitioner {
  val name: String

  /**
   * Returns the index of the partition the SubMatrix belongs to.
   *
   * @param key The key for the SubMatrix. Can be its row index, column index or position in the
   *            grid.
   * @return The index of the partition, which the SubMatrix belongs to.
   */
  override def getPartition(key: Any): Int = {
    Utils.nonNegativeMod(key.asInstanceOf[Int], numPartitions)
  }
}

/**
 * A grid partitioner, which stores every block in a separate partition.
 *
 * @param numRowBlocks Number of blocks that form the rows of the matrix.
 * @param numColBlocks Number of blocks that form the columns of the matrix.
 * @param rowPerBlock Number of rows that make up each block.
 * @param colPerBlock Number of columns that make up each block.
 */
class GridPartitioner(
    val numRowBlocks: Int,
    val numColBlocks: Int,
    override val rowPerBlock: Int,
    override val colPerBlock: Int)
  extends BlockMatrixPartitioner(numRowBlocks * numColBlocks, rowPerBlock, colPerBlock) {

  override val name = "grid"

  override val numPartitions = numRowBlocks * numColBlocks

  /** Checks whether the partitioners have the same characteristics */
  override def equals(obj: Any): Boolean = {
    obj match {
      case r: GridPartitioner =>
        (this.numPartitions == r.numPartitions) && (this.rowPerBlock == r.rowPerBlock) &&
          (this.colPerBlock == r.colPerBlock)
      case _ =>
        false
    }
  }
}

/**
 * A specialized partitioner that stores all blocks in the same row in just one partition.
 *
 * @param numPartitions Number of partitions. Should be set as the number of blocks that form
 *                      the rows of the matrix.
 * @param rowPerBlock Number of rows that make up each block.
 * @param colPerBlock Number of columns that make up each block.
 */
class RowBasedPartitioner(
    override val numPartitions: Int,
    override val rowPerBlock: Int,
    override val colPerBlock: Int)
  extends BlockMatrixPartitioner(numPartitions, rowPerBlock, colPerBlock) {

  override val name = "row"

  /** Checks whether the partitioners have the same characteristics */
  override def equals(obj: Any): Boolean = {
    obj match {
      case r: RowBasedPartitioner =>
        (this.numPartitions == r.numPartitions) && (this.rowPerBlock == r.rowPerBlock) &&
          (this.colPerBlock == r.colPerBlock)
      case _ =>
        false
    }
  }
}

/**
 * A specialized partitioner that stores all blocks in the same column in just one partition.
 *
 * @param numPartitions Number of partitions. Should be set as the number of blocks that form
 *                      the columns of the matrix.
 * @param rowPerBlock Number of rows that make up each block.
 * @param colPerBlock Number of columns that make up each block.
 */
class ColumnBasedPartitioner(
    override val numPartitions: Int,
    override val rowPerBlock: Int,
    override val colPerBlock: Int)
  extends BlockMatrixPartitioner(numPartitions, rowPerBlock, colPerBlock) {

  override val name = "column"

  /** Checks whether the partitioners have the same characteristics */
  override def equals(obj: Any): Boolean = {
    obj match {
      case p: ColumnBasedPartitioner =>
        (this.numPartitions == p.numPartitions) && (this.rowPerBlock == p.rowPerBlock) &&
          (this.colPerBlock == p.colPerBlock)
      case r: RowBasedPartitioner =>
        (this.numPartitions == r.numPartitions) && (this.colPerBlock == r.rowPerBlock)
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
 * @param partitioner A partitioner that specifies how SubMatrices are stored in the cluster
 */
class BlockMatrix(
    val numRowBlocks: Int,
    val numColBlocks: Int,
    val rdd: RDD[SubMatrix],
    val partitioner: BlockMatrixPartitioner) extends DistributedMatrix with Logging {

  /**
   * Alternate constructor for BlockMatrix without the input of a partitioner. Will use a Grid
   * Partitioner by default.
   *
   * @param numRowBlocks Number of blocks that form the rows of this matrix
   * @param numColBlocks Number of blocks that form the columns of this matrix
   * @param rdd The RDD of SubMatrices (local matrices) that form this matrix
   */
  def this(numRowBlocks: Int, numColBlocks: Int, rdd: RDD[SubMatrix]) = {
    this(numRowBlocks, numColBlocks, rdd, new GridPartitioner(numRowBlocks, numColBlocks,
        rdd.first().mat.numRows, rdd.first().mat.numCols))
  }
  // A key-value pair RDD is required to partition properly
  private var matrixRDD: RDD[(Int, SubMatrix)] = keyBy()

  private lazy val dims: (Long, Long) = getDim

  override def numRows(): Long = dims._1
  override def numCols(): Long = dims._2

  if (partitioner.name.equals("column")) {
    require(numColBlocks == partitioner.numPartitions, "The number of column blocks should match" +
      s" the number of partitions of the column partitioner. numColBlocks: $numColBlocks, " +
      s"partitioner.numPartitions: ${partitioner.numPartitions}")
  } else if (partitioner.name.equals("row")) {
    require(numRowBlocks == partitioner.numPartitions, "The number of row blocks should match" +
      s" the number of partitions of the row partitioner. numRowBlocks: $numRowBlocks, " +
      s"partitioner.numPartitions: ${partitioner.numPartitions}")
  } else if (partitioner.name.equals("grid")) {
    require(numRowBlocks * numColBlocks == partitioner.numPartitions, "The number of blocks " +
      s"should match the number of partitions of the grid partitioner. numRowBlocks * " +
      s"numColBlocks: ${numRowBlocks * numColBlocks}, " +
      s"partitioner.numPartitions: ${partitioner.numPartitions}")
  } else {
    throw new IllegalArgumentException("Unrecognized partitioner.")
  }

  /** Returns the dimensions of the matrix. */
  def getDim: (Long, Long) = {

    val firstRowColumn = rdd.filter(block => block.blockRowIndex == 0 || block.blockColIndex == 0).
      map { block =>
       ((block.blockRowIndex, block.blockColIndex), (block.mat.numRows, block.mat.numCols))
      }

    firstRowColumn.treeAggregate((0L, 0L))(
      seqOp = (c, v) => (c, v) match { case ((x_dim, y_dim), ((indX, indY), (nRow, nCol))) =>
        if (indX == 0 && indY == 0) {
          (x_dim + nRow, y_dim + nCol)
        } else if (indX == 0) {
          (x_dim, y_dim + nCol)
        } else {
          (x_dim + nRow, y_dim)
        }
      },
      combOp = (c1, c2) => (c1, c2) match {
        case ((x_dim1, y_dim1), (x_dim2, y_dim2)) =>
          (x_dim1 + x_dim2, y_dim1 + y_dim2)
      })
  }

  /** Returns the Frobenius Norm of the matrix */
  def normFro(): Double = {
    math.sqrt(rdd.map(lm => lm.mat.values.map(x => math.pow(x, 2)).sum).reduce(_ + _))
  }

  /** Cache the underlying RDD. */
  def cache(): DistributedMatrix = {
    matrixRDD.cache()
    this
  }

  /** Set the storage level for the underlying RDD. */
  def persist(storageLevel: StorageLevel): DistributedMatrix = {
    matrixRDD.persist(storageLevel)
    this
  }

  /** Add a key to the underlying rdd for partitioning and joins. */
  private def keyBy(part: BlockMatrixPartitioner = partitioner): RDD[(Int, SubMatrix)] = {
    rdd.map { block =>
      part match {
        case r: RowBasedPartitioner => (block.blockRowIndex, block)
        case c: ColumnBasedPartitioner => (block.blockColIndex, block)
        case g: GridPartitioner => (block.blockRowIndex + numRowBlocks * block.blockColIndex, block)
        case _ => throw new IllegalArgumentException("Unrecognized partitioner")
      }
    }
  }

  /**
   * Repartition the BlockMatrix using a different partitioner.
   *
   * @param part The partitioner to partition by
   * @return The repartitioned BlockMatrix
   */
  def repartition(part: BlockMatrixPartitioner = partitioner): DistributedMatrix = {
    matrixRDD = keyBy(part)
    this
  }

  /** Collect the distributed matrix on the driver. */
  def collect(): DenseMatrix = {
    val parts = rdd.map(x => ((x.blockRowIndex, x.blockColIndex), x.mat)).
      collect().sortBy(x => (x._1._2, x._1._1))
    val nRows = numRows().toInt
    val nCols = numCols().toInt
    val values = new Array[Double](nRows * nCols)
    var rowStart = 0
    var colStart = 0
    parts.foreach { part =>
      if (part._1._1 == 0) rowStart = 0
      val block = part._2
      var j = 0
      while (j < block.numCols) {
        var i = 0
        val indStart = (j + colStart) * nRows + rowStart
        val indEnd = block.numRows
        val matStart = j * block.numRows
        val mat = block.values
        while (i < indEnd) {
          values(indStart + i) = mat(matStart + i)
          i += 1
        }
        j += 1
      }
      rowStart += block.numRows
      if (part._1._1 == numRowBlocks - 1) colStart += block.numCols
    }
    new DenseMatrix(nRows, nCols, values)
  }

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  private[mllib] def toBreeze(): BDM[Double] = {
    val localMat = collect()
    new BDM[Double](localMat.numRows, localMat.numCols, localMat.values)
  }
}
