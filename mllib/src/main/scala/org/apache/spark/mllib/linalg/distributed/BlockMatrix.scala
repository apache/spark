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
import org.apache.spark.mllib.linalg.{Matrices, DenseMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * Represents a local matrix that makes up one block of a distributed BlockMatrix
 *
 * @param blockIdRow The row index of this block
 * @param blockIdCol The column index of this block
 * @param mat The underlying local matrix
 */
case class SubMatrix(blockIdRow: Int, blockIdCol: Int, mat: DenseMatrix) extends Serializable

/**
 * Information of the submatrices of the BlockMatrix maintained on the driver
 *
 * @param partitionId The id of the partition the block is found in
 * @param blockIdRow The row index of this block
 * @param blockIdCol The column index of this block
 * @param startRow The starting row index with respect to the distributed BlockMatrix
 * @param numRows The number of rows in this block
 * @param startCol The starting column index with respect to the distributed BlockMatrix
 * @param numCols The number of columns in this block
 */
case class SubMatrixInfo(
    partitionId: Int,
    blockIdRow: Int,
    blockIdCol: Int,
    startRow: Long,
    numRows: Int,
    startCol: Long,
    numCols: Int) extends Serializable

/**
 * A partitioner that decides how the matrix is distributed in the cluster
 *
 * @param numPartitions Number of partitions
 * @param rowPerBlock Number of rows that make up each block.
 * @param colPerBlock Number of columns that make up each block.
 */
abstract class BlockMatrixPartitioner(
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
 * @param rdd The RDD of SubMatrixs (local matrices) that form this matrix
 * @param partitioner A partitioner that specifies how SubMatrixs are stored in the cluster
 */
class BlockMatrix(
    val numRowBlocks: Int,
    val numColBlocks: Int,
    val rdd: RDD[SubMatrix],
    val partitioner: BlockMatrixPartitioner) extends DistributedMatrix with Logging {

  // A key-value pair RDD is required to partition properly
  private var matrixRDD: RDD[(Int, SubMatrix)] = keyBy()

  @transient var blockInfo_ : Map[(Int, Int), SubMatrixInfo] = null

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
    val bi = getBlockInfo
    val xDim = bi.map { x =>
      (x._1._1, x._2.numRows.toLong)
    }.groupBy(x => x._1).values.map(_.head._2.toLong).reduceLeft(_ + _)

    val yDim = bi.map { x =>
      (x._1._2, x._2.numCols.toLong)
    }.groupBy(x => x._1).values.map(_.head._2.toLong).reduceLeft(_ + _)

    (xDim, yDim)
  }

  /** Calculates the information for each block and collects it on the driver */
  private def calculateBlockInfo(): Unit = {
    // collect may cause akka frameSize errors
    val blockStartRowColsParts = matrixRDD.mapPartitionsWithIndex { case (partId, iter) =>
      iter.map { case (id, block) =>
        ((block.blockIdRow, block.blockIdCol), (partId, block.mat.numRows, block.mat.numCols))
      }
    }.collect()
    val blockStartRowCols = blockStartRowColsParts.sortBy(_._1)

    // Group blockInfo by rowId, pick the first row and sort on rowId
    val rowReps = blockStartRowCols.groupBy(_._1._1).values.map(_.head).toSeq.sortBy(_._1._1)

    // Group blockInfo by columnId, pick the first column and sort on columnId
    val colReps = blockStartRowCols.groupBy(_._1._2).values.map(_.head).toSeq.sortBy(_._1._2)

    // Calculate startRows
    val cumulativeRowSum = rowReps.scanLeft((0, 0L)) { case (x1, x2) =>
      (x1._1 + 1, x1._2 + x2._2._2)
    }.toMap

    val cumulativeColSum = colReps.scanLeft((0, 0L)) { case (x1, x2) =>
      (x1._1 + 1, x1._2 + x2._2._3)
    }.toMap

    blockInfo_ = blockStartRowCols.map{ case ((rowId, colId), (partId, numRow, numCol)) =>
      ((rowId, colId), new SubMatrixInfo(partId, rowId, colId, cumulativeRowSum(rowId),
        numRow, cumulativeColSum(colId), numCol))
    }.toMap
  }

  /** Returns a map of the information of the blocks that form the distributed matrix. */
  def getBlockInfo: Map[(Int, Int), SubMatrixInfo]  = {
    if (blockInfo_ == null) {
      calculateBlockInfo()
    }
    blockInfo_
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
        case r: RowBasedPartitioner => (block.blockIdRow, block)
        case c: ColumnBasedPartitioner => (block.blockIdCol, block)
        case g: GridPartitioner => (block.blockIdRow + numRowBlocks * block.blockIdCol, block)
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
    val parts = rdd.map(x => ((x.blockIdRow, x.blockIdCol), x.mat)).
      collect().sortBy(x => (x._1._2, x._1._1))
    val nRows = numRows().toInt
    val nCols = numCols().toInt
    val values = new Array[Double](nRows * nCols)
    val blockInfos = getBlockInfo
    parts.foreach { part =>
      val blockInfo = blockInfos((part._1._1, part._1._2))
      // Figure out where this part should be put
      var j = 0
      while (j < blockInfo.numCols) {
        var i = 0
        val indStart = (j + blockInfo.startCol.toInt) * nRows + blockInfo.startRow.toInt
        val indEnd = blockInfo.numRows
        val matStart = j * blockInfo.numRows
        val mat = part._2.values
        while (i < indEnd) {
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
    val localMat = collect()
    new BDM[Double](localMat.numRows, localMat.numCols, localMat.values)
  }

  def add(other: DistributedMatrix): DistributedMatrix = {
    other match {
      // We really need a function to check if two matrices are partitioned similarly
      case otherBlocked: BlockMatrix =>
        if (checkPartitioning(otherBlocked, OperationNames.add)){
          val addedBlocks = rdd.zip(otherBlocked.rdd).map{ case (a, b) =>
            val result = a.mat.toBreeze + b.mat.toBreeze
            new BlockPartition(a.blockIdRow, a.blockIdCol,
              Matrices.fromBreeze(result).asInstanceOf[DenseMatrix])
          }
          new BlockMatrix(numRowBlocks, numColBlocks, addedBlocks, partitioner)
        } else {
          throw new SparkException(
            "Cannot add matrices with non-matching partitioners")
        }
      case _ =>
        throw new IllegalArgumentException("Cannot add matrices of different types")
    }
  }

  def multiply(other: DistributedMatrix): BlockMatrix = {
    other match {
      case otherBlocked: BlockMatrix =>
        if (checkPartitioning(otherBlocked, OperationNames.multiply)){

          val resultPartitioner = new GridPartitioner(numRowBlocks, otherBlocked.numColBlocks,
            partitioner.rowPerBlock, otherBlocked.partitioner.colPerBlock)

          val multiplyBlocks = matrixRDD.join(otherBlocked.matrixRDD, partitioner).
            map { case (key, (mat1, mat2)) =>
            val C = mat1.mat multiply mat2.mat
            (mat1.blockIdRow + numRowBlocks * mat2.blockIdCol, C.toBreeze)
          }.reduceByKey(resultPartitioner, (a, b) => a + b)

          val newBlocks = multiplyBlocks.map{ case (index, mat) =>
            val colId = index / numRowBlocks
            val rowId = index - colId * numRowBlocks
            new BlockPartition(rowId, colId, Matrices.fromBreeze(mat).asInstanceOf[DenseMatrix])
          }
          new BlockMatrix(numRowBlocks, otherBlocked.numColBlocks, newBlocks, resultPartitioner)
        } else {
          throw new SparkException(
            "Cannot multiply matrices with non-matching partitioners")
        }
      case _ =>
        throw new IllegalArgumentException("Cannot add matrices of different types")
    }
  }

  private def checkPartitioning(other: BlockMatrix, operation: Int): Boolean = {
    val otherPartitioner = other.partitioner
    operation match {
      case OperationNames.add =>
        partitioner.equals(otherPartitioner)
      case OperationNames.multiply =>
        partitioner.name == "column" && otherPartitioner.name == "row" &&
          partitioner.numPartitions == otherPartitioner.numPartitions &&
          partitioner.colPerBlock == otherPartitioner.rowPerBlock &&
          numColBlocks == other.numRowBlocks
      case _ =>
        throw new IllegalArgumentException("Unsupported operation")
    }
  }
}

/**
 * Maintains supported and default block matrix operation names.
 *
 * Currently supported operations: `add`, `multiply`.
 */
private object OperationNames {

  val add: Int = 1
  val multiply: Int = 2

}
}
