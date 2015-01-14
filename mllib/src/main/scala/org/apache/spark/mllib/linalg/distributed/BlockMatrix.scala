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

import org.apache.spark.{Logging, Partitioner, SparkException}
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
 * @param rowPerBlock Number of rows that make up each block.
 * @param colPerBlock Number of columns that make up each block.
 */
private[mllib] class GridPartitioner(
    val numRowBlocks: Int,
    val numColBlocks: Int,
    val rowPerBlock: Int,
    val colPerBlock: Int,
    override val numPartitions: Int) extends Partitioner {

  /**
   * Returns the index of the partition the SubMatrix belongs to.
   *
   * @param key The key for the SubMatrix. Can be its position in the grid (its column major index)
   *            or a tuple of three integers that are the final row index after the multiplication,
   *            the index of the block to multiply with, and the final column index after the
   *            multiplication.
   * @return The index of the partition, which the SubMatrix belongs to.
   */
  override def getPartition(key: Any): Int = {
    key match {
      case ind: (Int, Int) =>
        Utils.nonNegativeMod(ind._1 + ind._2 * numRowBlocks, numPartitions)
      case indices: (Int, Int, Int) =>
        Utils.nonNegativeMod(indices._1 + indices._3 * numRowBlocks, numPartitions)
      case _ =>
        throw new IllegalArgumentException("Unrecognized key")
    }
  }

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

  type SubMatrix = ((Int, Int), Matrix) // ((blockRowIndex, blockColIndex), matrix)

  /**
   * Alternate constructor for BlockMatrix without the input of a partitioner. Will use a Grid
   * Partitioner by default.
   *
   * @param numRowBlocks Number of blocks that form the rows of this matrix
   * @param numColBlocks Number of blocks that form the columns of this matrix
   * @param rdd The RDD of SubMatrices (local matrices) that form this matrix
   * @param rowPerBlock Number of rows that make up each block.
   * @param colPerBlock Number of columns that make up each block.
   */
  def this(
      numRowBlocks: Int,
      numColBlocks: Int,
      rdd: RDD[((Int, Int), Matrix)],
      rowPerBlock: Int,
      colPerBlock: Int) = {
    this(numRowBlocks, numColBlocks, rdd)
    val part = new GridPartitioner(numRowBlocks, numColBlocks, rowPerBlock,
      colPerBlock, rdd.partitions.length)
    setPartitioner(part)
  }

  private[mllib] var partitioner: GridPartitioner = {
    val firstSubMatrix = rdd.first()._2
    new GridPartitioner(numRowBlocks, numColBlocks,
      firstSubMatrix.numRows, firstSubMatrix.numCols, rdd.partitions.length)
  }

  /**
   * Set the partitioner for the matrix. For internal use only. Users should use `repartition`.
   * @param part A partitioner that specifies how SubMatrices are stored in the cluster
   */
  private def setPartitioner(part: GridPartitioner): Unit = {
    partitioner = part
  }

  private lazy val dims: (Long, Long) = getDim

  override def numRows(): Long = dims._1
  override def numCols(): Long = dims._2

  /** Returns the dimensions of the matrix. */
  def getDim: (Long, Long) = {
    // picks the sizes of the matrix with the maximum indices
    def pickSizeByGreaterIndex(
        example: (Int, Int, Int, Int),
        base: (Int, Int, Int, Int)): (Int, Int, Int, Int) = {
      if (example._1 > base._1 && example._2 > base._2) {
        (example._1, example._2, example._3, example._4)
      } else if (example._1 > base._1) {
        (example._1, base._2, example._3, base._4)
      } else if (example._2 > base._2) {
        (base._1, example._2, base._3, example._4)
      } else {
        (base._1, base._2, base._3, base._4)
      }
    }

    val lastRowCol = rdd.treeAggregate((0, 0, 0, 0))(
      seqOp = (c, v) => (c, v) match { case (base, ((blockXInd, blockYInd), mat)) =>
        pickSizeByGreaterIndex((blockXInd, blockYInd, mat.numRows, mat.numCols), base)
      },
      combOp = (c1, c2) => (c1, c2) match {
        case (res1, res2) =>
          pickSizeByGreaterIndex(res1, res2)
      })

    (lastRowCol._1.toLong * partitioner.rowPerBlock + lastRowCol._3,
      lastRowCol._2.toLong * partitioner.colPerBlock + lastRowCol._4)
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
  def cache(): DistributedMatrix = {
    rdd.cache()
    this
  }

  /** Set the storage level for the underlying RDD. */
  def persist(storageLevel: StorageLevel): DistributedMatrix = {
    rdd.persist(storageLevel)
    this
  }

  /** Collect the distributed matrix on the driver as a local matrix. */
  def toLocalMatrix(): Matrix = {
    val parts = rdd.collect().sortBy(x => (x._1._2, x._1._1))
    val nRows = numRows().toInt
    val nCols = numCols().toInt
    val values = new Array[Double](nRows * nCols)

    parts.foreach { part =>
      val rowOffset = part._1._1 * partitioner.rowPerBlock
      val colOffset = part._1._2 * partitioner.colPerBlock
      val block = part._2
      var j = 0
      while (j < block.numCols) {
        var i = 0
        val indStart = (j + colOffset) * nRows + rowOffset
        val indEnd = block.numRows
        val matStart = j * block.numRows
        val mat = block.toArray
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
    val localMat = toLocalMatrix()
    new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
  }

  def add(other: DistributedMatrix): DistributedMatrix = {
    other match {
      // We really need a function to check if two matrices are partitioned similarly
      case otherBlocked: BlockMatrix =>
        if (checkPartitioning(otherBlocked, OperationNames.add)){
          val addedBlocks = rdd.zip(otherBlocked.rdd).map{ case (a, b) =>
            val result = a.mat.toBreeze + b.mat.toBreeze
            new SubMatrix(a.blockRowIndex, a.blockColIndex,
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
          val otherPartitioner = otherBlocked.partitioner
          val resultPartitioner = new GridPartitioner(numRowBlocks, otherBlocked.numColBlocks,
            partitioner.rowPerBlock, otherBlocked.partitioner.colPerBlock)

          val newBlocks =
            if (partitioner.name == "column" && otherPartitioner.name == "row" &&
              partitioner.numPartitions == otherPartitioner.numPartitions) {

              val multiplyBlocks = matrixRDD.join(otherBlocked.matrixRDD, partitioner).
                  map { case (key, (mat1, mat2)) =>
                  val C = mat1.mat multiply mat2.mat
                  (mat1.blockRowIndex + numRowBlocks * mat2.blockColIndex, C.toBreeze)
                }.reduceByKey(resultPartitioner, (a, b) => a + b)

              multiplyBlocks.map{ case (index, mat) =>
                val colId = index / numRowBlocks
                val rowId = index - colId * numRowBlocks
                new SubMatrix(rowId, colId, Matrices.fromBreeze(mat).asInstanceOf[DenseMatrix])
              }
            } else {
              val flatA = matrixRDD.flatMap{ case (index, block) =>
                val colId = block.blockColIndex
                val rowId = block.blockRowIndex

                for (j <- 0 until otherBlocked.numColBlocks) yield ((rowId, colId, j), block)
              }

              val flatB = otherBlocked.matrixRDD.flatMap{ case (index, block) =>
                val colId = block.blockColIndex
                val rowId = block.blockRowIndex

                for (i <- 0 until numRowBlocks) yield ((i, rowId, colId), block)
              }

              val multiplyBlocks = flatA.join(flatB, resultPartitioner).
                map { case ((rowId, j, colId), (mat1, mat2)) =>
                  val C = mat1.mat multiply mat2.mat
                  (rowId + numRowBlocks * colId, C.toBreeze)
              }.reduceByKey(resultPartitioner, (a, b) => a + b)

              multiplyBlocks.map{ case (index, mat) =>
                val colId = index / numRowBlocks
                val rowId = index - colId * numRowBlocks
                new SubMatrix(rowId, colId, Matrices.fromBreeze(mat).asInstanceOf[DenseMatrix])
              }
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
        partitioner.rowPerBlock == otherPartitioner.rowPerBlock &&
          partitioner.colPerBlock == otherPartitioner.colPerBlock &&
          numColBlocks == other.numRowBlocks
      case OperationNames.multiply =>
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
