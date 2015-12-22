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

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.{Logging, Partitioner, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Matrix, SparseMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * A grid partitioner, which uses a regular grid to partition coordinates.
 *
 * @param rows Number of rows.
 * @param cols Number of columns.
 * @param rowsPerPart Number of rows per partition, which may be less at the bottom edge.
 * @param colsPerPart Number of columns per partition, which may be less at the right edge.
 */
private[mllib] class GridPartitioner(
    val rows: Int,
    val cols: Int,
    val rowsPerPart: Int,
    val colsPerPart: Int) extends Partitioner {

  require(rows > 0)
  require(cols > 0)
  require(rowsPerPart > 0)
  require(colsPerPart > 0)

  private val rowPartitions = math.ceil(rows * 1.0 / rowsPerPart).toInt
  private val colPartitions = math.ceil(cols * 1.0 / colsPerPart).toInt

  override val numPartitions: Int = rowPartitions * colPartitions

  /**
   * Returns the index of the partition the input coordinate belongs to.
   *
   * @param key The partition id i (calculated through this method for coordinate (i, j) in
   *            `simulateMultiply`, the coordinate (i, j) or a tuple (i, j, k), where k is
   *            the inner index used in multiplication. k is ignored in computing partitions.
   * @return The index of the partition, which the coordinate belongs to.
   */
  override def getPartition(key: Any): Int = {
    key match {
      case i: Int => i
      case (i: Int, j: Int) =>
        getPartitionId(i, j)
      case (i: Int, j: Int, _: Int) =>
        getPartitionId(i, j)
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized key: $key.")
    }
  }

  /** Partitions sub-matrices as blocks with neighboring sub-matrices. */
  private def getPartitionId(i: Int, j: Int): Int = {
    require(0 <= i && i < rows, s"Row index $i out of range [0, $rows).")
    require(0 <= j && j < cols, s"Column index $j out of range [0, $cols).")
    i / rowsPerPart + j / colsPerPart * rowPartitions
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case r: GridPartitioner =>
        (this.rows == r.rows) && (this.cols == r.cols) &&
          (this.rowsPerPart == r.rowsPerPart) && (this.colsPerPart == r.colsPerPart)
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      rows: java.lang.Integer,
      cols: java.lang.Integer,
      rowsPerPart: java.lang.Integer,
      colsPerPart: java.lang.Integer)
  }
}

private[mllib] object GridPartitioner {

  /** Creates a new [[GridPartitioner]] instance. */
  def apply(rows: Int, cols: Int, rowsPerPart: Int, colsPerPart: Int): GridPartitioner = {
    new GridPartitioner(rows, cols, rowsPerPart, colsPerPart)
  }

  /** Creates a new [[GridPartitioner]] instance with the input suggested number of partitions. */
  def apply(rows: Int, cols: Int, suggestedNumPartitions: Int): GridPartitioner = {
    require(suggestedNumPartitions > 0)
    val scale = 1.0 / math.sqrt(suggestedNumPartitions)
    val rowsPerPart = math.round(math.max(scale * rows, 1.0)).toInt
    val colsPerPart = math.round(math.max(scale * cols, 1.0)).toInt
    new GridPartitioner(rows, cols, rowsPerPart, colsPerPart)
  }
}

/**
 * Represents a distributed matrix in blocks of local matrices.
 *
 * @param blocks The RDD of sub-matrix blocks ((blockRowIndex, blockColIndex), sub-matrix) that
 *               form this distributed matrix. If multiple blocks with the same index exist, the
 *               results for operations like add and multiply will be unpredictable.
 * @param rowsPerBlock Number of rows that make up each block. The blocks forming the final
 *                     rows are not required to have the given number of rows
 * @param colsPerBlock Number of columns that make up each block. The blocks forming the final
 *                     columns are not required to have the given number of columns
 * @param nRows Number of rows of this matrix. If the supplied value is less than or equal to zero,
 *              the number of rows will be calculated when `numRows` is invoked.
 * @param nCols Number of columns of this matrix. If the supplied value is less than or equal to
 *              zero, the number of columns will be calculated when `numCols` is invoked.
 */
@Since("1.3.0")
class BlockMatrix @Since("1.3.0") (
    @Since("1.3.0") val blocks: RDD[((Int, Int), Matrix)],
    @Since("1.3.0") val rowsPerBlock: Int,
    @Since("1.3.0") val colsPerBlock: Int,
    private var nRows: Long,
    private var nCols: Long) extends DistributedMatrix with Logging {

  private type MatrixBlock = ((Int, Int), Matrix) // ((blockRowIndex, blockColIndex), sub-matrix)

  /**
   * Alternate constructor for BlockMatrix without the input of the number of rows and columns.
   *
   * @param blocks The RDD of sub-matrix blocks ((blockRowIndex, blockColIndex), sub-matrix) that
   *               form this distributed matrix. If multiple blocks with the same index exist, the
   *               results for operations like add and multiply will be unpredictable.
   * @param rowsPerBlock Number of rows that make up each block. The blocks forming the final
   *                     rows are not required to have the given number of rows
   * @param colsPerBlock Number of columns that make up each block. The blocks forming the final
   *                     columns are not required to have the given number of columns
   */
  @Since("1.3.0")
  def this(
      blocks: RDD[((Int, Int), Matrix)],
      rowsPerBlock: Int,
      colsPerBlock: Int) = {
    this(blocks, rowsPerBlock, colsPerBlock, 0L, 0L)
  }

  @Since("1.3.0")
  override def numRows(): Long = {
    if (nRows <= 0L) estimateDim()
    nRows
  }

  @Since("1.3.0")
  override def numCols(): Long = {
    if (nCols <= 0L) estimateDim()
    nCols
  }

  @Since("1.3.0")
  val numRowBlocks = math.ceil(numRows() * 1.0 / rowsPerBlock).toInt
  @Since("1.3.0")
  val numColBlocks = math.ceil(numCols() * 1.0 / colsPerBlock).toInt

  private[mllib] def createPartitioner(): GridPartitioner =
    GridPartitioner(numRowBlocks, numColBlocks, suggestedNumPartitions = blocks.partitions.size)

  private lazy val blockInfo = blocks.mapValues(block => (block.numRows, block.numCols)).cache()

  /** Estimates the dimensions of the matrix. */
  private def estimateDim(): Unit = {
    val (rows, cols) = blockInfo.map { case ((blockRowIndex, blockColIndex), (m, n)) =>
      (blockRowIndex.toLong * rowsPerBlock + m,
        blockColIndex.toLong * colsPerBlock + n)
    }.reduce { (x0, x1) =>
      (math.max(x0._1, x1._1), math.max(x0._2, x1._2))
    }
    if (nRows <= 0L) nRows = rows
    assert(rows <= nRows, s"The number of rows $rows is more than claimed $nRows.")
    if (nCols <= 0L) nCols = cols
    assert(cols <= nCols, s"The number of columns $cols is more than claimed $nCols.")
  }

  /**
   * Validates the block matrix info against the matrix data (`blocks`) and throws an exception if
   * any error is found.
   */
  @Since("1.3.0")
  def validate(): Unit = {
    logDebug("Validating BlockMatrix...")
    // check if the matrix is larger than the claimed dimensions
    estimateDim()
    logDebug("BlockMatrix dimensions are okay...")

    // Check if there are multiple MatrixBlocks with the same index.
    blockInfo.countByKey().foreach { case (key, cnt) =>
      if (cnt > 1) {
        throw new SparkException(s"Found multiple MatrixBlocks with the indices $key. Please " +
          "remove blocks with duplicate indices.")
      }
    }
    logDebug("MatrixBlock indices are okay...")
    // Check if each MatrixBlock (except edges) has the dimensions rowsPerBlock x colsPerBlock
    // The first tuple is the index and the second tuple is the dimensions of the MatrixBlock
    val dimensionMsg = s"dimensions different than rowsPerBlock: $rowsPerBlock, and " +
      s"colsPerBlock: $colsPerBlock. Blocks on the right and bottom edges can have smaller " +
      s"dimensions. You may use the repartition method to fix this issue."
    blockInfo.foreach { case ((blockRowIndex, blockColIndex), (m, n)) =>
      if ((blockRowIndex < numRowBlocks - 1 && m != rowsPerBlock) ||
          (blockRowIndex == numRowBlocks - 1 && (m <= 0 || m > rowsPerBlock))) {
        throw new SparkException(s"The MatrixBlock at ($blockRowIndex, $blockColIndex) has " +
          dimensionMsg)
      }
      if ((blockColIndex < numColBlocks - 1 && n != colsPerBlock) ||
        (blockColIndex == numColBlocks - 1 && (n <= 0 || n > colsPerBlock))) {
        throw new SparkException(s"The MatrixBlock at ($blockRowIndex, $blockColIndex) has " +
          dimensionMsg)
      }
    }
    logDebug("MatrixBlock dimensions are okay...")
    logDebug("BlockMatrix is valid!")
  }

  /** Caches the underlying RDD. */
  @Since("1.3.0")
  def cache(): this.type = {
    blocks.cache()
    this
  }

  /** Persists the underlying RDD with the specified storage level. */
  @Since("1.3.0")
  def persist(storageLevel: StorageLevel): this.type = {
    blocks.persist(storageLevel)
    this
  }

  /** Converts to CoordinateMatrix. */
  @Since("1.3.0")
  def toCoordinateMatrix(): CoordinateMatrix = {
    val entryRDD = blocks.flatMap { case ((blockRowIndex, blockColIndex), mat) =>
      val rowStart = blockRowIndex.toLong * rowsPerBlock
      val colStart = blockColIndex.toLong * colsPerBlock
      val entryValues = new ArrayBuffer[MatrixEntry]()
      mat.foreachActive { (i, j, v) =>
        if (v != 0.0) entryValues.append(new MatrixEntry(rowStart + i, colStart + j, v))
      }
      entryValues
    }
    new CoordinateMatrix(entryRDD, numRows(), numCols())
  }

  /** Converts to IndexedRowMatrix. The number of columns must be within the integer range. */
  @Since("1.3.0")
  def toIndexedRowMatrix(): IndexedRowMatrix = {
    require(numCols() < Int.MaxValue, "The number of columns must be within the integer range. " +
      s"numCols: ${numCols()}")
    // TODO: This implementation may be optimized
    toCoordinateMatrix().toIndexedRowMatrix()
  }

  /** Collect the distributed matrix on the driver as a `DenseMatrix`. */
  @Since("1.3.0")
  def toLocalMatrix(): Matrix = {
    require(numRows() < Int.MaxValue, "The number of rows of this matrix should be less than " +
      s"Int.MaxValue. Currently numRows: ${numRows()}")
    require(numCols() < Int.MaxValue, "The number of columns of this matrix should be less than " +
      s"Int.MaxValue. Currently numCols: ${numCols()}")
    require(numRows() * numCols() < Int.MaxValue, "The length of the values array must be " +
      s"less than Int.MaxValue. Currently numRows * numCols: ${numRows() * numCols()}")
    val m = numRows().toInt
    val n = numCols().toInt
    val mem = m * n / 125000
    if (mem > 500) logWarning(s"Storing this matrix will require $mem MB of memory!")
    val localBlocks = blocks.collect()
    val values = new Array[Double](m * n)
    localBlocks.foreach { case ((blockRowIndex, blockColIndex), submat) =>
      val rowOffset = blockRowIndex * rowsPerBlock
      val colOffset = blockColIndex * colsPerBlock
      submat.foreachActive { (i, j, v) =>
        val indexOffset = (j + colOffset) * m + rowOffset + i
        values(indexOffset) = v
      }
    }
    new DenseMatrix(m, n, values)
  }

  /**
   * Transpose this `BlockMatrix`. Returns a new `BlockMatrix` instance sharing the
   * same underlying data. Is a lazy operation.
   */
  @Since("1.3.0")
  def transpose: BlockMatrix = {
    val transposedBlocks = blocks.map { case ((blockRowIndex, blockColIndex), mat) =>
      ((blockColIndex, blockRowIndex), mat.transpose)
    }
    new BlockMatrix(transposedBlocks, colsPerBlock, rowsPerBlock, nCols, nRows)
  }

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  private[mllib] def toBreeze(): BDM[Double] = {
    val localMat = toLocalMatrix()
    new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
  }

  /**
   * Adds two block matrices together. The matrices must have the same size and matching
   * `rowsPerBlock` and `colsPerBlock` values. If one of the blocks that are being added are
   * instances of [[SparseMatrix]], the resulting sub matrix will also be a [[SparseMatrix]], even
   * if it is being added to a [[DenseMatrix]]. If two dense matrices are added, the output will
   * also be a [[DenseMatrix]].
   */
  @Since("1.3.0")
  def add(other: BlockMatrix): BlockMatrix = {
    require(numRows() == other.numRows(), "Both matrices must have the same number of rows. " +
      s"A.numRows: ${numRows()}, B.numRows: ${other.numRows()}")
    require(numCols() == other.numCols(), "Both matrices must have the same number of columns. " +
      s"A.numCols: ${numCols()}, B.numCols: ${other.numCols()}")
    if (rowsPerBlock == other.rowsPerBlock && colsPerBlock == other.colsPerBlock) {
      val addedBlocks = blocks.cogroup(other.blocks, createPartitioner())
        .map { case ((blockRowIndex, blockColIndex), (a, b)) =>
          if (a.size > 1 || b.size > 1) {
            throw new SparkException("There are multiple MatrixBlocks with indices: " +
              s"($blockRowIndex, $blockColIndex). Please remove them.")
          }
          if (a.isEmpty) {
            new MatrixBlock((blockRowIndex, blockColIndex), b.head)
          } else if (b.isEmpty) {
            new MatrixBlock((blockRowIndex, blockColIndex), a.head)
          } else {
            val result = a.head.toBreeze + b.head.toBreeze
            new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
          }
      }
      new BlockMatrix(addedBlocks, rowsPerBlock, colsPerBlock, numRows(), numCols())
    } else {
      throw new SparkException("Cannot add matrices with different block dimensions")
    }
  }

  /** Block (i,j) --> Set of destination partitions */
  private type BlockDestinations = Map[(Int, Int), Set[Int]]

  /**
   * Simulate the multiplication with just block indices in order to cut costs on communication,
   * when we are actually shuffling the matrices.
   * The `colsPerBlock` of this matrix must equal the `rowsPerBlock` of `other`.
   * Exposed for tests.
   *
   * @param other The BlockMatrix to multiply
   * @param partitioner The partitioner that will be used for the resulting matrix `C = A * B`
   * @return A tuple of [[BlockDestinations]]. The first element is the Map of the set of partitions
   *         that we need to shuffle each blocks of `this`, and the second element is the Map for
   *         `other`.
   */
  private[distributed] def simulateMultiply(
      other: BlockMatrix,
      partitioner: GridPartitioner): (BlockDestinations, BlockDestinations) = {
    val leftMatrix = blockInfo.keys.collect() // blockInfo should already be cached
    val rightMatrix = other.blocks.keys.collect()
    val leftDestinations = leftMatrix.map { case (rowIndex, colIndex) =>
      val rightCounterparts = rightMatrix.filter(_._1 == colIndex)
      val partitions = rightCounterparts.map(b => partitioner.getPartition((rowIndex, b._2)))
      ((rowIndex, colIndex), partitions.toSet)
    }.toMap
    val rightDestinations = rightMatrix.map { case (rowIndex, colIndex) =>
      val leftCounterparts = leftMatrix.filter(_._2 == rowIndex)
      val partitions = leftCounterparts.map(b => partitioner.getPartition((b._1, colIndex)))
      ((rowIndex, colIndex), partitions.toSet)
    }.toMap
    (leftDestinations, rightDestinations)
  }

  /**
   * Left multiplies this [[BlockMatrix]] to `other`, another [[BlockMatrix]]. The `colsPerBlock`
   * of this matrix must equal the `rowsPerBlock` of `other`. If `other` contains
   * [[SparseMatrix]], they will have to be converted to a [[DenseMatrix]]. The output
   * [[BlockMatrix]] will only consist of blocks of [[DenseMatrix]]. This may cause
   * some performance issues until support for multiplying two sparse matrices is added.
   *
   * Note: The behavior of multiply has changed in 1.6.0. `multiply` used to throw an error when
   * there were blocks with duplicate indices. Now, the blocks with duplicate indices will be added
   * with each other.
   */
  @Since("1.3.0")
  def multiply(other: BlockMatrix): BlockMatrix = {
    require(numCols() == other.numRows(), "The number of columns of A and the number of rows " +
      s"of B must be equal. A.numCols: ${numCols()}, B.numRows: ${other.numRows()}. If you " +
      "think they should be equal, try setting the dimensions of A and B explicitly while " +
      "initializing them.")
    if (colsPerBlock == other.rowsPerBlock) {
      val resultPartitioner = GridPartitioner(numRowBlocks, other.numColBlocks,
        math.max(blocks.partitions.length, other.blocks.partitions.length))
      val (leftDestinations, rightDestinations) = simulateMultiply(other, resultPartitioner)
      // Each block of A must be multiplied with the corresponding blocks in the columns of B.
      val flatA = blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
        val destinations = leftDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
        destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
      }
      // Each block of B must be multiplied with the corresponding blocks in each row of A.
      val flatB = other.blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
        val destinations = rightDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
        destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
      }
      val newBlocks = flatA.cogroup(flatB, resultPartitioner).flatMap { case (pId, (a, b)) =>
        a.flatMap { case (leftRowIndex, leftColIndex, leftBlock) =>
          b.filter(_._1 == leftColIndex).map { case (rightRowIndex, rightColIndex, rightBlock) =>
            val C = rightBlock match {
              case dense: DenseMatrix => leftBlock.multiply(dense)
              case sparse: SparseMatrix => leftBlock.multiply(sparse.toDense)
              case _ =>
                throw new SparkException(s"Unrecognized matrix type ${rightBlock.getClass}.")
            }
            ((leftRowIndex, rightColIndex), C.toBreeze)
          }
        }
      }.reduceByKey(resultPartitioner, (a, b) => a + b).mapValues(Matrices.fromBreeze)
      // TODO: Try to use aggregateByKey instead of reduceByKey to get rid of intermediate matrices
      new BlockMatrix(newBlocks, rowsPerBlock, other.colsPerBlock, numRows(), other.numCols())
    } else {
      throw new SparkException("colsPerBlock of A doesn't match rowsPerBlock of B. " +
        s"A.colsPerBlock: $colsPerBlock, B.rowsPerBlock: ${other.rowsPerBlock}")
    }
  }
}
