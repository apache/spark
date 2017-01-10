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

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, SparseVector => BSV, Vector => BV}

import org.apache.spark.{Partitioner, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg._
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
    GridPartitioner(numRowBlocks, numColBlocks, suggestedNumPartitions = blocks.partitions.length)

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
        if (v != 0.0) entryValues += new MatrixEntry(rowStart + i, colStart + j, v)
      }
      entryValues
    }
    new CoordinateMatrix(entryRDD, numRows(), numCols())
  }


  /** Converts to IndexedRowMatrix. The number of columns must be within the integer range. */
  @Since("1.3.0")
  def toIndexedRowMatrix(): IndexedRowMatrix = {
    val cols = numCols().toInt

    require(cols < Int.MaxValue, s"The number of columns should be less than Int.MaxValue ($cols).")

    val rows = blocks.flatMap { case ((blockRowIdx, blockColIdx), mat) =>
      mat.rowIter.zipWithIndex.map {
        case (vector, rowIdx) =>
          blockRowIdx * rowsPerBlock + rowIdx -> (blockColIdx, vector.asBreeze)
      }
    }.groupByKey().map { case (rowIdx, vectors) =>
      val numberNonZeroPerRow = vectors.map(_._2.activeSize).sum.toDouble / cols.toDouble

      val wholeVector = if (numberNonZeroPerRow <= 0.1) { // Sparse at 1/10th nnz
        BSV.zeros[Double](cols)
      } else {
        BDV.zeros[Double](cols)
      }

      vectors.foreach { case (blockColIdx: Int, vec: BV[Double]) =>
        val offset = colsPerBlock * blockColIdx
        wholeVector(offset until Math.min(cols, offset + colsPerBlock)) := vec
      }
      new IndexedRow(rowIdx, Vectors.fromBreeze(wholeVector))
    }
    new IndexedRowMatrix(rows)
  }

  /**
   * Collect the distributed matrix on the driver as a `DenseMatrix`.
   */
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
   * For given matrices `this` and `other` of compatible dimensions and compatible block dimensions,
   * it applies a binary function on their corresponding blocks.
   *
   * @param other The second BlockMatrix argument for the operator specified by `binMap`
   * @param binMap A function taking two breeze matrices and returning a breeze matrix
   * @return A [[BlockMatrix]] whose blocks are the results of a specified binary map on blocks
   *         of `this` and `other`.
   * Note: `blockMap` ONLY works for `add` and `subtract` methods and it does not support
   * operators such as (a, b) => -a + b
   * TODO: Make the use of zero matrices more storage efficient.
   */
  private[mllib] def blockMap(
      other: BlockMatrix,
      binMap: (BM[Double], BM[Double]) => BM[Double]): BlockMatrix = {
    require(numRows() == other.numRows(), "Both matrices must have the same number of rows. " +
      s"A.numRows: ${numRows()}, B.numRows: ${other.numRows()}")
    require(numCols() == other.numCols(), "Both matrices must have the same number of columns. " +
      s"A.numCols: ${numCols()}, B.numCols: ${other.numCols()}")
    if (rowsPerBlock == other.rowsPerBlock && colsPerBlock == other.colsPerBlock) {
      val newBlocks = blocks.cogroup(other.blocks, createPartitioner())
        .map { case ((blockRowIndex, blockColIndex), (a, b)) =>
          if (a.size > 1 || b.size > 1) {
            throw new SparkException("There are multiple MatrixBlocks with indices: " +
              s"($blockRowIndex, $blockColIndex). Please remove them.")
          }
          if (a.isEmpty) {
            val zeroBlock = BM.zeros[Double](b.head.numRows, b.head.numCols)
            val result = binMap(zeroBlock, b.head.asBreeze)
            new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
          } else if (b.isEmpty) {
            new MatrixBlock((blockRowIndex, blockColIndex), a.head)
          } else {
            val result = binMap(a.head.asBreeze, b.head.asBreeze)
            new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
          }
      }
      new BlockMatrix(newBlocks, rowsPerBlock, colsPerBlock, numRows(), numCols())
    } else {
      throw new SparkException("Cannot perform on matrices with different block dimensions")
    }
  }

  /**
   * Adds the given block matrix `other` to `this` block matrix: `this + other`.
   * The matrices must have the same size and matching `rowsPerBlock` and `colsPerBlock`
   * values. If one of the blocks that are being added are instances of `SparseMatrix`,
   * the resulting sub matrix will also be a `SparseMatrix`, even if it is being added
   * to a `DenseMatrix`. If two dense matrices are added, the output will also be a
   * `DenseMatrix`.
   */
  @Since("1.3.0")
  def add(other: BlockMatrix): BlockMatrix =
    blockMap(other, (x: BM[Double], y: BM[Double]) => x + y)

  /**
   * Subtracts the given block matrix `other` from `this` block matrix: `this - other`.
   * The matrices must have the same size and matching `rowsPerBlock` and `colsPerBlock`
   * values. If one of the blocks that are being subtracted are instances of `SparseMatrix`,
   * the resulting sub matrix will also be a `SparseMatrix`, even if it is being subtracted
   * from a `DenseMatrix`. If two dense matrices are subtracted, the output will also be a
   * `DenseMatrix`.
   */
  @Since("2.0.0")
  def subtract(other: BlockMatrix): BlockMatrix =
    blockMap(other, (x: BM[Double], y: BM[Double]) => x - y)

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
      partitioner: GridPartitioner,
      midDimSplitNum: Int): (BlockDestinations, BlockDestinations) = {
    val leftMatrix = blockInfo.keys.collect()
    val rightMatrix = other.blockInfo.keys.collect()

    val rightCounterpartsHelper = rightMatrix.groupBy(_._1).mapValues(_.map(_._2))
    val leftDestinations = leftMatrix.map { case (rowIndex, colIndex) =>
      val rightCounterparts = rightCounterpartsHelper.getOrElse(colIndex, Array.empty[Int])
      val partitions = rightCounterparts.map(b => partitioner.getPartition((rowIndex, b)))
      val midDimSplitIndex = colIndex % midDimSplitNum
      ((rowIndex, colIndex),
        partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
    }.toMap

    val leftCounterpartsHelper = leftMatrix.groupBy(_._2).mapValues(_.map(_._1))
    val rightDestinations = rightMatrix.map { case (rowIndex, colIndex) =>
      val leftCounterparts = leftCounterpartsHelper.getOrElse(rowIndex, Array.empty[Int])
      val partitions = leftCounterparts.map(b => partitioner.getPartition((b, colIndex)))
      val midDimSplitIndex = rowIndex % midDimSplitNum
      ((rowIndex, colIndex),
        partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
    }.toMap

    (leftDestinations, rightDestinations)
  }

  /**
   * Left multiplies this [[BlockMatrix]] to `other`, another [[BlockMatrix]]. The `colsPerBlock`
   * of this matrix must equal the `rowsPerBlock` of `other`. If `other` contains
   * `SparseMatrix`, they will have to be converted to a `DenseMatrix`. The output
   * [[BlockMatrix]] will only consist of blocks of `DenseMatrix`. This may cause
   * some performance issues until support for multiplying two sparse matrices is added.
   *
   * @note The behavior of multiply has changed in 1.6.0. `multiply` used to throw an error when
   * there were blocks with duplicate indices. Now, the blocks with duplicate indices will be added
   * with each other.
   */
  @Since("1.3.0")
  def multiply(other: BlockMatrix): BlockMatrix = {
    multiply(other, 1)
  }

  /**
   * Left multiplies this [[BlockMatrix]] to `other`, another [[BlockMatrix]]. This method add
   * `numMidDimSplits` parameter, which represent how many splits it will cut on the middle
   * dimension when doing matrix multiplication. The intention for this
   * parameter is to solve two problems in old implementation, suppose we have M*N dimensions
   * matrix A multiply N*P dimensions matrix B, when N is much larger than M and P, then,
   *
   *   * When the middle dimension N is too large, it will cause reducer OOM.
   *   * Even if OOM do not occur, it will still cause parallism too low.
   *
   * And through setting proper `numMidDimSplits`, we can also reduce shuffled data size when
   * do matrix multiply. The mechanism of parameter `numMidDimSplits` is shown below:
   *
   * suppose we have block matrix A, contains 200 blocks (`2 numRowBlocks * 100 numColBlocks`),
   * blocks arranged in 2 rows, 100 cols:
   * ```
   * A00 A01 A02 ... A0,99
   * A10 A11 A12 ... A1,99
   * ```
   * and we have block matrix B, also contains 200 blocks (`100 numRowBlocks * 2 numColBlocks`),
   * blocks arranged in 100 rows, 2 cols:
   * ```
   * B00    B01
   * B10    B11
   * B20    B21
   * ...
   * B99,0  B99,1
   * ```
   * Suppose all blocks in the two matrices are dense for now.
   * Now we call `A.multiply(B, numMidDimSplit)`, and suppose the generated `resultPartitioner`
   * contains 2 rowPartitions and 2 colPartitions.
   *
   * When `numMidDimSplit == 1`, it is equivalent to old implementation, it will works as
   * following, containing 2 shuffling step:
   *
   * step-1
   * Step-1 will generate 4 reducer, I tag them as reducer-00, reducer-01, reducer-10, reducer-11,
   * and shuffle data as following:
   * ```
   * A00 A01 A02 ... A0,99
   * B00 B10 B20 ... B99,0    shuffled into reducer-00
   *
   * A00 A01 A02 ... A0,99
   * B01 B11 B21 ... B99,1    shuffled into reducer-01
   *
   * A10 A11 A12 ... A1,99
   * B00 B10 B20 ... B99,0    shuffled into reducer-10
   *
   * A10 A11 A12 ... A1,99
   * B01 B11 B21 ... B99,1    shuffled into reducer-11
   * ```
   *
   * and the shuffling above is a `cogroup` transform, note that each reducer contains only one
   * group.
   *
   * step-2
   * Step-2 will do an `aggregateByKey` transform on the result of step-1, will also generate
   * 4 reducers, and generate the final result RDD, contains 4 partitions, each partition contains
   * one block.
   *
   * The main problems are in step-1. Now we have only 4 reducers, but matrix A and B have 400
   * blocks in total, obviously the reducer number is too small.
   * and, we can see that, each reducer contains only one group(the group concept in `coGroup`
   * transform), each group contains 200 blocks. This is terrible because we know that `coGroup`
   * transformer will load each group into memory when computing. It is un-extensable in the
   * algorithm level.
   * Suppose matrix A has 10000 cols blocks or more instead of 100? Than each reducer will load
   * 20000 blocks into memory. It will easily cause reducer OOM.
   *
   * now we set `numMidDimSplits = 10`, now we can generate 40 reducers in step-1:
   *
   * the reducer-ij above now will be splited into 10 reducers:
   *  reducer-ij0, reducer-ij1, ... reducer-ij9, each reducer will receive 20 blocks.
   * now the shuffle works as following:
   *
   * reducer-000 to reducer-009
   * ```
   * A0,0 A0,10 A0,20 ... A0,90
   * B0,0 B10,0 B20,0 ... B90,0    shuffled into reducer-000
   *
   * A0,1 A0,11 A0,21 ... A0,91
   * B1,0 B11,0 B21,0 ... B91,0    shuffled into reducer-001
   *
   * A0,2 A0,12 A0,22 ... A0,92
   * B2,0 B12,0 B22,0 ... B92,0    shuffled into reducer-002
   *
   * ...
   *
   * A0,9 A0,19 A0,29 ... A0,99
   * B9,0 B19,0 B29,0 ... B99,0    shuffled into reducer-009
   * ```
   *
   * reducer-010 to reducer-019
   * ```
   * A0,0 A0,10 A0,20 ... A0,90
   * B0,1 B10,1 B20,1 ... B90,1    shuffled into reducer-010
   *
   * A0,1 A0,11 A0,21 ... A0,91
   * B1,1 B11,1 B21,1 ... B91,1    shuffled into reducer-011
   *
   * A0,2 A0,12 A0,22 ... A0,92
   * B2,1 B12,1 B22,1 ... B92,1    shuffled into reducer-012
   *
   * ...
   *
   * A0,9 A0,19 A0,29 ... A0,99
   * B9,1 B19,1 B29,1 ... B99,1    shuffled into reducer-019
   * ```
   *
   * reducer-100 to reducer-109 and reducer-110 to reducer-119 is similar to the
   *  above, I omit to write them out.
   *
   * Shuffled data size analysis
   *
   * The optimization has some subtle influence on the total shuffled data size.
   * Appropriate `numMidDimSplits` will significantly reduce the shuffled data size,
   * but too large `numMidDimSplits` may increase the shuffled data in reverse. Use a simple
   * case to represent it here:
   *
   * Suppose we have two same size square matrices X and Y, both have
   * `16 numRowBlocks * 16 numColBlocks`. X and Y are both dense matrix. Now let me analysis
   *  the shuffling data size in the following case:
   *
   * case 1: X and Y both partitioned in 16 rowPartitions and 16 colPartitions, numMidDimSplits = 1
   * ShufflingDataSize = (16 * 16 * (16 + 16) + 16 * 16) blocks = 8448 blocks
   *
   * case 2: X and Y both partitioned in 8 rowPartitions and 8 colPartitions, numMidDimSplits = 4
   * ShufflingDataSize = (8 * 8 * (32 + 32) + 16 * 16 * 4) blocks = 5120 blocks
   *
   * The two cases above all have parallism = 256, case 1 `numMidDimSplits = 1` is equivalent
   * with current implementation in mllib, but case 2 shuffling data is 60.6% of case 1, it shows
   * that proper `numMidDimSplits` will significantly reduce the shuffling data size.
   */
  @Since("2.2.0")
  def multiply(
      other: BlockMatrix,
      numMidDimSplits: Int): BlockMatrix = {
    require(numCols() == other.numRows(), "The number of columns of A and the number of rows " +
      s"of B must be equal. A.numCols: ${numCols()}, B.numRows: ${other.numRows()}. If you " +
      "think they should be equal, try setting the dimensions of A and B explicitly while " +
      "initializing them.")
    if (colsPerBlock == other.rowsPerBlock) {
      val resultPartitioner = GridPartitioner(numRowBlocks, other.numColBlocks,
        math.max(blocks.partitions.length, other.blocks.partitions.length))
      val (leftDestinations, rightDestinations)
        = simulateMultiply(other, resultPartitioner, numMidDimSplits)
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
      val intermediatePartitioner = new Partitioner {
        override def numPartitions: Int = resultPartitioner.numPartitions * numMidDimSplits
        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
      }
      val newBlocks = flatA.cogroup(flatB, intermediatePartitioner).flatMap { case (pId, (a, b)) =>
        a.flatMap { case (leftRowIndex, leftColIndex, leftBlock) =>
          b.filter(_._1 == leftColIndex).map { case (rightRowIndex, rightColIndex, rightBlock) =>
            val C = rightBlock match {
              case dense: DenseMatrix => leftBlock.multiply(dense)
              case sparse: SparseMatrix => leftBlock.multiply(sparse.toDense)
              case _ =>
                throw new SparkException(s"Unrecognized matrix type ${rightBlock.getClass}.")
            }
            ((leftRowIndex, rightColIndex), C.asBreeze)
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
