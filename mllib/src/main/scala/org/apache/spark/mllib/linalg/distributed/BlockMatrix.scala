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

import breeze.linalg.{DenseMatrix => BDM, inv,
    LU, upperTriangular, lowerTriangular, diag, DenseVector}

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
 *              the number of rows will be calculated when `numRows` is inv(invoked.)
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
      throw new SparkException("Cannot add matrices with different block dimensions." +
        s"A.rowsPerBlock: ${this.rowsPerBlock}, B.rowsPerBlock: ${other.rowsPerBlock}")
    }
  }

  /**
   * subtracts two block matrices A.subtract(B) as A-B.  Works exactly as add
   *
   * @param BlockMatrix to be subtracted
   * @return BlockMatrix
   * @since 1.6.0
   */
  def subtract(other: BlockMatrix): BlockMatrix = {
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
          val result = -1.0 * b.head.toBreeze
          new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
        } else {
          val result = a.head.toBreeze - b.head.toBreeze
          new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
        }
      }
      new BlockMatrix(addedBlocks, rowsPerBlock, colsPerBlock, numRows(), numCols())
    } else {
      throw new SparkException("Cannot subtract matrices with different block dimensions." +
        s"A.rowsPerBlock: ${this.rowsPerBlock}, B.rowsPerBlock: ${other.rowsPerBlock}")
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
<<<<<<< HEAD
   * @since 1.3.0
=======
   * Note: The behavior of multiply has changed in 1.6.0. `multiply` used to throw an error when
   * there were blocks with duplicate indices. Now, the blocks with duplicate indices will be added
   * with each other.
>>>>>>> 7ee7d5a3c4ff77d2cee2afce36ff41f6302e6315
   */
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

  /**
   * Schur Complement of a BlockMatrix.  For a matrix that is in 4 partitions:
   *  A=[a11, a12; a21; a22], the Schur Complement S is S = a22 - (a21 * a11^-1 * a12).
   * The Schur Complement is always (n-1) x (n-1), which is the size of a22. a11 is expected
   * to fit into memory so that Breeze inversions can be computed.
   *
   * @return BlockMatrix Schur Complement as BlockMatrix
   * @since 1.6.0
   */
  private[mllib] def SchurComplement: BlockMatrix = {
    require(this.numRowBlocks == this.numColBlocks, "Block Matrix must be square.")
    require(this.numRowBlocks > 1, "Block Matrix must be larger than one block.")
    val topRange = (0, 0)
    val botRange = (1, this.numColBlocks - 1)
    val a11 = this.subBlock(topRange, topRange)
    val a12 = this.subBlock(topRange, botRange)
    val a21 = this.subBlock(botRange, topRange)
    val a22 = this.subBlock(botRange, botRange)

    val a11Brz = inv(a11.toBreeze) // note that intermediate a11 calcs derive from inv(a11)
    val a11Mtx = Matrices.dense(a11.numRows.toInt, a11.numCols.toInt, a11Brz.toArray)
    val a11RDD = this.blocks.sparkContext.parallelize(Seq(((0, 0), a11Mtx)))
    val a11Inv = new BlockMatrix(a11RDD, this.rowsPerBlock, this.colsPerBlock)

    a22.subtract(a21.multiply(a11Inv.multiply(a12)))
  }

  /**
   * Returns a rectangular (sub)BlockMatrix with block ranges as specified.  Block Ranges
   * refer to a range of blocks that each contain a matrix.  The returned BlockMatrix
   * is numbered so that the upper left block is indexed as (0,0).
   *
   *
   * @param blockRowRange The lower and upper row range of blocks, as (Int,Int)
   * @param blockColRange The lower and upper col range of blocks, as (Int, Int)
   * @return a BlockMatrix with (0,0) as the upper leftmost block index
   * @since 1.6.0
   */
  private [mllib] def subBlock(blockRowRange: (Int, Int), blockColRange: (Int, Int)): BlockMatrix = {

    val rowMin = blockRowRange._1
    val rowMax = blockRowRange._2
    val colMin = blockColRange._1
    val colMax = blockColRange._2
    val extractedSeq = this.blocks.filter{ case((x, y), matrix) =>
      x >= rowMin && x<= rowMax &&         // finding blocks
        y >= colMin && y<= colMax }.map {   // shifting indices
      case(((x, y), matrix) ) => ((x-rowMin, y-colMin), matrix)
    }
    new BlockMatrix(extractedSeq, rowsPerBlock, colsPerBlock)
  }

  /**
   * Computes the LU decomposition of a Single Block from BlockMatrix using the
   * Breeze LU method.  The method (as written) operates -only- on the upper
   * left (0,0) corner of the BlockMatrix.
   *
   * @return List[BDM[Double]] of Breeze Matrices (BDM) (P,L,U) for blockLU method.
   * @since 1.6.0
   */
  private [mllib] def singleBlockPLU: List[BDM[Double]] = {
    // returns PA = LU factorization from Breeze
    val PLU = LU(this.subBlock((0, 0), (0, 0)).toBreeze)
    val k = PLU._1.cols
    val L = lowerTriangular(PLU._1) - diag(diag(PLU._1)) + diag(DenseVector.fill(k){1.0})
    val U = upperTriangular(PLU._1);
    var P = diag(DenseVector.fill(k){1.0})
    var Pi = diag(DenseVector.fill(k){1.0})
    // size of square matrix
    // populating permutation matrix
    var i = 0
    while (i < k) {
      val I = {
        if (i == 0){k - 1}
        else {i - 1}
      }
      val J = PLU._2(i) -1
      if (i != J) {
        Pi(i, J) += 1.0
        Pi(J, i) += 1.0
        Pi(i, i) -= 1.0
        Pi(J, J) -= 1.0
      }
      P = Pi * P  // constructor Pi*P for PA=LU
      // resetting Pi for next iteration
      if (i != J) {
        Pi(i, J) -= 1.0
        Pi(J, i) -= 1.0
        Pi(i, i) += 1.0
        Pi(J, J) += 1.0
      }
      i += 1
    }
    List(P, L, U)
  }


  /**
   * This method reassigns 'absolute' index locations (i,j), to sequences.  This is
   * designed to reconsitute the orignal block locations that were lost in the
   * subBlock method.
   *
   * @param rowMin The new lowest row value
   * @param colMin The new lowest column value
   * @return an RDD of Sequences with new block indexing
   * @since 1.6.0
   *
   */
  private [mllib] def shiftIndices(rowMin: Int, colMin: Int): RDD[((Int, Int), Matrix)] = {
    // This routine recovers the absolute indexing of the block matrices for reassembly
    val extractedSeq = this.blocks.map {   // shifting indices
      case(((x, y), matrix)) => ((x + rowMin, y + colMin), matrix)
    }
    extractedSeq
  }

  /**
   * A class that contains the 3 main BlockMatrix items to be returned
   * when calling blockLU.
   *
   * @param P The Permutation BlockMatrix
   * @param L Lower Diagonal BlockMatrix
   * @param U Upper Diagonal BlockMatrix
   *
   */
  private [mllib] case class PLU(P: BlockMatrix, L: BlockMatrix, U: BlockMatrix)

  /**
   * Extends the base class PLU with additional matrices that are used
   * int he solve method.
   *
   * @param P The Permutation BlockMatrix
   * @param L Lower Diagonal BlockMatrix
   * @param U Upper Diagonal BlockMatrix
   *
   * @param dLi The inverse of the lower diagaonal matrices (in (i,i)th
   *             cells only).
   * @param dUi The inverse of the upper diagaonal matrices (in (i,i)th
   *             cells only).
   */
  private [mllib] case class PLUandInverses(
      P: BlockMatrix,
      L: BlockMatrix,
      U: BlockMatrix,
      dLi: BlockMatrix,
      dUi: BlockMatrix)

  /**
   * Computes the LU Decomposition of a Square Matrix.  For a matrix A of size (n x n)
   * LU decomposition computes the Lower Triangular Matrix L, the Upper Triangular
   * Matrix U, along with a Permutation Matrix P, such that PA=LU.  The Permutation
   * Matrix addresses cases where zero entries prevent forward substitution
   * solution of L or U.
   *
   * The BlockMatrix version takes a BlockMatrix as an input and returns a Tuple
   * of 5 BlockMatrix objects:
   * P, L, U (in that order), such that P.multiply(A)-L.multiply(U) = 0
   * and Li, Ui, which are the inverse of the block diagonal terms for L and U.
   *
   * The blockLU method will return only P,L, and U, but blockLUtoSolver will return
   * the extra Li and Ui matrices, which will be used by the solve method
   * so that it does not need to recompute these values.
   *
   * The method follows a procedure similar to the method used in ScaLAPACK, but
   * places more emphasis on preparing BlockMatrix objects as inputs to large
   * BlockMatrix.multiply operations.
   *
   *
   * @return  PLUandInverses(P,L,U,Li,Ui) as a Tuple of BlockMatrix
   * @since 1.6.0
   */
  private [mllib] def blockLUtoSolver: PLUandInverses = {

    // builds up the array as a union of RDD sets
    val nDiagBlocks = this.numColBlocks
    // Matrix changes shape during recursion...the "absolute location" must be
    // preserved when reconstructing.
    val rowsAbs = this.numRowBlocks
    val colsAbs = rowsAbs
    // accessing the spark context
    val sc = this.blocks.sparkContext

    /**
     * LUSequences is a class that is defined to make the recursiveSequencesBuild section
     * more readable.
     *
     * These are passed as an RDD of blocks:
     * @param P the permutation matrix.
     * @param L the lower diagonal matrix.
     * @param U the upper diagonal matrix.
     * @param Li the inverse lower diagonal matrix (only populating (i,i) cells).
     * @param Ui the inverse upper diagonal matrix (only populating (i,i) cells).
     * @param LD the lower diagonal matrices (only populating (i,i) cells).
     * @param UD the upper diagonal matrices (only populating (i,i) cells).
     * This is passed as a BlockMatrix
     * @param A the Schur Complement from the previous iteration, treated as the source matrix
     *          for the next iteraton.
     *
     *
     * @Since("1.6.0")
     */

    case class LUSequences(
        P: RDD[((Int, Int), Matrix)],
        L: RDD[((Int, Int), Matrix)],
        U: RDD[((Int, Int), Matrix)],
        Li: RDD[((Int, Int), Matrix)],
        Ui: RDD[((Int, Int), Matrix)],
        LD: RDD[((Int, Int), Matrix)],
        UD: RDD[((Int, Int), Matrix)],
        A: BlockMatrix)

    /**
     * Recursive Sequence Build is a nested recursion method that builds up all of the
     * sequences that are converted to BlockMatrix classes for large matrix
     * multiplication operations.  The Schur Complement is calculated at each
     * recursion step and fed into the next iteration as the input matrix.
     *
     * dP, dL, dU, dLi, dUi have the solutions to LU(S) in the (i,i) (diagonal) blocks.
     * dLi and dUi, are the inverses of each block in dL and dU.  UD and LD contain the
     * extracted subBlocks from the incoming matrix (which is the Schur Complement
     * from the previous iteration).  These matrices occupy the U12 and L21 spaces at
     * each iteration, and form the strict upper and lower block diagonal matrices,
     * respectively.  This means that for UD, only (i,j>i) blocks are populated with
     * the cascading Schur calculations, while for LD, (i, j<i) blocks are populated.
     *
     * @param rowI
     * @param prev
     * @return dP, dL, dU, dLi, dUi, LD, UD, S  All are RDDs of Sequences that are
     *         iteratively built, while S is a BlockMatrix used in the recursion loop
     * @since 1.6.0
     */
    def recursiveSequencesBuild(rowI: Int, prev: LUSequences): LUSequences = {

      val rowsRel = prev.A.numRowBlocks
      val colsRel = prev.A.numColBlocks
      val topRangeRel = (0, 0)
      val botRangeRel = (1, rowsRel - 1)
      val topRangeAbs = (rowI, rowI)
      val botRangeAbs = (rowI + 1, rowsAbs - 1 )
      val PLU: List[BDM[Double]] = prev.A.singleBlockPLU
      val PBrz = PLU(0)
      val LBrz = PLU(1)
      val UBrz = PLU(2)

      val P = Matrices.dense(PBrz.rows, PBrz.cols, PBrz.toArray)
      val L = Matrices.dense(LBrz.rows, LBrz.cols, LBrz.toArray)
      val U = Matrices.dense(UBrz.rows, UBrz.cols, UBrz.toArray)
     // packing into parallel collections and appending
      val nextP = sc.parallelize(Seq(((rowI, rowI), P)))
      val nextL = sc.parallelize(Seq(((rowI, rowI), L)))
      val nextU = sc.parallelize(Seq(((rowI, rowI), U)))
      if (rowI == nDiagBlocks-1){ // terminal case
        // padding (Last,Last) blocks of UD and LD matrices with Zero matrix for last iteration
        val ZB = BDM.zeros[Double](LBrz.rows, LBrz.cols)
        val Z = Matrices.dense(LBrz.rows, LBrz.cols, ZB.toArray)
        val lastZ = sc.parallelize(Seq(((rowsAbs-1, colsAbs-1), Z)))
        val nextTuple = new LUSequences(nextP ++ prev.P, nextL ++ prev.L, nextU ++ prev.U,
                            lastZ ++ prev.Li, lastZ ++ prev.Ui,
                            lastZ ++ prev.LD, lastZ ++ prev.UD, prev.A)
        nextTuple
      }
      else {                      // recursion block
        val Li = Matrices.dense(LBrz.rows, LBrz.cols, inv(LBrz).toArray)
        val Ui = Matrices.dense(UBrz.rows, UBrz.cols, inv(UBrz).toArray)
        val nextLi = sc.parallelize(Seq(((rowI, rowI), Li)))
        val nextUi = sc.parallelize(Seq(((rowI, rowI), Ui)))

        val nextLD = prev.A.subBlock(botRangeRel, topRangeRel).
          shiftIndices(botRangeAbs._1, topRangeAbs._1)
        val nextUD = prev.A.subBlock(topRangeRel, botRangeRel).
          shiftIndices(topRangeAbs._1, botRangeAbs._1)

        val nextTuple = new LUSequences(nextP ++ prev.P, nextL ++ prev.L, nextU ++ prev.U,
                            nextLi ++ prev.Li, nextUi ++ prev.Ui,
                            prev.LD ++ nextLD, prev.UD ++ nextUD, prev.A.SchurComplement)
        recursiveSequencesBuild(rowI + 1, nextTuple)
      }
    }

    // first iteration
    val PLU: List[BDM[Double]] = this.singleBlockPLU
    val PBrz = PLU(0)
    val LBrz = PLU(1)
    val UBrz = PLU(2)
    val P = Matrices.dense(this.rowsPerBlock, this.colsPerBlock, PBrz.toArray)
    val L = Matrices.dense(this.rowsPerBlock, this.colsPerBlock, LBrz.toArray)
    val U = Matrices.dense(this.rowsPerBlock, this.colsPerBlock, UBrz.toArray)

    val Li = Matrices.dense(this.rowsPerBlock, this.colsPerBlock, inv(LBrz).toArray)
    val Ui = Matrices.dense(this.rowsPerBlock, this.colsPerBlock, inv(UBrz).toArray)
    // packing into parallel collections and appending
    val nextP = sc.parallelize(Seq(((0, 0), P)))
    val nextL = sc.parallelize(Seq(((0, 0), L)))
    val nextU = sc.parallelize(Seq(((0, 0), U)))
    val nextLi = sc.parallelize(Seq(((0, 0), Li)))
    val nextUi = sc.parallelize(Seq(((0, 0), Ui)))

    // padding (0,0) blocks of UD and LD matrices with Zero matrix for first iteration
    val ZB = BDM.zeros[Double](this.rowsPerBlock, this.colsPerBlock)
    val Z = Matrices.dense(this.rowsPerBlock, this.colsPerBlock, ZB.toArray)
    val firstZ = sc.parallelize(Seq(((0, 0), Z)))
    val topRange = (0, 0)
    val botRange = (1, this.numRowBlocks-1)

    val nextLD = this.subBlock(botRange, topRange).
      shiftIndices(botRange._1, topRange._1)
    val nextUD = this.subBlock(topRange, botRange).
      shiftIndices(topRange._1, botRange._1)

    val nextTuple = new LUSequences(nextP, nextL, nextU, nextLi, nextUi,
                     firstZ ++ nextLD, firstZ ++ nextUD,
                     this.SchurComplement)

    // call to recursive build after initialization step
    val lastSequences = recursiveSequencesBuild(1, nextTuple)
    val rowsPerBlock = this.rowsPerBlock
    val colsPerBlock = this.colsPerBlock
    val dP = new BlockMatrix(lastSequences.P, rowsPerBlock, colsPerBlock)
    val dL = new BlockMatrix(lastSequences.L, rowsPerBlock, colsPerBlock)
    val dU = new BlockMatrix(lastSequences.U, rowsPerBlock, colsPerBlock)
    val dLi = new BlockMatrix(lastSequences.Li, rowsPerBlock, colsPerBlock)
    val dUi = new BlockMatrix(lastSequences.Ui, rowsPerBlock, colsPerBlock)
    val LD = new BlockMatrix(lastSequences.LD, rowsPerBlock, colsPerBlock)
    val UD = new BlockMatrix(lastSequences.UD, rowsPerBlock, colsPerBlock)

    // Large Matrix Multiplication Operations
    // dL and dU are the sets of L and U Matrices along the diagonal blocks,
    // respectively.  dLinv and dUinv are the sets of inverted U and L matrices along
    // the diagonal blocks, respectively.  dP is the set of P Matrices along the diagonal
    // blocks  (equivalent to full P).  Recall that permutation matrices are inverted
    // as P.transpose = Pinv.
    // UD is the preformed upper (off) diagonal matrix containing the Schur Complements
    // LD is the preformed lower (off) diagonal matrix containing the Schur Complements

    val PFin = dP
  // L       = ( dP * LD * dUinv  + d[Linv] )
    val LFin = (dP.multiply(LD.multiply(dUi))).add(dL)
  // U       = ( d[Linv] * dP * UD + dU )
    val UFin = dLi.multiply(dP.multiply(UD)).add(dU)
 // val UFin =  dLi.multiply(UD).add(dU)
    new PLUandInverses(PFin, LFin, UFin, dLi, dUi)
  }


/**
 * Returns the LU Decomposition of a Square Matrix.  For a matrix A of size (n x n)
 * LU decomposition computes the Lower Triangular Matrix L, the Upper Triangular
 * Matrix U, along with a Permutation Matrix P, such that PA=LU.  The Permutation
 * Matrix addresses cases where zero entries prevent forward substitution
 * solution of L or U.  The main method, blockLUtoSolver, returns more values that are used
 * by the solver, and this method returns only P, L, and U.
 *
 * @return PLU(P,L,U) as a Tuple of BlockMatrix
 * @since 1.6.0
 */
  def blockLU: PLU = {
      val solution = this.blockLUtoSolver
      new PLU(solution.P, solution.L, solution.U)
  }

/**
 * For the matrix Equation AX=B, where A is NxN blocks, and X, B are matrices of
 * dimension NxW blocks,  A.solve(B) returns X.  B can be a single column vector of length N
 * (W=1), or a matrix of W column vectors. In all cases, B must be a BlockMatrix with
 * the same block size and number of row blocks.  The width can vary according to the
 * number of columns in B.
 *
 * @return X as a BlockMatrix
 * @since 1.6.0
 */
  def solve(B: BlockMatrix): BlockMatrix = {
    val solution = this.blockLUtoSolver
    val P = solution.P
    val L = solution.L
    val U = solution.U
    val Li = solution.dLi
    val Ui = solution.dUi
    val pB = P.multiply(B)
    val N = this.numRowBlocks
    val W = B.numColBlocks
    val sc = this.blocks.sparkContext

    // last diagonal block is not always fully populated
    val numRowsLast = this.subBlock((N - 1, N - 1), (N - 1, N - 1)).numRows.toInt
    val numColsLast = this.subBlock((N - 1, N - 1), (N - 1, N - 1)).numCols.toInt
    // solving AX = b;
    // 1) solve LY = PB for y
    // 2) solve UX = Y  for x

    // Solving LY = PB for Y using (see docs):
    def recursiveYBuild(m: Int, prevY: BlockMatrix): BlockMatrix = {
      // note that only m and prevY are passed, while the remaining
      // variables are used from the scope of BlockMatrix.solve()

      // the diagonal block inv(L) is *mostly* computed in the blockLU routine
      // but the last diagonal block is not used for constructing the inverse.
      // It is computed here.
      val invL = {
        if (m == N - 1){
          new BlockMatrix(
            sc.parallelize(Seq(((0, 0), Matrices.dense(numRowsLast, numColsLast,
              inv(L.subBlock((m, m), (m, m)).toBreeze).toArray)))),
            this.rowsPerBlock, this.colsPerBlock)
        }
        else { Li.subBlock((m, m), (m, m)) }
      }

      val nextY = new BlockMatrix(
        invL.multiply( pB.subBlock((m, m), (0, W - 1)).subtract(
          L.subBlock((m, m), (0, m-1)).multiply(prevY))).shiftIndices(m, 0),
        this.rowsPerBlock, this.colsPerBlock)

      val currentY = new BlockMatrix(prevY.blocks ++ nextY.blocks,
        this.rowsPerBlock, this.colsPerBlock)

      if (m == N - 1){currentY}   // terminal case
      else {recursiveYBuild(m + 1, currentY)}     // recursive case
    }

    // Solving LY = PB for Y using (see docs):
    val firstY = Li.subBlock((0, 0), (0, 0)).
      multiply(pB.subBlock((0, 0), (0, W-1)))

    val Y = recursiveYBuild(1, firstY )

    // defining X recursion build for second solver
    def recursiveXBuild(mRev: Int, prevX: BlockMatrix): BlockMatrix = {
      // very similar to recursiveYBuild, but builds from the bottom up,
      // which requires extra bookkeepping.

      val nextX = new BlockMatrix(
        Ui.subBlock((mRev, mRev), (mRev, mRev)).multiply(
          Y.subBlock((mRev, mRev), (0, W - 1)).subtract(
            U.subBlock((mRev, mRev), (mRev + 1, N - 1 )).multiply(
              prevX.subBlock((mRev + 1, N - 1), (0, W - 1))
            ))).shiftIndices(mRev, 0),
        this.rowsPerBlock, this.colsPerBlock)

      val currentX = new BlockMatrix(prevX.blocks ++ nextX.blocks,
        this.rowsPerBlock, this.colsPerBlock)


      if (mRev == 0 ){currentX}   // terminal case
      else {recursiveXBuild(mRev - 1, currentX)}     // recursive case
    }

    // Solving UX = Y for X
    val mRev = N - 1

    // last diagonal block of inv(U) is not stored...and needs to be computed
    val invU = new BlockMatrix( sc.parallelize( Seq(((mRev, mRev),
      Matrices.dense( numRowsLast, numColsLast,
        inv(U.subBlock((mRev, mRev), (mRev, mRev)).toBreeze).toArray)))),
      this.rowsPerBlock, this.colsPerBlock)

    val firstX = new BlockMatrix(
      invU.subBlock((mRev, mRev), (mRev, mRev)).
        multiply(Y.subBlock((mRev, mRev), (0, W - 1))).
        shiftIndices(mRev, 0),
      this.rowsPerBlock, this.colsPerBlock)

    val X = recursiveXBuild(mRev-1, firstX)
    X
  }
}
