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
import org.apache.spark.mllib.linalg.{Matrix, SparseMatrix, Vectors}
import org.apache.spark.rdd.RDD

/**
 * Represents an entry in a distributed matrix.
 * @param i row index
 * @param j column index
 * @param value value of the entry
 */
@Since("1.0.0")
case class MatrixEntry(i: Long, j: Long, value: Double)

/**
 * Represents a matrix in coordinate format.
 *
 * @param entries matrix entries
 * @param nRows number of rows. A non-positive value means unknown, and then the number of rows will
 *              be determined by the max row index plus one.
 * @param nCols number of columns. A non-positive value means unknown, and then the number of
 *              columns will be determined by the max column index plus one.
 */
@Since("1.0.0")
class CoordinateMatrix @Since("1.0.0") (
    @Since("1.0.0") val entries: RDD[MatrixEntry],
    private var nRows: Long,
    private var nCols: Long) extends DistributedMatrix {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  @Since("1.0.0")
  def this(entries: RDD[MatrixEntry]) = this(entries, 0L, 0L)

  /** Gets or computes the number of columns. */
  @Since("1.0.0")
  override def numCols(): Long = {
    if (nCols <= 0L) {
      computeSize()
    }
    nCols
  }

  /** Gets or computes the number of rows. */
  @Since("1.0.0")
  override def numRows(): Long = {
    if (nRows <= 0L) {
      computeSize()
    }
    nRows
  }

  /** Transposes this CoordinateMatrix. */
  @Since("1.3.0")
  def transpose(): CoordinateMatrix = {
    new CoordinateMatrix(entries.map(x => MatrixEntry(x.j, x.i, x.value)), numCols(), numRows())
  }

  /** Converts to IndexedRowMatrix. The number of columns must be within the integer range. */
  @Since("1.0.0")
  def toIndexedRowMatrix(): IndexedRowMatrix = {
    val nl = numCols()
    if (nl > Int.MaxValue) {
      sys.error(s"Cannot convert to a row-oriented format because the number of columns $nl is " +
        "too large.")
    }
    val n = nl.toInt
    val indexedRows = entries.map(entry => (entry.i, (entry.j.toInt, entry.value)))
      .groupByKey()
      .map { case (i, vectorEntries) =>
        IndexedRow(i, Vectors.sparse(n, vectorEntries.toSeq))
      }
    new IndexedRowMatrix(indexedRows, numRows(), n)
  }

  /**
   * Converts to RowMatrix, dropping row indices after grouping by row index.
   * The number of columns must be within the integer range.
   */
  @Since("1.0.0")
  def toRowMatrix(): RowMatrix = {
    toIndexedRowMatrix().toRowMatrix()
  }

  /**
   * Converts to BlockMatrix. Creates blocks of `SparseMatrix` with size 1024 x 1024.
   */
  @Since("1.3.0")
  def toBlockMatrix(): BlockMatrix = {
    toBlockMatrix(1024, 1024)
  }

  /**
   * Converts to BlockMatrix. Creates blocks of `SparseMatrix`.
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

    // Since block matrices require an integer row and col index
    require(math.ceil(m.toDouble / rowsPerBlock) <= Int.MaxValue,
      "Number of rows divided by rowsPerBlock cannot exceed maximum integer.")
    require(math.ceil(n.toDouble / colsPerBlock) <= Int.MaxValue,
      "Number of cols divided by colsPerBlock cannot exceed maximum integer.")

    val numRowBlocks = math.ceil(m.toDouble / rowsPerBlock).toInt
    val numColBlocks = math.ceil(n.toDouble / colsPerBlock).toInt
    val partitioner = GridPartitioner(numRowBlocks, numColBlocks, entries.partitions.length)

    val blocks: RDD[((Int, Int), Matrix)] = entries.map { entry =>
      val blockRowIndex = (entry.i / rowsPerBlock).toInt
      val blockColIndex = (entry.j / colsPerBlock).toInt

      val rowId = entry.i % rowsPerBlock
      val colId = entry.j % colsPerBlock

      ((blockRowIndex, blockColIndex), (rowId.toInt, colId.toInt, entry.value))
    }.groupByKey(partitioner).map { case ((blockRowIndex, blockColIndex), entry) =>
      val effRows = math.min(m - blockRowIndex.toLong * rowsPerBlock, rowsPerBlock).toInt
      val effCols = math.min(n - blockColIndex.toLong * colsPerBlock, colsPerBlock).toInt
      ((blockRowIndex, blockColIndex), SparseMatrix.fromCOO(effRows, effCols, entry))
    }
    new BlockMatrix(blocks, rowsPerBlock, colsPerBlock, m, n)
  }

  /** Determines the size by computing the max row/column index. */
  private def computeSize(): Unit = {
    // Reduce will throw an exception if `entries` is empty.
    val (m1, n1) = entries.map(entry => (entry.i, entry.j)).reduce { case ((i1, j1), (i2, j2)) =>
      (math.max(i1, i2), math.max(j1, j2))
    }
    // There may be empty columns at the very right and empty rows at the very bottom.
    nRows = math.max(nRows, m1 + 1L)
    nCols = math.max(nCols, n1 + 1L)
  }

  /** Collects data and assembles a local matrix. */
  private[mllib] override def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    entries.collect().foreach { case MatrixEntry(i, j, value) =>
      mat(i.toInt, j.toInt) = value
    }
    mat
  }
}
