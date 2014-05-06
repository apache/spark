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

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors

/**
 * :: Experimental ::
 * Represents an entry in an distributed matrix.
 * @param i row index
 * @param j column index
 * @param value value of the entry
 */
@Experimental
case class MatrixEntry(i: Long, j: Long, value: Double)

/**
 * :: Experimental ::
 * Represents a matrix in coordinate format.
 *
 * @param entries matrix entries
 * @param nRows number of rows. A non-positive value means unknown, and then the number of rows will
 *              be determined by the max row index plus one.
 * @param nCols number of columns. A non-positive value means unknown, and then the number of
 *              columns will be determined by the max column index plus one.
 */
@Experimental
class CoordinateMatrix(
    val entries: RDD[MatrixEntry],
    private var nRows: Long,
    private var nCols: Long) extends DistributedMatrix {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(entries: RDD[MatrixEntry]) = this(entries, 0L, 0L)

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (nCols <= 0L) {
      computeSize()
    }
    nCols
  }

  /** Gets or computes the number of rows. */
  override def numRows(): Long = {
    if (nRows <= 0L) {
      computeSize()
    }
    nRows
  }

  /** Converts to IndexedRowMatrix. The number of columns must be within the integer range. */
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
  def toRowMatrix(): RowMatrix = {
    toIndexedRowMatrix().toRowMatrix()
  }

  /** Determines the size by computing the max row/column index. */
  private def computeSize() {
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
