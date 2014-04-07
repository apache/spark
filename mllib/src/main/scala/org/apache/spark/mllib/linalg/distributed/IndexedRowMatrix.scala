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

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

/** Represents a row of RowRDDMatrix. */
case class IndexedMatrixRow(index: Long, vector: Vector)

/**
 * Represents a row-oriented RDDMatrix with indexed rows.
 *
 * @param rows indexed rows of this matrix
 * @param nRows number of rows. A non-positive value means unknown, and then the number of rows will
 *              be determined by the max row index plus one.
 * @param nCols number of columns. A non-positive value means unknown, and then the number of
 *              columns will be determined by the size of the first row.
 */
class IndexedRowMatrix(
    val rows: RDD[IndexedMatrixRow],
    private var nRows: Long,
    private var nCols: Int) extends DistributedMatrix {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(rows: RDD[IndexedMatrixRow]) = this(rows, 0L, 0)

  /** Gets or computes the number of columns. */
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

  /** Drops row indices and converts this matrix to a RowRDDMatrix. */
  def toRowMatrix(): RowMatrix = {
    new RowMatrix(rows.map(_.vector), 0L, nCols)
  }
}
