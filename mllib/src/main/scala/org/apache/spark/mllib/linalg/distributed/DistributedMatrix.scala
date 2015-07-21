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
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Represents a distributively stored matrix backed by one or more RDDs.
 */
trait DistributedMatrix extends Serializable {

  /** Gets or computes the number of rows. */
  def numRows(): Long

  /** Gets or computes the number of columns. */
  def numCols(): Long

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  private[mllib] def toBreeze(): BDM[Double]
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.distributed.DistributedMatrix]].
 */
object DistributedMatrices {

  /**
   * Creates a Row Matrix.
   *
   * @param rows A RDD[Vector]
   * @param numRows Number of rows in the matrix
   * @param numCols Number of columns in the matrix
   */
  def rowMatrix(rows: RDD[Vector], numRows: Long = 0, numCols: Int = 0): RowMatrix = {
    new RowMatrix(rows, numRows, numCols)
  }

  /**
   * Creates an IndexedRowMatrix.
   *
   * @param rows A RDD[IndexedRow]
   * @param numRows Number of rows in the matrix
   * @param numCols Number of columns in the matrix
   */
  def indexedRowMatrix(
      rows: RDD[IndexedRow],
      numRows: Long = 0,
      numCols: Int = 0): IndexedRowMatrix = {
    new IndexedRowMatrix(rows, numRows, numCols)
  }

  /**
   * Creates a CoordinateMatrix.
   *
   * @param rows A RDD[MatrixEntry]
   * @param numRows Number of rows in the matrix
   * @param numCols Number of columns in the matrix
   */
  def coordinateMatrix(
      rows: RDD[MatrixEntry],
      numRows: Long = 0,
      numCols: Long = 0): CoordinateMatrix = {
    new CoordinateMatrix(rows, numRows, numCols)
  }
}
