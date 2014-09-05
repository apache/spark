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

package org.apache.spark.mllib.linalg

import breeze.linalg.{Matrix => BM, DenseMatrix => BDM, CSCMatrix => BSM}

/**
 * Trait for a local matrix.
 */
trait Matrix extends Serializable {

  /** Number of rows. */
  def numRows: Int

  /** Number of columns. */
  def numCols: Int

  /** Converts to a dense array in column major. */
  def toArray: Array[Double]

  /** Converts to a breeze matrix. */
  private[mllib] def toBreeze: BM[Double]

  /** Gets the (r, c)-th element. */
  private[mllib] def apply(i: Int): Double
  private[mllib] def apply(r: Int, c: Int): Double

  // Update element at (r, c)
  private[mllib] def update(r: Int, c: Int, v: Double)

  def copy: Matrix // Deep Copy

  // Matrix multiply operations for convenience
  def times(y: DenseMatrix): DenseMatrix = BLAS.gemm(1.0, this, y)
  def times(y: DenseVector): DenseVector = BLAS.gemv(1.0, this, y)
  def transpose_times(y: DenseMatrix): DenseMatrix = BLAS.gemm("T", "N", 1.0, this, y)
  def transpose_times(y: DenseVector): DenseVector = BLAS.gemv("T", 1.0, this, y)

  override def toString: String = toBreeze.toString()
}

/**
 * Column-majored dense matrix.
 * The entry values are stored in a single array of doubles with columns listed in sequence.
 * For example, the following matrix
 * {{{
 *   1.0 2.0
 *   3.0 4.0
 *   5.0 6.0
 * }}}
 * is stored as `[1.0, 3.0, 5.0, 2.0, 4.0, 6.0]`.
 *
 * @param numRows number of rows
 * @param numCols number of columns
 * @param values matrix entries in column major
 */
class DenseMatrix(val numRows: Int, val numCols: Int, val values: Array[Double]) extends Matrix {

  require(values.length == numRows * numCols)

  override def toArray: Array[Double] = values

  private[mllib] override def toBreeze: BM[Double] = new BDM[Double](numRows, numCols, values)

  private[mllib] override def apply(i: Int): Double = values(i)
  private[mllib] override def apply(r: Int, c: Int): Double = values(index(r,c))

  private[mllib] def index(r: Int, c: Int): Int = r + numRows * c

  private[mllib] override def update(r: Int, c: Int, v: Double){
    values(index(r, c)) = v
  }

  def copy = new DenseMatrix(numRows, numCols, values.clone())
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.DenseMatrix]].
 *
 * These methods can be used to generate common matrix types such as the Identity matrix, any
 * diagonal matrix, zero matrix, etc. Also provides methods to transform an `RDD[Vector]` into
 * `RDD[DenseMatrix]`, which speeds up gradient updates for multi-model training.
 */
object DenseMatrix {

  def zeros(rows: Int, cols: Int) = new DenseMatrix(rows, cols, Array.fill(rows*cols)(0.0))

  def ones(rows: Int, cols: Int) = new DenseMatrix(rows, cols, Array.fill(rows*cols)(1.0))

  def eye(n: Int) = {
    val identity = DenseMatrix.zeros(n,n)
    for (i <- 0 until n){
      identity.update(i,i,1.0)
    }
    identity
  }

  def rand(rows: Int, cols: Int) = {
    val rand = new scala.util.Random
    new DenseMatrix(rows,cols, Array.fill(rows*cols)(rand.nextDouble()))
  }

  def randn(rows: Int, cols: Int) = {
    val rand = new scala.util.Random
    new DenseMatrix(rows,cols, Array.fill(rows*cols)(rand.nextGaussian()))
  }

  def diag(values: Array[Double]) = {
    val n = values.length
    val matrix = DenseMatrix.eye(n)
    for (i <- 0 until n) matrix.update(i,i,values(i))
    matrix
  }
}

/**
 * Column-majored sparse matrix.
 * The entry values are stored in Compressed-Column Storage (CCS) format.
 * For example, the following matrix
 * {{{
 *   1.0 0.0 4.0
 *   0.0 3.0 5.0
 *   2.0 0.0 6.0
 * }}}
 * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
 * `rowIndices=[0, 2, 1, 0, 1, 2]`, `colIndices=[0, 2, 3, 6]`.
 *
 * @param numRows number of rows
 * @param numCols number of columns
 * @param colIndices the index corresponding to the start of a new column
 * @param rowIndices the row index of the entry
 * @param values non-zero matrix entries in column major
 */
class SparseMatrix(val numRows: Int,
                   val numCols: Int,
                   val colIndices: Array[Int],
                   val rowIndices: Array[Int],
                   val values: Array[Double]) extends Matrix {

  require(values.length == rowIndices.length, "The number of row indices and values don't match!")
  require(colIndices.length == numCols + 1, "The length of the column indices should be the " +
    s"number of columns + 1. Currently, colIndices.length: ${colIndices.length}, numCols: $numCols")

  override def toArray: Array[Double] = values

  private[mllib] override def toBreeze: BM[Double] =
    new BSM[Double](values, numRows, numCols, colIndices, rowIndices)

  private[mllib] override def apply(i: Int): Double = values(i)
  private[mllib] override def apply(r: Int, c: Int): Double = {
    val ind = index(r,c)
    if (ind == -1) 0.0 else values(ind)
  }

  private[mllib] def index(r: Int, c: Int): Int = {
    val regionStart = colIndices(c)
    val regionEnd = colIndices(c+1)
    val region = rowIndices.slice(regionStart, regionEnd)
    if (region.contains(r)){
      region.indexOf(r) + regionStart
    } else {
      -1 // return zero element
    }
  }

  // TODO(Burak): Maybe convert to Breeze to update zero entries? I can't think of any MLlib
  // TODO: algorithm that would use mutable Sparse Matrices
  private[mllib] override def update(r: Int, c: Int, v: Double){
    val ind = index(r,c)
    if (ind == -1){
      throw new IllegalArgumentException("The given row and column indices correspond to a zero " +
        "value. Sparse Matrices are currently immutable.")
    } else {
      values(index(r, c)) = v
    }
  }

  def copy = new SparseMatrix(numRows, numCols, colIndices, rowIndices, values.clone())
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.Matrix]].
 */
object Matrices {

  /**
   * Creates a column-majored dense matrix.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param values matrix entries in column major
   */
  def dense(numRows: Int, numCols: Int, values: Array[Double]): Matrix = {
    new DenseMatrix(numRows, numCols, values)
  }

  /**
   * Creates a column-majored sparse matrix in Compressed Column Storage (CCS) format.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param colIndices the index corresponding to the start of a new column
   * @param rowIndices the row index of the entry
   * @param values non-zero matrix entries in column major
   */
  def sparse(
     numRows: Int,
     numCols: Int,
     colIndices: Array[Int],
     rowIndices: Array[Int],
     values: Array[Double]): Matrix = {
    new SparseMatrix(numRows, numCols, colIndices, rowIndices, values)
  }

  /**
   * Creates a Matrix instance from a breeze matrix.
   * @param breeze a breeze matrix
   * @return a Matrix instance
   */
  private[mllib] def fromBreeze(breeze: BM[Double]): Matrix = {
    breeze match {
      case dm: BDM[Double] =>
        require(dm.majorStride == dm.rows,
          "Do not support stride size different from the number of rows.")
        new DenseMatrix(dm.rows, dm.cols, dm.data)
      case sm: BSM[Double] =>
        new SparseMatrix(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
      case _ =>
        throw new UnsupportedOperationException(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }
}
