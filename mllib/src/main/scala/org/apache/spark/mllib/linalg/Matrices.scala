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

import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer
import java.util.Arrays

/**
 * Trait for a local matrix.
 */
sealed trait Matrix extends Serializable {

  /** Number of rows. */
  def numRows: Int

  /** Number of columns. */
  def numCols: Int

  /** Converts to a dense array in column major. */
  def toArray: Array[Double]

  /** Converts to a breeze matrix. */
  private[mllib] def toBreeze: BM[Double]

  /** Gets the (i, j)-th element. */
  private[mllib] def apply(i: Int, j: Int): Double

  /** Return the index for the (i, j)-th element in the backing array. */
  private[mllib] def index(i: Int, j: Int): Int

  /** Update element at (i, j) */
  private[mllib] def update(i: Int, j: Int, v: Double): Unit

  /** Get a deep copy of the matrix. */
  def copy: Matrix

  /** Convenience method for `Matrix`-`Matrix` multiplication.
    * Note: `SparseMatrix`-`SparseMatrix` multiplication is not supported */
  def multiply(y: Matrix): DenseMatrix = {
    val C: DenseMatrix = DenseMatrix.zeros(numRows, y.numCols)
    BLAS.gemm(false, false, 1.0, this, y, 0.0, C)
    C
  }

  /** Convenience method for `Matrix`-`DenseVector` multiplication. */
  def multiply(y: DenseVector): DenseVector = {
    val output = new DenseVector(new Array[Double](numRows))
    BLAS.gemv(1.0, this, y, 0.0, output)
    output
  }

  /** Convenience method for `Matrix`^T^-`Matrix` multiplication.
    * Note: `SparseMatrix`-`SparseMatrix` multiplication is not supported */
  def transposeMultiply(y: Matrix): DenseMatrix = {
    val C: DenseMatrix = DenseMatrix.zeros(numCols, y.numCols)
    BLAS.gemm(true, false, 1.0, this, y, 0.0, C)
    C
  }

  /** Convenience method for `Matrix`^T^-`DenseVector` multiplication. */
  def transposeMultiply(y: DenseVector): DenseVector = {
    val output = new DenseVector(new Array[Double](numCols))
    BLAS.gemv(true, 1.0, this, y, 0.0, output)
    output
  }

  /** A human readable representation of the matrix */
  override def toString: String = toBreeze.toString()

  private[mllib] def map(f: Double => Double): Matrix

  private[mllib] def update(f: Double => Double): Matrix

  private[mllib] def elementWiseOperateOnColumnsInPlace(f: (Double, Double) => Double,
                                                        y: Matrix): Matrix

  private[mllib] def elementWiseOperateOnRowsInPlace(f: (Double, Double) => Double,
                                                     y: Matrix): Matrix

  private[mllib] def elementWiseOperateInPlace(f: (Double, Double) => Double, y: Matrix): Matrix

  private[mllib] def elementWiseOperateScalarInPlace(f: (Double, Double) => Double,
                                                     y: Double): Matrix

  private[mllib] def operateInPlace(f: (Double, Double) => Double, y: Matrix): Matrix

  private[mllib] def elementWiseOperateOnColumns(f: (Double, Double) => Double, y: Matrix): Matrix

  private[mllib] def elementWiseOperateOnRows(f: (Double, Double) => Double, y: Matrix): Matrix

  private[mllib] def elementWiseOperate(f: (Double, Double) => Double, y: Matrix): Matrix

  private[mllib] def elementWiseOperateScalar(f: (Double, Double) => Double, y: Double): Matrix

  private[mllib] def operate(f: (Double, Double) => Double, y: Matrix): Matrix

  private[mllib] def *=(y: Matrix) = operateInPlace(_ * _, y)

  private[mllib] def *(y: Matrix) = operate(_ * _, y)

  private[mllib] def +=(y: Matrix) = operateInPlace(_ + _, y)

  private[mllib] def +(y: Matrix) = operate(_ + _, y)

  private[mllib] def -=(y: Matrix) = operateInPlace(_ - _, y)

  private[mllib] def -(y: Matrix) = operate(_ - _, y)

  private[mllib] def /=(y: Matrix) = operateInPlace(_ / _, y)

  private[mllib] def /(y: Matrix) = operate(_ / _, y)

  private[mllib] def *=(y: Double) = elementWiseOperateScalarInPlace(_ * _, y)

  private[mllib] def +=(y: Double) = elementWiseOperateScalarInPlace(_ + _, y)

  private[mllib] def -=(y: Double) = elementWiseOperateScalarInPlace(_ - _, y)

  private[mllib] def /=(y: Double) = elementWiseOperateScalarInPlace(_ / _, y)

  private[mllib] def *(y: Double) = elementWiseOperateScalar(_ * _, y)

  private[mllib] def +(y: Double) = elementWiseOperateScalar(_ + _, y)

  private[mllib] def -(y: Double) = elementWiseOperateScalar(_ - _, y)

  private[mllib] def /(y: Double) = elementWiseOperateScalar(_ / _, y)

  private[mllib] def neg: Matrix

  private[mllib] def negInPlace: Matrix

  /** Less-than-or-equal-to check. Outputs binary `DenseMatrix` */
  private[mllib] def compare(v: Double, f: (Double, Double) => Boolean): DenseMatrix

  /** Returns the p-th norm for each column */
  private[mllib] def colNorms(p: Double): Matrix

  private[mllib] def colSums: DenseMatrix = colSums(false)

  private[mllib] def colSums(absolute: Boolean, skipRows: DenseMatrix = null): DenseMatrix = {
    val sums = new DenseMatrix(1, numCols, Array.fill(numCols)(0.0))
    var j = 0
    this match {
      case sparse: SparseMatrix =>
        while (j < numCols){
          var i = sparse.colPtrs(j)
          val indEnd = sparse.colPtrs(j + 1)
          while (i < indEnd){
            var v = sparse.values(i)
            if (absolute) v = math.abs(v)
            sums.values(j) += v
            i += 1
          }
          j += 1
        }
      case dense: DenseMatrix =>
        while (j < numCols){
          var i = 0
          while (i < numRows){
            if (skipRows == null) {
              var v = dense.values(index(i, j))
              if (absolute) v = math.abs(v)
              sums.values(j) += v
            } else {
              if (skipRows(i) != 1.0) {
                var v = dense.values(index(i, j))
                if (absolute) v = math.abs(v)
                sums.values(j) += v
              }
            }

            i += 1
          }
          j += 1
        }
    }
    sums
  }

  private[mllib] def rowSums: DenseMatrix = rowSums(false)

  private[mllib] def rowSums(absolute: Boolean): DenseMatrix = {
    val sums = new DenseMatrix(numRows, 1, Array.fill(numRows)(0.0))
    var j = 0
    this match {
      case sparse: SparseMatrix =>
        while (j < numCols){
          var i = sparse.colPtrs(j)
          val indEnd = sparse.colPtrs(j + 1)
          while (i < indEnd){
            var v = sparse.values(i)
            if (absolute) v = math.abs(v)
            sums.values(sparse.rowIndices(i)) += v
            i += 1
          }
          j += 1
        }
      case dense: DenseMatrix =>
        while (j < numCols){
          var i = 0
          while (i < numRows){
            var v = dense.values(index(i, j))
            if (absolute) v = math.abs(v)
            sums.values(i) += v
            i += 1
          }
          j += 1
        }
    }
    sums
  }
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
class DenseMatrix(val numRows: Int, val numCols: Int, val values: Array[Double]) extends Matrix with Serializable {

  require(values.length == numRows * numCols, "The number of values supplied doesn't match the " +
    s"size of the matrix! values.length: ${values.length}, numRows * numCols: ${numRows * numCols}")

  override def toArray: Array[Double] = values

  private[mllib] def toBreeze: BM[Double] = new BDM[Double](numRows, numCols, values)

  private[mllib] def apply(i: Int): Double = values(i)

  private[mllib] def apply(i: Int, j: Int): Double = values(index(i, j))

  private[mllib] def index(i: Int, j: Int): Int = i + numRows * j

  private[mllib] def update(i: Int, j: Int, v: Double): Unit = {
    values(index(i, j)) = v
  }

  override def copy = new DenseMatrix(numRows, numCols, values.clone())

  private[mllib] def elementWiseOperateOnColumnsInPlace(
      f: (Double, Double) => Double,
      y: Matrix): DenseMatrix = {
    val y_vals = y.toArray
    val len = y_vals.length
    require(y_vals.length == numRows)
    var j = 0
    while (j < numCols){
      var i = 0
      while (i < len){
        val idx = index(i, j)
        values(idx) = f(values(idx), y_vals(i))
        i += 1
      }
      j += 1
    }
    this
  }

  private[mllib] def elementWiseOperateOnRowsInPlace(
     f: (Double, Double) => Double,
     y: Matrix): DenseMatrix = {
    val y_vals = y.toArray
    require(y_vals.length == numCols)
    var j = 0
    while (j < numCols){
      var i = 0
      while (i < numRows){
        val idx = index(i, j)
        values(idx) = f(values(idx), y_vals(j))
        i += 1
      }
      j += 1
    }
    this
  }

  private[mllib] def elementWiseOperateInPlace(f: (Double, Double) => Double, y: Matrix): DenseMatrix =  {
    val y_val = y.toArray
    val len = values.length
    require(y_val.length == values.length)
    var j = 0
    while (j < len){
      values(j) = f(values(j), y_val(j))
      j += 1
    }
    this
  }

  private[mllib] def elementWiseOperateScalarInPlace(f: (Double, Double) => Double, y: Double): DenseMatrix =  {
    var j = 0
    val len = values.length
    while (j < len){
      values(j) = f(values(j), y)
      j += 1
    }
    this
  }

  private[mllib] def operateInPlace(f: (Double, Double) => Double, y: Matrix): DenseMatrix = {
    if (y.numCols==1 || y.numRows == 1){
      require(numCols != numRows, "Operation is ambiguous. Please use elementWiseOperateOnRows " +
        "or elementWiseOperateOnColumns instead")
    }
    if (y.numCols == 1 && y.numRows == 1){
      elementWiseOperateScalarInPlace(f, y.toArray(0))
    } else {
      if (y.numCols==1) {
        elementWiseOperateOnColumnsInPlace(f, y)
      }else if (y.numRows==1){
        elementWiseOperateOnRowsInPlace(f, y)
      }else{
        elementWiseOperateInPlace(f, y)
      }
    }
  }

  private[mllib] def elementWiseOperateOnColumns(f: (Double, Double) => Double, y: Matrix): DenseMatrix = {
    val dup = this.copy
    dup.elementWiseOperateOnColumnsInPlace(f, y)
  }

  private[mllib] def elementWiseOperateOnRows(f: (Double, Double) => Double, y: Matrix): DenseMatrix = {
    val dup = this.copy
    dup.elementWiseOperateOnRowsInPlace(f, y)
  }

  private[mllib] def elementWiseOperate(f: (Double, Double) => Double, y: Matrix): DenseMatrix =  {
    val dup = this.copy
    dup.elementWiseOperateInPlace(f, y)
  }

  private[mllib] def elementWiseOperateScalar(f: (Double, Double) => Double, y: Double): DenseMatrix =  {
    val dup = this.copy
    dup.elementWiseOperateScalarInPlace(f, y)
  }

  private[mllib] def operate(f: (Double, Double) => Double, y: Matrix): DenseMatrix = {
    val dup = this.copy
    dup.operateInPlace(f, y)
  }

  def map(f: Double => Double) = new DenseMatrix(numRows, numCols, values.map(f))

  def update(f: Double => Double): DenseMatrix = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = f(values(i))
      i += 1
    }
    this
  }

  def colNorms(p: Double): DenseMatrix = {
    if (p==1.0) return colSums(true)
    val sums = new DenseMatrix(1, numCols, Array.fill(numCols)(0.0))
    var j = 0
    while (j < numCols){
      var i = 0
      while (i < numRows){
        val idx = index(i, j)
        sums.update(0,j, sums(j) + math.pow(values(idx),p))
        i += 1
      }
      j += 1
    }
    j = 0
    while (j < numCols){
      sums.update(0, j, math.pow(sums(j), 1/p))
      j += 1
    }
    sums
  }

  private[mllib] def negInPlace: DenseMatrix = {
    var j = 0
    val len = values.length
    while (j < len){
      values(j) *= -1
      j += 1
    }
    this
  }

  private[mllib] def neg: DenseMatrix = {
    val copy = new DenseMatrix(numRows, numCols, values.clone())
    copy.negInPlace
  }

  private[mllib] def compareInPlace(v: Double, f: (Double, Double) => Boolean): DenseMatrix = {
    var j = 0
    val len = values.length
    while (j < len){
      values(j) = if (f(values(j), v)) 1.0 else 0.0
      j += 1
    }
    this
  }

  private[mllib] def compare(v: Double, f: (Double, Double) => Boolean): DenseMatrix = {
    val copy = new DenseMatrix(numRows, numCols, values.clone())
    copy.compareInPlace(v, f)
  }

  private[mllib] def multiplyInPlace(y: Matrix): DenseMatrix = {
    val copy = this multiply y
    BLAS.copy(Vectors.dense(copy.values), Vectors.dense(values))
    this
  }
}

object DenseMatrix {

  /**
   * Generate a `DenseMatrix` consisting of zeros.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of zeros
   */
  def zeros(numRows: Int, numCols: Int): DenseMatrix =
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(0.0))

  /**
   * Generate a `DenseMatrix` consisting of ones.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of ones
   */
  def ones(numRows: Int, numCols: Int): DenseMatrix =
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(1.0))

  /**
   * Generate an Identity Matrix in `DenseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `DenseMatrix` with size `n` x `n` and values of ones on the diagonal
   */
  def eye(n: Int): DenseMatrix = {
    val identity = DenseMatrix.zeros(n, n)
    var i = 0
    while (i < n){
      identity.update(i, i, 1.0)
      i += 1
    }
    identity
  }

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def rand(numRows: Int, numCols: Int): DenseMatrix = {
    val rand = new XORShiftRandom
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rand.nextDouble()))
  }

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def randn(numRows: Int, numCols: Int): DenseMatrix = {
    val rand = new XORShiftRandom
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rand.nextGaussian()))
  }

  /**
   * Generate a diagonal matrix in `DenseMatrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `DenseMatrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
   */
  def diag(vector: Vector): DenseMatrix = {
    val n = vector.size
    val matrix = DenseMatrix.eye(n)
    val values = vector.toArray
    var i = 0
    while (i < n) {
      matrix.update(i, i, values(i))
      i += 1
    }
    matrix
  }
}

/**
 * Column-majored sparse matrix.
 * The entry values are stored in Compressed Sparse Column (CSC) format.
 * For example, the following matrix
 * {{{
 *   1.0 0.0 4.0
 *   0.0 3.0 5.0
 *   2.0 0.0 6.0
 * }}}
 * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
 * `rowIndices=[0, 2, 1, 0, 1, 2]`, `colPointers=[0, 2, 3, 6]`.
 *
 * @param numRows number of rows
 * @param numCols number of columns
 * @param colPtrs the index corresponding to the start of a new column
 * @param rowIndices the row index of the entry. They must be in strictly increasing order for each
 *                   column
 * @param values non-zero matrix entries in column major
 */
class SparseMatrix(
    val numRows: Int,
    val numCols: Int,
    val colPtrs: Array[Int],
    val rowIndices: Array[Int],
    val values: Array[Double]) extends Matrix with Serializable {

  require(values.length == rowIndices.length, "The number of row indices and values don't match! " +
    s"values.length: ${values.length}, rowIndices.length: ${rowIndices.length}")
  require(colPtrs.length == numCols + 1, "The length of the column indices should be the " +
    s"number of columns + 1. Currently, colPointers.length: ${colPtrs.length}, " +
    s"numCols: $numCols")

  override def toArray: Array[Double] = {
    val arr = new Array[Double](numRows * numCols)
    var j = 0
    while (j < numCols) {
      var i = colPtrs(j)
      val indEnd = colPtrs(j + 1)
      val offset = j * numRows
      while (i < indEnd) {
        val rowIndex = rowIndices(i)
        arr(offset + rowIndex) = values(i)
        i += 1
      }
      j += 1
    }
    arr
  }

  private[mllib] def toBreeze: BM[Double] =
    new BSM[Double](values, numRows, numCols, colPtrs, rowIndices)

  private[mllib] def apply(i: Int, j: Int): Double = {
    val ind = index(i, j)
    if (ind < 0) 0.0 else values(ind)
  }

  private[mllib] def index(i: Int, j: Int): Int = {
    Arrays.binarySearch(rowIndices, colPtrs(j), colPtrs(j + 1), i)
  }

  private[mllib] def update(i: Int, j: Int, v: Double): Unit = {
    val ind = index(i, j)
    if (ind == -1){
      throw new NoSuchElementException("The given row and column indices correspond to a zero " +
        "value. Only non-zero elements in Sparse Matrices can be updated.")
    } else {
      values(index(i, j)) = v
    }
  }

  override def copy = new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.clone())

  private[mllib] def elementWiseOperateOnColumnsInPlace(f: (Double, Double) => Double, y: Matrix): Matrix = {
    if (isMultiplication(f) || isDivision(f)) {
      val y_vals = y.toArray
      require(y_vals.length == numRows)
      var j = 0
      while (j < numCols){
        var i = colPtrs(j)
        val indEnd = colPtrs(j + 1)
        while (i < indEnd){
          values(i) = f(values(i), y_vals(rowIndices(i)))
          i += 1
        }
        j += 1
      }
      this
    } else {
      val dup = this.toDense
      dup.elementWiseOperateOnColumnsInPlace(f, y)
    }
  }

  private[mllib] def elementWiseOperateOnRowsInPlace(
      f: (Double, Double) => Double,
      y: Matrix): Matrix = {
    if (isMultiplication(f) || isDivision(f)) {
      val y_vals = y.toArray
      require(y_vals.length == numCols)
      var j = 0
      while (j < numCols){
        var i = colPtrs(j)
        val indEnd = colPtrs(j + 1)
        while (i < indEnd){
          values(i) = f(values(i), y_vals(j))
          i += 1
        }
        j += 1
      }
      this
    } else {
      val dup = this.toDense
      dup.elementWiseOperateOnRowsInPlace(f, y)
    }
  }

  private[mllib] def elementWiseOperateInPlace(
      f: (Double, Double) => Double,
      y: Matrix): Matrix =  {
    require(y.numCols == numCols)
    require(y.numRows == numRows)
    if (isMultiplication(f) || isDivision(f)) {
      var j = 0
      while (j < numCols){
        var i = colPtrs(j)
        val indEnd = colPtrs(j + 1)
        while (i < indEnd) {
          values(i) = f(values(i), y(rowIndices(i), j))
          i += 1
        }
        j += 1
      }
      this
    } else {
      val dup = this.toDense
      dup.elementWiseOperateInPlace(f, y)
    }
  }

  private[mllib] def elementWiseOperateScalarInPlace(
      f: (Double, Double) => Double,
      y: Double): Matrix =  {
    if (isMultiplication(f) || isDivision(f)) {
      var j = 0
      val len = values.length
      while (j < len){
        values(j) = f(values(j), y)
        j += 1
      }
      this
    } else {
      val dup = this.toDense
      dup.elementWiseOperateScalarInPlace(f, y)
    }
  }

  private def isMultiplication(f: (Double, Double) => Double): Boolean = {
    if (f(2, 9) != 18) return false
    if (f(3, 7) != 21) return false
    if (f(8, 9) != 72) return false
    true
  }

  private def isDivision(f: (Double, Double) => Double): Boolean = {
    if (f(12, 3) != 4) return false
    if (f(72, 4) != 18) return false
    if (f(72, 9) != 8) return false
    true
  }

  private[mllib] def operateInPlace(f: (Double, Double) => Double, y: Matrix): Matrix = {
    if (y.numCols==1 || y.numRows == 1) {
      require(numCols != numRows, "Operation is ambiguous. Please use elementWiseMultiplyRows " +
        "or elementWiseMultiplyColumns instead")
    }
    if (y.numCols == 1 && y.numRows == 1) {
      elementWiseOperateScalarInPlace(f, y.toArray(0))
    } else {
      if (y.numCols == 1) {
        elementWiseOperateOnColumnsInPlace(f, y)
      }else if (y.numRows == 1){
        elementWiseOperateOnRowsInPlace(f, y)
      }else{
        elementWiseOperateInPlace(f, y)
      }
    }
  }

  private[mllib] def elementWiseOperateOnColumns(
      f: (Double, Double) => Double,
      y: Matrix): Matrix = {
    val dup = y match {
      case sy: SparseMatrix => this.copy
      case dy: DenseMatrix => this.toDense
    }
    dup.elementWiseOperateOnColumnsInPlace(f, y)
  }
  private[mllib] def elementWiseOperateOnRows(
      f: (Double, Double) => Double,
      y: Matrix): Matrix = {
    val dup = y match {
      case sy: SparseMatrix => this.copy
      case dy: DenseMatrix => this.toDense
    }
    dup.elementWiseOperateOnRowsInPlace(f, y)
  }
  private[mllib] def elementWiseOperate(f: (Double, Double) => Double, y: Matrix): Matrix =  {
    val dup = y match {
      case sy: SparseMatrix => this.copy
      case dy: DenseMatrix => this.toDense
    }
    dup.elementWiseOperateInPlace(f, y)
  }
  private[mllib] def elementWiseOperateScalar(f: (Double, Double) => Double, y: Double): Matrix =  {
    if (isMultiplication(f) || isDivision(f)) {
      val dup = this.copy
      dup.elementWiseOperateScalarInPlace(f, y)
    } else {
      val dup = this.toDense
      dup.elementWiseOperateScalarInPlace(f, y)
    }
  }

  private[mllib] def operate(f: (Double, Double) => Double, y: Matrix): Matrix = {
    val dup = y match {
      case sy: SparseMatrix => this.copy
      case dy: DenseMatrix => this.toDense
    }
    dup.operateInPlace(f, y)
  }

  def map(f: Double => Double) =
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.map(f))

  def update(f: Double => Double): SparseMatrix = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = f(values(i))
      i += 1
    }
    this
  }

  def colNorms(p: Double): DenseMatrix = {
    if (p==1.0) return colSums(true)
    val sums = new DenseMatrix(1, numCols, Array.fill(numCols)(0.0))
    var j = 0
    while (j < numCols){
      var i = colPtrs(j)
      val indEnd = colPtrs(j + 1)
      while (i < indEnd){
        sums.values(j) += math.pow(values(i),p)
        i += 1
      }
      j += 1
    }
    sums.update(math.pow(_, 1/p))
    sums
  }

  private[mllib] def negInPlace: SparseMatrix = {
    var j = 0
    val len = values.length
    while (j < len){
      values(j) *= -1
      j += 1
    }
    this
  }

  private[mllib] def neg: SparseMatrix = {
    val copy = this.copy
    copy.negInPlace
  }

  private[mllib] def compare(v: Double, f: (Double, Double) => Boolean): DenseMatrix = {
    val copy = new DenseMatrix(numRows, numCols, this.toArray)
    copy.compareInPlace(v, f)
  }

  def toDense: DenseMatrix = new DenseMatrix(numRows, numCols, this.toArray)
}

object SparseMatrix {

  /**
   * Generate an Identity Matrix in `SparseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `SparseMatrix` with size `n` x `n` and values of ones on the diagonal
   */
  def speye(n: Int): SparseMatrix = {
    new SparseMatrix(n, n, (0 to n).toArray, (0 until n).toArray, Array.fill(n)(1.0))
  }

  private def genRand(numRows: Int, numCols: Int, raw: Array[Double], nonZero: Int): SparseMatrix = {
    val sparseA: ArrayBuffer[Double] = new ArrayBuffer(nonZero)

    val sCols: ArrayBuffer[Int] = new ArrayBuffer(numCols + 1)
    val sRows: ArrayBuffer[Int] = new ArrayBuffer(nonZero)

    var i = 0
    var nnz = 0
    var lastCol = -1

    raw.foreach { v =>
      val r = i % numRows
      val c = (i - r) / numRows
      if ( v != 0.0) {
        sRows.append(r)
        sparseA.append(v)
        while (c != lastCol){
          sCols.append(nnz)
          lastCol += 1
        }
        nnz += 1
      }
      i += 1
    }
    sCols.append(sparseA.length)
    new SparseMatrix(numRows, numCols, sCols.toArray, sRows.toArray, sparseA.toArray)
  }

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param seed the seed for the random generator
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def sprand(
      numRows: Int,
      numCols: Int,
      density: Double,
      seed: Long = Utils.random.nextLong()): SparseMatrix = {

    require(density > 0.0 && density < 1.0, "density must be a double in the range " +
      s"0.0 < d < 1.0. Currently, density: $density")
    val rand = new XORShiftRandom(seed)
    val length = numRows * numCols
    val rawA = Array.fill(length)(0.0)
    var nnz = 0
    for (i <- 0 until length) {
      val p = rand.nextDouble()
      if (p < density) {
        rawA.update(i, rand.nextDouble())
        nnz += 1
      }
    }
    genRand(numRows, numCols, rawA, nnz)
  }

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param seed the seed for the random generator
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def sprandn(
      numRows: Int,
      numCols: Int,
      density: Double,
      seed: Long = Utils.random.nextLong()): SparseMatrix = {

    require(density > 0.0 && density < 1.0, "density must be a double in the range " +
      s"0.0 < d < 1.0. Currently, density: $density")
    val rand = new XORShiftRandom(seed)
    val length = numRows * numCols
    val rawA = Array.fill(length)(0.0)
    var nnz = 0
    for (i <- 0 until length) {
      val p = rand.nextDouble()
      if (p < density) {
        rawA.update(i, rand.nextGaussian())
        nnz += 1
      }
    }
    genRand(numRows, numCols, rawA, nnz)
  }

  /**
   * Generate a diagonal matrix in `DenseMatrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `SparseMatrix` with size `values.length` x `values.length` and non-zero `values`
   *         on the diagonal
   */
  def diag(vector: Vector): SparseMatrix = {
    val n = vector.size
    vector match {
      case sVec: SparseVector =>
        val rows = sVec.indices
        val values = sVec.values
        var i = 0
        var lastCol = -1
        val colPtrs = new ArrayBuffer[Int](n)
        rows.foreach { r =>
          while (r != lastCol) {
            colPtrs.append(i)
            lastCol += 1
          }
          i += 1
        }
        colPtrs.append(n)
        new SparseMatrix(n, n, colPtrs.toArray, rows, values)
      case dVec: DenseVector =>
        val values = dVec.values
        var i = 0
        var nnz = 0
        val sVals = values.filter( v => v != 0.0)
        var lastCol = -1
        val colPtrs = new ArrayBuffer[Int](n + 1)
        val sRows = new ArrayBuffer[Int](sVals.length)
        values.foreach { v =>
          if (v != 0.0) {
            sRows.append(i)
            while (lastCol != i) {
              colPtrs.append(nnz)
              lastCol += 1
            }
            nnz += 1
          }
          i += 1
        }
        while (lastCol != i) {
          colPtrs.append(nnz)
          lastCol += 1
        }
        new SparseMatrix(n, n, colPtrs.toArray, sRows.toArray, sVals)
    }
  }
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
   * Creates a column-majored sparse matrix in Compressed Sparse Column (CSC) format.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param colPtrs the index corresponding to the start of a new column
   * @param rowIndices the row index of the entry
   * @param values non-zero matrix entries in column major
   */
  def sparse(
     numRows: Int,
     numCols: Int,
     colPtrs: Array[Int],
     rowIndices: Array[Int],
     values: Array[Double]): Matrix = {
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values)
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

  /**
   * Generate a `DenseMatrix` consisting of zeros.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values of zeros
   */
  def zeros(numRows: Int, numCols: Int): Matrix = DenseMatrix.zeros(numRows, numCols)

  /**
   * Generate a `DenseMatrix` consisting of ones.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values of ones
   */
  def ones(numRows: Int, numCols: Int): Matrix = DenseMatrix.ones(numRows, numCols)

  /**
   * Generate an Identity Matrix in `DenseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `Matrix` with size `n` x `n` and values of ones on the diagonal
   */
  def eye(n: Int): Matrix = DenseMatrix.eye(n)

  /**
   * Generate an Identity Matrix in `SparseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `Matrix` with size `n` x `n` and values of ones on the diagonal
   */
  def speye(n: Int): Matrix = SparseMatrix.speye(n)

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def rand(numRows: Int, numCols: Int): Matrix = DenseMatrix.rand(numRows, numCols)

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def randn(numRows: Int, numCols: Int): Matrix = DenseMatrix.randn(numRows, numCols)

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param seed the seed for the random generator
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def sprand(
      numRows: Int,
      numCols: Int,
      density: Double,
      seed: Long = Utils.random.nextLong()): Matrix =
    SparseMatrix.sprand(numRows, numCols, density, seed)

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param seed the seed for the random generator
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def sprandn(
      numRows: Int,
      numCols: Int,
      density: Double,
      seed: Long = Utils.random.nextLong()): Matrix =
    SparseMatrix.sprandn(numRows, numCols, density, seed)

  /**
   * Generate a diagonal matrix in `DenseMatrix` format from the supplied values. Use
   * [[org.apache.spark.mllib.linalg.SparseMatrix.diag()]] in order to generate the matrix in
   * `SparseMatrix` format.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `Matrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
   */
  def diag(vector: Vector): Matrix = DenseMatrix.diag(vector)

  /**
   * Horizontally concatenate a sequence of matrices. The returned matrix will be in the format
   * the matrices are supplied in. Supplying a mix of dense and sparse matrices is not supported.
   * @param matrices sequence of matrices
   * @return a single `Matrix` composed of the matrices that were horizontally concatenated
   */
  private[mllib] def horzCat(matrices: Seq[Matrix]): Matrix = {
    if (matrices.size == 1) {
      return matrices(0)
    }
    val numRows = matrices(0).numRows
    var rowsMatch = true
    var isDense = false
    var isSparse = false
    for (mat <- matrices) {
      if (numRows != mat.numRows) rowsMatch = false
      mat match {
        case sparse: SparseMatrix => isSparse = true
        case dense: DenseMatrix => isDense = true
      }
    }
    require(rowsMatch, "The number of rows of the matrices in this array, don't match!")
    var numCols = 0
    matrices.foreach(numCols += _.numCols)
    if (isSparse && !isDense) {
      val allColPtrs: Array[Int] = Array(0) ++ matrices.flatMap { mat =>
        val ptr = mat.asInstanceOf[SparseMatrix].colPtrs
        ptr.slice(1, ptr.length)
      }
      var counter = 0
      val adjustedPtrs = allColPtrs.map { p =>
        counter += p
        counter
      }
      new SparseMatrix(numRows, numCols, adjustedPtrs,
        matrices.flatMap(_.asInstanceOf[SparseMatrix].rowIndices).toArray,
        matrices.flatMap(_.asInstanceOf[SparseMatrix].values).toArray)
    } else if (!isSparse && !isDense) {
      throw new IllegalArgumentException("The supplied matrices are neither in SparseMatrix or" +
        " DenseMatrix format!")
    }else {
      new DenseMatrix(numRows, numCols, matrices.flatMap(_.toArray).toArray)
    }
  }
  // partitionMetaData correspond to the index of the partition and the max number of non-zeros
  // in that partition so that we can preallocate a memory efficient buffer
  private[mllib] def fromRDD(
      rows: RDD[(Double, Vector)],
      partitionMetaData: Array[(Int, Int)],
      batchSize : Int,
      buildSparseThreshold: Double,
      generateOnTheFly: Boolean = true): RDD[(DenseMatrix, Matrix)] = {

    if (!generateOnTheFly){
      rows.mapPartitions { iter =>
        iter.grouped(batchSize)
      }.map(fromSeq(_, batchSize))
    }else {
      val numFeatures = rows.first()._2.size

      rows.mapPartitionsWithIndex{ case (ind, iter) =>
        val findPartition = partitionMetaData.find(_._1 == ind)
        val matrixBuffer =
          if (findPartition.get._2 != -1) {
            val nnz = findPartition.get._2
            val density = nnz * 1.0 / (numFeatures * batchSize)
            if (density <= buildSparseThreshold) {
              (DenseMatrix.zeros(batchSize, 1), new SparseMatrix(numFeatures, batchSize,
                Array.fill(batchSize + 1)(0), Array.fill(nnz)(0), Array.fill(nnz)(0.0)))
            } else {
              (DenseMatrix.zeros(batchSize, 1), DenseMatrix.zeros(numFeatures, batchSize))
            }
          } else {
            (DenseMatrix.zeros(batchSize, 1), DenseMatrix.zeros(numFeatures, batchSize))
          }
        iter.grouped(batchSize).map(fromSeqIntoBuffer(_, matrixBuffer, batchSize)._2)
      }
    }
  }

  // Collects data on the maximum number of non-zero elements in a partition for each
  // batch of matrices
  private[mllib] def getSparsityData(
      rows: RDD[(Double, Vector)],
      batchSize : Int = 64): Array[(Int, Int)] = {
    val numFeatures = rows.first()._2.size

    val partitionMetaData = rows.mapPartitionsWithIndex { case (ind, iter) =>
      val matrixBuffer =
        (DenseMatrix.zeros(batchSize, 1), DenseMatrix.zeros(numFeatures, batchSize))
      var partitionMaxNNZ = -1

      iter.grouped(batchSize).foreach { r =>
        val (metaData, _) = fromSeqIntoBuffer(r, matrixBuffer, batchSize)
        val maxNNZ =
          if (metaData > partitionMaxNNZ) metaData else partitionMaxNNZ

        partitionMaxNNZ = maxNNZ
      }

      Iterator((ind, partitionMaxNNZ))
    }
    partitionMetaData.collect()
  }

  private def fromSeq(rows: Seq[(Double, Vector)], batchSize: Int) : (DenseMatrix, Matrix) = {
    val numExamples = rows.length
    val numFeatures = rows(0)._2.size
    val matrixBuffer = DenseMatrix.zeros(numExamples, numFeatures)
    val labelBuffer = DenseMatrix.zeros(numExamples, 1)
    flattenMatrix(rows, matrixBuffer, labelBuffer, batchSize)

    (matrixBuffer, labelBuffer)
  }

  private def fromSeqIntoBuffer(
      rows: Seq[(Double, Vector)],
      buffer: (DenseMatrix, Matrix),
      batchSize: Int) : (Int, (DenseMatrix, Matrix)) = {
    val labelBuffer = buffer._1
    val matrixBuffer = buffer._2
    val metadata = flattenMatrix(rows, matrixBuffer, labelBuffer, batchSize)

    (metadata, buffer)
  }

  private def flattenMatrix(
      vals: Seq[(Double, Vector)],
      matrixInto: Matrix,
      labelsInto: DenseMatrix,
      batchSize: Int): Int = {
    val numExamples = vals.length
    val numFeatures = vals(0)._2.size
    var i = 0
    var nnz = 0
    matrixInto match {
      case intoSparse: SparseMatrix =>
        for (r <- vals) {
          labelsInto.values(i) = r._1
          r._2 match {
            case sVec: SparseVector =>
              val len = sVec.indices.length
              var j = 0
              intoSparse.colPtrs(i) = nnz
              while (j < len) {
                intoSparse.rowIndices(nnz) = sVec.indices(j)
                intoSparse.values(nnz) = sVec.values(j)
                nnz += 1
                j += 1
              }
            case dVec: DenseVector =>
              var j = 0
              intoSparse.colPtrs(i) = nnz
              while (j < numFeatures) {
                val value = dVec.values(j)
                if (value != 0.0) {
                  intoSparse.rowIndices(nnz) = j
                  intoSparse.values(nnz) = dVec.values(j)
                  nnz += 1
                }
                j += 1
              }
          }
          i += 1
        }
        while (i < batchSize) {
          intoSparse.colPtrs(i) = nnz
          i += 1
        }
      case intoDense: DenseMatrix =>
        for (r <- vals) {
          labelsInto.values(i) = r._1
          val startIndex = numFeatures * i
          r._2 match {
            case sVec: SparseVector =>
              val len = sVec.indices.length
              var j = 0
              var sVecCounter = 0
              while (j < numFeatures) {
                intoDense.values(startIndex + j) = 0.0
                if (sVecCounter < len) {
                  if (j == sVec.indices(sVecCounter)) {
                    intoDense.values(startIndex + j) = sVec.values(sVecCounter)
                    nnz += 1
                    sVecCounter += 1
                  }
                }
                j += 1
              }
            case dVec: DenseVector =>
              var j = 0
              while (j < numFeatures) {
                val value = dVec.values(j)
                if (value != 0.0) nnz += 1
                intoDense.values(startIndex + j) = value
                j += 1
              }
          }
          i += 1
        }
        // clear existing values if we can not fill up the matrix
        if (numExamples != batchSize) {
          var j = numExamples * numFeatures
          val len = intoDense.values.length
          while (j < len) {
            intoDense.values(j) = 0.0
            j += 1
          }
        }
      }
    nnz
  }
}
