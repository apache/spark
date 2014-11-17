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

import java.util.Arrays

import breeze.linalg.{Matrix => BM, DenseMatrix => BDM, CSCMatrix => BSM}
import org.apache.spark.util.Utils

import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable.ArrayBuffer

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

  /** Convenience method for `Matrix`-`DenseMatrix` multiplication. */
  def multiply(y: DenseMatrix): DenseMatrix = {
    val C: DenseMatrix = Matrices.zeros(numRows, y.numCols).asInstanceOf[DenseMatrix]
    BLAS.gemm(false, false, 1.0, this, y, 0.0, C)
    C
  }

  /** Convenience method for `Matrix`-`DenseVector` multiplication. */
  def multiply(y: DenseVector): DenseVector = {
    val output = new DenseVector(new Array[Double](numRows))
    BLAS.gemv(1.0, this, y, 0.0, output)
    output
  }

  /** Convenience method for `Matrix`^T^-`DenseMatrix` multiplication. */
  def transposeMultiply(y: DenseMatrix): DenseMatrix = {
    val C: DenseMatrix = Matrices.zeros(numCols, y.numCols).asInstanceOf[DenseMatrix]
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

  /** Map the values of this matrix using a function. Generates a new matrix. */
  private[mllib] def map(f: Double => Double): Matrix

  /** Update all the values of this matrix using the function f. Performed in-place. */
  private[mllib] def update(f: Double => Double): Matrix
}

/**
 * Column-major dense matrix.
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

  require(values.length == numRows * numCols, "The number of values supplied doesn't match the " +
    s"size of the matrix! values.length: ${values.length}, numRows * numCols: ${numRows * numCols}")

  override def toArray: Array[Double] = values

  override def equals(o: Any) = o match {
    case m: DenseMatrix =>
      m.numRows == numRows && m.numCols == numCols && Arrays.equals(toArray, m.toArray)
    case _ => false
  }

  private[mllib] def toBreeze: BM[Double] = new BDM[Double](numRows, numCols, values)

  private[mllib] def apply(i: Int): Double = values(i)

  private[mllib] def apply(i: Int, j: Int): Double = values(index(i, j))

  private[mllib] def index(i: Int, j: Int): Int = i + numRows * j

  private[mllib] def update(i: Int, j: Int, v: Double): Unit = {
    values(index(i, j)) = v
  }

  override def copy = new DenseMatrix(numRows, numCols, values.clone())

  private[mllib] def map(f: Double => Double) = new DenseMatrix(numRows, numCols, values.map(f))

  private[mllib] def update(f: Double => Double): DenseMatrix = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = f(values(i))
      i += 1
    }
    this
  }
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.DenseMatrix]].
 */
object DenseMatrix {

  /**
   * Generate a `DenseMatrix` consisting of zeros.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of zeros
   */
  def zeros(numRows: Int, numCols: Int): DenseMatrix =
    new DenseMatrix(numRows, numCols, new Array[Double](numRows * numCols))

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
   * @param seed the seed seed for the random number generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def rand(numRows: Int, numCols: Int, seed: Long): DenseMatrix = {
    val rand = new XORShiftRandom(seed)
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rand.nextDouble()))
  }

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def rand(numRows: Int, numCols: Int): DenseMatrix = {
    rand(numRows, numCols, Utils.random.nextLong())
  }

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param seed the seed seed for the random number generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def randn(numRows: Int, numCols: Int, seed: Long): DenseMatrix = {
    val rand = new XORShiftRandom(seed)
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rand.nextGaussian()))
  }

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def randn(numRows: Int, numCols: Int): DenseMatrix = {
    randn(numRows, numCols, Utils.random.nextLong())
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
 * Column-major sparse matrix.
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
    val values: Array[Double]) extends Matrix {

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

  private[mllib] def map(f: Double => Double) =
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.map(f))

  private[mllib] def update(f: Double => Double): SparseMatrix = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = f(values(i))
      i += 1
    }
    this
  }
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.SparseMatrix]].
 */
object SparseMatrix {

  /**
   * Generate an Identity Matrix in `SparseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `SparseMatrix` with size `n` x `n` and values of ones on the diagonal
   */
  def speye(n: Int): SparseMatrix = {
    new SparseMatrix(n, n, (0 to n).toArray, (0 until n).toArray, Array.fill(n)(1.0))
  }

  /** Generates a SparseMatrix given an Array[Double] of size numRows * numCols. The number of
    * non-zeros in `raw` is provided for efficiency. */
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
      seed: Long): SparseMatrix = {

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
   * Generate a `SparseMatrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def sprand(numRows: Int, numCols: Int, density: Double): SparseMatrix = {
    sprand(numRows, numCols, density, Utils.random.nextLong())
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
      seed: Long): SparseMatrix = {

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
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def sprandn(numRows: Int, numCols: Int, density: Double): SparseMatrix = {
    sprandn(numRows, numCols, density, Utils.random.nextLong())
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
   * Creates a column-major dense matrix.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param values matrix entries in column major
   */
  def dense(numRows: Int, numCols: Int, values: Array[Double]): Matrix = {
    new DenseMatrix(numRows, numCols, values)
  }

  /**
   * Creates a column-major sparse matrix in Compressed Sparse Column (CSC) format.
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
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of zeros
   */
  def zeros(numRows: Int, numCols: Int): Matrix = DenseMatrix.zeros(numRows, numCols)

  /**
   * Generate a `DenseMatrix` consisting of ones.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of ones
   */
  def ones(numRows: Int, numCols: Int): Matrix = DenseMatrix.ones(numRows, numCols)

  /**
   * Generate a dense Identity Matrix in `Matrix` format.
   * @param n number of rows and columns of the matrix
   * @return `DenseMatrix` with size `n` x `n` and values of ones on the diagonal
   */
  def eye(n: Int): Matrix = DenseMatrix.eye(n)

  /**
   * Generate a sparse Identity Matrix in `Matrix` format.
   * @param n number of rows and columns of the matrix
   * @return `SparseMatrix` with size `n` x `n` and values of ones on the diagonal
   */
  def speye(n: Int): Matrix = SparseMatrix.speye(n)

  /**
   * Generate a dense `Matrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param seed the seed for the random generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def rand(numRows: Int, numCols: Int, seed: Long): Matrix =
    DenseMatrix.rand(numRows, numCols, seed)

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def rand(numRows: Int, numCols: Int): Matrix = DenseMatrix.rand(numRows, numCols)

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param seed the seed for the random generator
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def sprand(numRows: Int, numCols: Int, density: Double, seed: Long): Matrix =
    SparseMatrix.sprand(numRows, numCols, density, seed)

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def sprand(numRows: Int, numCols: Int, density: Double): Matrix =
    SparseMatrix.sprand(numRows, numCols, density)

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param seed the seed for the random generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def randn(numRows: Int, numCols: Int, seed: Long): Matrix =
    DenseMatrix.randn(numRows, numCols, seed)

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def randn(numRows: Int, numCols: Int): Matrix = DenseMatrix.randn(numRows, numCols)

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param seed the seed for the random generator
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def sprandn(numRows: Int, numCols: Int, density: Double, seed: Long): Matrix =
    SparseMatrix.sprandn(numRows, numCols, density, seed)

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def sprandn(numRows: Int, numCols: Int, density: Double): Matrix =
    SparseMatrix.sprandn(numRows, numCols, density)

  /**
   * Generate a diagonal matrix in `DenseMatrix` format from the supplied values.
   * @param vector a `Vector` tat will form the values on the diagonal of the matrix
   * @return Square `DenseMatrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
   */
  def diag(vector: Vector): Matrix = DenseMatrix.diag(vector)

  /**
   * Horizontally concatenate a sequence of matrices. The returned matrix will be in the format
   * the matrices are supplied in. Supplying a mix of dense and sparse matrices will result in
   * a dense matrix.
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
    var numCols = 0
    matrices.foreach { mat =>
      if (numRows != mat.numRows) rowsMatch = false
      mat match {
        case sparse: SparseMatrix => isSparse = true
        case dense: DenseMatrix => isDense = true
      }
      numCols += mat.numCols
    }
    require(rowsMatch, "The number of rows of the matrices in this sequence, don't match!")

    if (isSparse && !isDense) {
      val allColPtrs: Array[(Int, Int)] = Array((0, 0)) ++
        matrices.zipWithIndex.flatMap { case (mat, ind) =>
          val ptr = mat.asInstanceOf[SparseMatrix].colPtrs
          ptr.slice(1, ptr.length).map(p => (ind, p))
      }
      var counter = 0
      var lastIndex = 0
      var lastPtr = 0
      val adjustedPtrs = allColPtrs.map { case (ind, p) =>
        if (ind != lastIndex) {
          counter += lastPtr
          lastIndex = ind
        }
        lastPtr = p
        counter + p
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

  /**
   * Vertically concatenate a sequence of matrices. The returned matrix will be in the format
   * the matrices are supplied in. Supplying a mix of dense and sparse matrices will result in
   * a dense matrix.
   * @param matrices sequence of matrices
   * @return a single `Matrix` composed of the matrices that were horizontally concatenated
   */
  private[mllib] def vertCat(matrices: Seq[Matrix]): Matrix = {
    if (matrices.size == 1) {
      return matrices(0)
    }
    val numCols = matrices(0).numCols
    var colsMatch = true
    var isDense = false
    var isSparse = false
    var numRows = 0
    var valsLength = 0
    matrices.foreach { mat =>
      if (numCols != mat.numCols) colsMatch = false
      mat match {
        case sparse: SparseMatrix =>
          isSparse = true
          valsLength += sparse.values.length
        case dense: DenseMatrix =>
          isDense = true
          valsLength += dense.values.length
      }
      numRows += mat.numRows

    }
    require(colsMatch, "The number of rows of the matrices in this sequence, don't match!")

    if (isSparse && !isDense) {
      val matMap = matrices.zipWithIndex.map(d => (d._2, d._1.asInstanceOf[SparseMatrix])).toMap
      // (matrixInd, colInd, colStart, colEnd, numRows)
      val allColPtrs: Seq[(Int, Int, Int, Int, Int)] =
        matMap.flatMap { case (ind, mat) =>
          val ptr = mat.colPtrs
          var colStart = 0
          var j = 0
          ptr.slice(1, ptr.length).map { p =>
            j += 1
            val oldColStart = colStart
            colStart = p
            (j - 1, ind, oldColStart, p, mat.numRows)
          }
        }.toSeq
      val values = new ArrayBuffer[Double](valsLength)
      val rowInd = new ArrayBuffer[Int](valsLength)
      val newColPtrs = new Array[Int](numCols)

      // group metadata by column index and then sort in increasing order of column index
      allColPtrs.groupBy(_._1).toArray.sortBy(_._1).foreach { case (colInd, data) =>
        // then sort by matrix index
        val sortedPtrs = data.sortBy(_._1)
        var startRow = 0
        sortedPtrs.foreach { case (colIdx, matrixInd, colStart, colEnd, nRows) =>
          val selectedMatrix = matMap(matrixInd)
          val selectedValues = selectedMatrix.values.slice(colStart, colEnd)
          val selectedRowIdx = selectedMatrix.rowIndices.slice(colStart, colEnd)
          val len = selectedValues.length
          newColPtrs(colIdx) += len
          var i = 0
          while (i < len) {
            values.append(selectedValues(i))
            rowInd.append(selectedRowIdx(i) + startRow)
            i += 1
          }
          startRow += nRows
        }
      }
      val adjustedPtrs = newColPtrs.scanLeft(0)(_ + _)
      new SparseMatrix(numRows, numCols, adjustedPtrs, rowInd.toArray, values.toArray)
    } else if (!isSparse && !isDense) {
      throw new IllegalArgumentException("The supplied matrices are neither in SparseMatrix or" +
        " DenseMatrix format!")
    }else {
      val matData = matrices.zipWithIndex.flatMap { case (mat, ind) =>
        val values = mat.toArray
        for (j <- 0 until numCols) yield (j, ind,
          values.slice(j * mat.numRows, (j + 1) * mat.numRows))
      }.sortBy(x => (x._1, x._2))
      new DenseMatrix(numRows, numCols, matData.flatMap(_._3).toArray)
    }
  }
}
