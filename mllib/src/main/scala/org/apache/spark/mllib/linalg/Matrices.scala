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

import java.util.{Arrays, Random}

import scala.collection.mutable.{ArrayBuffer, Map}

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}

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
  private[mllib] def transposeMultiply(y: DenseMatrix): DenseMatrix = {
    val C: DenseMatrix = Matrices.zeros(numCols, y.numCols).asInstanceOf[DenseMatrix]
    BLAS.gemm(true, false, 1.0, this, y, 0.0, C)
    C
  }

  /** Convenience method for `Matrix`^T^-`DenseVector` multiplication. */
  private[mllib] def transposeMultiply(y: DenseVector): DenseVector = {
    val output = new DenseVector(new Array[Double](numCols))
    BLAS.gemv(true, 1.0, this, y, 0.0, output)
    output
  }

  /** A human readable representation of the matrix */
  override def toString: String = toBreeze.toString()

  /** Map the values of this matrix using a function. Generates a new matrix. Performs the
    * function on only the backing array. For example, an operation such as addition or
    * subtraction will only be performed on the non-zero values in a `SparseMatrix`. */
  private[mllib] def map(f: Double => Double): Matrix

  /** Update all the values of this matrix using the function f. Performed in-place on the
    * backing array. For example, an operation such as addition or subtraction will only be
    * performed on the non-zero values in a `SparseMatrix`. */
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

  /** Generate a `SparseMatrix` from the given `DenseMatrix`. */
  def toSparse(): SparseMatrix = {
    val sparseA: ArrayBuffer[Double] = new ArrayBuffer()
    val sCols: ArrayBuffer[Int] = new ArrayBuffer(numCols + 1)
    val sRows: ArrayBuffer[Int] = new ArrayBuffer()
    var i = 0
    var nnz = 0
    var lastCol = -1
    values.foreach { v =>
      val r = i % numRows
      val c = (i - r) / numRows
      if (v != 0.0) {
        sRows.append(r)
        sparseA.append(v)
        while (c != lastCol) {
          sCols.append(nnz)
          lastCol += 1
        }
        nnz += 1
      }
      i += 1
    }
    while (numCols > lastCol) {
      sCols.append(sparseA.length)
      lastCol += 1
    }
    new SparseMatrix(numRows, numCols, sCols.toArray, sRows.toArray, sparseA.toArray)
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
    while (i < n) {
      identity.update(i, i, 1.0)
      i += 1
    }
    identity
  }

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def rand(numRows: Int, numCols: Int, rng: Random): DenseMatrix = {
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rng.nextDouble()))
  }

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def randn(numRows: Int, numCols: Int, rng: Random): DenseMatrix = {
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rng.nextGaussian()))
  }

  /**
   * Generate a diagonal matrix in `DenseMatrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `DenseMatrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
   */
  def diag(vector: Vector): DenseMatrix = {
    val n = vector.size
    val matrix = DenseMatrix.zeros(n, n)
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
  require(values.length == colPtrs.last, "The last value of colPtrs must equal the number of " +
    s"elements. values.length: ${values.length}, colPtrs.last: ${colPtrs.last}")

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
    if (ind == -1) {
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

  /** Generate a `DenseMatrix` from the given `SparseMatrix`. */
  def toDense(): DenseMatrix = {
    new DenseMatrix(numRows, numCols, toArray)
  }
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.SparseMatrix]].
 */
object SparseMatrix {

  /**
   * Generate a `SparseMatrix` from Coordinate List (COO) format. Input must be an array of
   * (row, column, value) tuples. Array must be sorted first by *column* index and then by row
   * index.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param entries Array of ((row, column), value) tuples
   * @return The corresponding `SparseMatrix`
   */
  def fromCOO(numRows: Int, numCols: Int, entries: Array[((Int, Int), Double)]): SparseMatrix = {
    val colPtrs = new ArrayBuffer[Int](numCols + 1)
    colPtrs.append(0)
    var nnz = 0
    var lastCol = 0
    val values = entries.map { case ((i, j), v) =>
      while (j != lastCol) {
        colPtrs.append(nnz)
        lastCol += 1
        if (lastCol > numCols) {
          throw new IndexOutOfBoundsException("Please make sure that the entries array is " +
            "sorted by COLUMN index first and then by row index.")
        }
      }
      nnz += 1
      v
    }
    while (numCols > lastCol) {
      colPtrs.append(nnz)
      lastCol += 1
    }
    new SparseMatrix(numRows, numCols, colPtrs.toArray, entries.map(_._1._1), values)
  }

  /**
   * Generate an Identity Matrix in `SparseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `SparseMatrix` with size `n` x `n` and values of ones on the diagonal
   */
  def speye(n: Int): SparseMatrix = {
    new SparseMatrix(n, n, (0 to n).toArray, (0 until n).toArray, Array.fill(n)(1.0))
  }

  /** Generates a `SparseMatrix` with a given random number generator and `method`, which
    * specifies the distribution. */
  private def genRandMatrix(
      numRows: Int,
      numCols: Int,
      density: Double,
      rng: Random,
      method: Random => Double): SparseMatrix = {
    require(density >= 0.0 && density <= 1.0, "density must be a double in the range " +
      s"0.0 <= d <= 1.0. Currently, density: $density")
    val length = math.ceil(numRows * numCols * density).toInt
    val entries = Map[(Int, Int), Double]()
    var i = 0
    while (i < length) {
      var rowIndex = rng.nextInt(numRows)
      var colIndex = rng.nextInt(numCols)
      while (entries.contains((rowIndex, colIndex))) {
        rowIndex = rng.nextInt(numRows)
        colIndex = rng.nextInt(numCols)
      }
      entries += (rowIndex, colIndex) -> method(rng)
      i += 1
    }
    SparseMatrix.fromCOO(numRows, numCols, entries.toArray.sortBy(v => (v._1._2, v._1._1)))
  }

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. uniform random numbers. The number of non-zero
   * elements equal the ceiling of `numRows` x `numCols` x `density`
   *
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def sprand(numRows: Int, numCols: Int, density: Double, rng: Random): SparseMatrix = {
    def method(rand: Random): Double = rand.nextDouble()
    genRandMatrix(numRows, numCols, density, rng, method)
  }

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def sprandn(numRows: Int, numCols: Int, density: Double, rng: Random): SparseMatrix = {
    def method(rand: Random): Double = rand.nextGaussian()
    genRandMatrix(numRows, numCols, density, rng, method)
  }

  /**
   * Generate a diagonal matrix in `SparseMatrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `SparseMatrix` with size `values.length` x `values.length` and non-zero
   *         `values` on the diagonal
   */
  def diag(vector: Vector): SparseMatrix = {
    val n = vector.size
    vector match {
      case sVec: SparseVector =>
        val indices = sVec.indices.map(i => (i, i))
        SparseMatrix.fromCOO(n, n, indices.zip(sVec.values))
      case dVec: DenseVector =>
        val values = dVec.values.zipWithIndex
        val nnzVals = values.filter(v => v._1 != 0.0)
        SparseMatrix.fromCOO(n, n, nnzVals.map(v => ((v._2, v._2), v._1)))
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
   * Generate a dense Identity Matrix in `Matrix` format.
   * @param n number of rows and columns of the matrix
   * @return `Matrix` with size `n` x `n` and values of ones on the diagonal
   */
  def eye(n: Int): Matrix = DenseMatrix.eye(n)

  /**
   * Generate a sparse Identity Matrix in `Matrix` format.
   * @param n number of rows and columns of the matrix
   * @return `Matrix` with size `n` x `n` and values of ones on the diagonal
   */
  def speye(n: Int): Matrix = SparseMatrix.speye(n)

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def rand(numRows: Int, numCols: Int, rng: Random): Matrix =
    DenseMatrix.rand(numRows, numCols, rng)

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  def sprand(numRows: Int, numCols: Int, density: Double, rng: Random): Matrix =
    SparseMatrix.sprand(numRows, numCols, density, rng)

  /**
   * Generate a `DenseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def randn(numRows: Int, numCols: Int, rng: Random): Matrix =
    DenseMatrix.randn(numRows, numCols, rng)

  /**
   * Generate a `SparseMatrix` consisting of i.i.d. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  def sprandn(numRows: Int, numCols: Int, density: Double, rng: Random): Matrix =
    SparseMatrix.sprandn(numRows, numCols, density, rng)

  /**
   * Generate a diagonal matrix in `DenseMatrix` format from the supplied values.
   * @param vector a `Vector` tat will form the values on the diagonal of the matrix
   * @return Square `Matrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
   */
  def diag(vector: Vector): Matrix = DenseMatrix.diag(vector)

  /**
   * Horizontally concatenate a sequence of matrices. The returned matrix will be in the format
   * the matrices are supplied in. Supplying a mix of dense and sparse matrices will result in
   * a sparse matrix. If the Array is empty, an empty `DenseMatrix` will be returned.
   * @param matrices array of matrices
   * @return a single `Matrix` composed of the matrices that were horizontally concatenated
   */
  def horzcat(matrices: Array[Matrix]): Matrix = {
    if (matrices.size == 1) {
      return matrices(0)
    } else if (matrices.size == 0) {
      return new DenseMatrix(0, 0, Array[Double]())
    }
    val numRows = matrices(0).numRows
    var rowsMatch = true
    var hasDense = false
    var hasSparse = false
    var numCols = 0
    matrices.foreach { mat =>
      if (numRows != mat.numRows) rowsMatch = false
      mat match {
        case sparse: SparseMatrix => hasSparse = true
        case dense: DenseMatrix => hasDense = true
        case _ => throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${mat.getClass}")
      }
      numCols += mat.numCols
    }
    require(rowsMatch, "The number of rows of the matrices in this sequence, don't match!")

    if (!hasSparse && hasDense) {
      new DenseMatrix(numRows, numCols, matrices.flatMap(_.toArray))
    } else {
      var startCol = 0
      val entries: Array[((Int, Int), Double)] = matrices.flatMap {
        case spMat: SparseMatrix =>
          var j = 0
          var cnt = 0
          val ptr = spMat.colPtrs
          val data = spMat.rowIndices.zip(spMat.values).map { case (i, v) =>
            cnt += 1
            if (cnt <= ptr(j + 1)) {
              ((i, j + startCol), v)
            } else {
              while (ptr(j + 1) < cnt) {
                j += 1
              }
              ((i, j + startCol), v)
            }
          }
          startCol += spMat.numCols
          data
        case dnMat: DenseMatrix =>
          val nnzValues = dnMat.values.zipWithIndex.filter(v => v._1 != 0.0)
          val data = nnzValues.map { case (v, i) =>
            val rowIndex = i % dnMat.numRows
            val colIndex = i / dnMat.numRows
            ((rowIndex, colIndex + startCol), v)
          }
          startCol += dnMat.numCols
          data
      }
      SparseMatrix.fromCOO(numRows, numCols, entries)
    }
  }

  /**
   * Vertically concatenate a sequence of matrices. The returned matrix will be in the format
   * the matrices are supplied in. Supplying a mix of dense and sparse matrices will result in
   * a sparse matrix. If the Array is empty, an empty `DenseMatrix` will be returned.
   * @param matrices array of matrices
   * @return a single `Matrix` composed of the matrices that were vertically concatenated
   */
  def vertcat(matrices: Array[Matrix]): Matrix = {
    if (matrices.size == 1) {
      return matrices(0)
    } else if (matrices.size == 0) {
      return new DenseMatrix(0, 0, Array[Double]())
    }
    val numCols = matrices(0).numCols
    var colsMatch = true
    var hasDense = false
    var hasSparse = false
    var numRows = 0
    var valsLength = 0
    matrices.foreach { mat =>
      if (numCols != mat.numCols) colsMatch = false
      mat match {
        case sparse: SparseMatrix =>
          hasSparse = true
          valsLength += sparse.values.length
        case dense: DenseMatrix =>
          hasDense = true
          valsLength += dense.values.length
        case _ => throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${mat.getClass}")
      }
      numRows += mat.numRows

    }
    require(colsMatch, "The number of rows of the matrices in this sequence, don't match!")

    if (!hasSparse && hasDense) {
      val matData = matrices.zipWithIndex.flatMap { case (mat, ind) =>
        val values = mat.toArray
        for (j <- 0 until numCols) yield (j, ind,
          values.slice(j * mat.numRows, (j + 1) * mat.numRows))
      }.sortBy(x => (x._1, x._2))
      new DenseMatrix(numRows, numCols, matData.flatMap(_._3))
    } else {
      var startRow = 0
      val entries: Array[((Int, Int), Double)] = matrices.flatMap {
        case spMat: SparseMatrix =>
          var j = 0
          var cnt = 0
          val ptr = spMat.colPtrs
          val data = spMat.rowIndices.zip(spMat.values).map { case (i, v) =>
            cnt += 1
            if (cnt <= ptr(j + 1)) {
              ((i + startRow, j), v)
            } else {
              while (ptr(j + 1) < cnt) {
                j += 1
              }
              ((i + startRow, j), v)
            }
          }
          startRow += spMat.numRows
          data
        case dnMat: DenseMatrix =>
          val nnzValues = dnMat.values.zipWithIndex.filter(v => v._1 != 0.0)
          val data = nnzValues.map { case (v, i) =>
            val rowIndex = i % dnMat.numRows
            val colIndex = i / dnMat.numRows
            ((rowIndex + startRow, colIndex), v)
          }
          startRow += dnMat.numRows
          data
      }.sortBy(d => (d._1._2, d._1._1))
      SparseMatrix.fromCOO(numRows, numCols, entries)
    }
  }
}
