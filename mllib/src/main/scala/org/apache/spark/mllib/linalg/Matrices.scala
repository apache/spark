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

import scala.collection.mutable.{ArrayBuilder => MArrayBuilder, HashSet => MHashSet, ArrayBuffer}

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}

/**
 * Trait for a local matrix.
 */
sealed trait Matrix extends Serializable {

  /** Number of rows. */
  def numRows: Int

  /** Number of columns. */
  def numCols: Int

  /** Flag that keeps track whether the matrix is transposed or not. False by default. */
  val isTransposed: Boolean = false

  /** Converts to a dense array in column major. */
  def toArray: Array[Double] = {
    val newArray = new Array[Double](numRows * numCols)
    foreachActive { (i, j, v) =>
      newArray(j * numRows + i) = v
    }
    newArray
  }

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

  /** Transpose the Matrix. Returns a new `Matrix` instance sharing the same underlying data. */
  def transpose: Matrix

  /** Convenience method for `Matrix`-`DenseMatrix` multiplication. */
  def multiply(y: DenseMatrix): DenseMatrix = {
    val C: DenseMatrix = DenseMatrix.zeros(numRows, y.numCols)
    BLAS.gemm(1.0, this, y, 0.0, C)
    C
  }

  /** Convenience method for `Matrix`-`DenseVector` multiplication. */
  def multiply(y: DenseVector): DenseVector = {
    val output = new DenseVector(new Array[Double](numRows))
    BLAS.gemv(1.0, this, y, 0.0, output)
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

  /**
   * Applies a function `f` to all the active elements of dense and sparse matrix. The ordering
   * of the elements are not defined.
   *
   * @param f the function takes three parameters where the first two parameters are the row
   *          and column indices respectively with the type `Int`, and the final parameter is the
   *          corresponding value in the matrix with type `Double`.
   */
  private[spark] def foreachActive(f: (Int, Int, Double) => Unit)
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
 * @param isTransposed whether the matrix is transposed. If true, `values` stores the matrix in
 *                     row major.
 */
class DenseMatrix(
    val numRows: Int,
    val numCols: Int,
    val values: Array[Double],
    override val isTransposed: Boolean) extends Matrix {

  require(values.length == numRows * numCols, "The number of values supplied doesn't match the " +
    s"size of the matrix! values.length: ${values.length}, numRows * numCols: ${numRows * numCols}")

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
  def this(numRows: Int, numCols: Int, values: Array[Double]) =
    this(numRows, numCols, values, false)

  override def equals(o: Any) = o match {
    case m: DenseMatrix =>
      m.numRows == numRows && m.numCols == numCols && Arrays.equals(toArray, m.toArray)
    case _ => false
  }

  private[mllib] def toBreeze: BM[Double] = {
    if (!isTransposed) {
      new BDM[Double](numRows, numCols, values)
    } else {
      val breezeMatrix = new BDM[Double](numCols, numRows, values)
      breezeMatrix.t
    }
  }

  private[mllib] def apply(i: Int): Double = values(i)

  private[mllib] def apply(i: Int, j: Int): Double = values(index(i, j))

  private[mllib] def index(i: Int, j: Int): Int = {
    if (!isTransposed) i + numRows * j else j + numCols * i
  }

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

  override def transpose: Matrix = new DenseMatrix(numCols, numRows, values, !isTransposed)

  private[spark] override def foreachActive(f: (Int, Int, Double) => Unit): Unit = {
    if (!isTransposed) {
      // outer loop over columns
      var j = 0
      while (j < numCols) {
        var i = 0
        val indStart = j * numRows
        while (i < numRows) {
          f(i, j, values(indStart + i))
          i += 1
        }
        j += 1
      }
    } else {
      // outer loop over rows
      var i = 0
      while (i < numRows) {
        var j = 0
        val indStart = i * numCols
        while (j < numCols) {
          f(i, j, values(indStart + j))
          j += 1
        }
        i += 1
      }
    }
  }

  /** Generate a `SparseMatrix` from the given `DenseMatrix`. The new matrix will have isTransposed
    * set to false. */
  def toSparse(): SparseMatrix = {
    val spVals: MArrayBuilder[Double] = new MArrayBuilder.ofDouble
    val colPtrs: Array[Int] = new Array[Int](numCols + 1)
    val rowIndices: MArrayBuilder[Int] = new MArrayBuilder.ofInt
    var nnz = 0
    var j = 0
    while (j < numCols) {
      var i = 0
      while (i < numRows) {
        val v = values(index(i, j))
        if (v != 0.0) {
          rowIndices += i
          spVals += v
          nnz += 1
        }
        i += 1
      }
      j += 1
      colPtrs(j) = nnz
    }
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices.result(), spVals.result())
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
 * @param isTransposed whether the matrix is transposed. If true, the matrix can be considered
 *                     Compressed Sparse Row (CSR) format, where `colPtrs` behaves as rowPtrs,
 *                     and `rowIndices` behave as colIndices, and `values` are stored in row major.
 */
class SparseMatrix(
    val numRows: Int,
    val numCols: Int,
    val colPtrs: Array[Int],
    val rowIndices: Array[Int],
    val values: Array[Double],
    override val isTransposed: Boolean) extends Matrix {

  require(values.length == rowIndices.length, "The number of row indices and values don't match! " +
    s"values.length: ${values.length}, rowIndices.length: ${rowIndices.length}")
  // The Or statement is for the case when the matrix is transposed
  require(colPtrs.length == numCols + 1 || colPtrs.length == numRows + 1, "The length of the " +
    "column indices should be the number of columns + 1. Currently, colPointers.length: " +
    s"${colPtrs.length}, numCols: $numCols")
  require(values.length == colPtrs.last, "The last value of colPtrs must equal the number of " +
    s"elements. values.length: ${values.length}, colPtrs.last: ${colPtrs.last}")

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
   * @param rowIndices the row index of the entry. They must be in strictly increasing
   *                   order for each column
   * @param values non-zero matrix entries in column major
   */
  def this(
      numRows: Int,
      numCols: Int,
      colPtrs: Array[Int],
      rowIndices: Array[Int],
      values: Array[Double]) = this(numRows, numCols, colPtrs, rowIndices, values, false)

  private[mllib] def toBreeze: BM[Double] = {
     if (!isTransposed) {
       new BSM[Double](values, numRows, numCols, colPtrs, rowIndices)
     } else {
       val breezeMatrix = new BSM[Double](values, numCols, numRows, colPtrs, rowIndices)
       breezeMatrix.t
     }
  }

  private[mllib] def apply(i: Int, j: Int): Double = {
    val ind = index(i, j)
    if (ind < 0) 0.0 else values(ind)
  }

  private[mllib] def index(i: Int, j: Int): Int = {
    if (!isTransposed) {
      Arrays.binarySearch(rowIndices, colPtrs(j), colPtrs(j + 1), i)
    } else {
      Arrays.binarySearch(rowIndices, colPtrs(i), colPtrs(i + 1), j)
    }
  }

  private[mllib] def update(i: Int, j: Int, v: Double): Unit = {
    val ind = index(i, j)
    if (ind == -1) {
      throw new NoSuchElementException("The given row and column indices correspond to a zero " +
        "value. Only non-zero elements in Sparse Matrices can be updated.")
    } else {
      values(ind) = v
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

  override def transpose: Matrix =
    new SparseMatrix(numCols, numRows, colPtrs, rowIndices, values, !isTransposed)

  private[spark] override def foreachActive(f: (Int, Int, Double) => Unit): Unit = {
    if (!isTransposed) {
      var j = 0
      while (j < numCols) {
        var idx = colPtrs(j)
        val idxEnd = colPtrs(j + 1)
        while (idx < idxEnd) {
          f(rowIndices(idx), j, values(idx))
          idx += 1
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < numRows) {
        var idx = colPtrs(i)
        val idxEnd = colPtrs(i + 1)
        while (idx < idxEnd) {
          val j = rowIndices(idx)
          f(i, j, values(idx))
          idx += 1
        }
        i += 1
      }
    }
  }

  /** Generate a `DenseMatrix` from the given `SparseMatrix`. The new matrix will have isTransposed
    * set to false. */
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
   * (i, j, value) tuples. Entries that have duplicate values of i and j are
   * added together. Tuples where value is equal to zero will be omitted.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param entries Array of (i, j, value) tuples
   * @return The corresponding `SparseMatrix`
   */
  def fromCOO(numRows: Int, numCols: Int, entries: Iterable[(Int, Int, Double)]): SparseMatrix = {
    val sortedEntries = entries.toSeq.sortBy(v => (v._2, v._1))
    val numEntries = sortedEntries.size
    if (sortedEntries.nonEmpty) {
      // Since the entries are sorted by column index, we only need to check the first and the last.
      for (col <- Seq(sortedEntries.head._2, sortedEntries.last._2)) {
        require(col >= 0 && col < numCols, s"Column index out of range [0, $numCols): $col.")
      }
    }
    val colPtrs = new Array[Int](numCols + 1)
    val rowIndices = MArrayBuilder.make[Int]
    rowIndices.sizeHint(numEntries)
    val values = MArrayBuilder.make[Double]
    values.sizeHint(numEntries)
    var nnz = 0
    var prevCol = 0
    var prevRow = -1
    var prevVal = 0.0
    // Append a dummy entry to include the last one at the end of the loop.
    (sortedEntries.view :+ (numRows, numCols, 1.0)).foreach { case (i, j, v) =>
      if (v != 0) {
        if (i == prevRow && j == prevCol) {
          prevVal += v
        } else {
          if (prevVal != 0) {
            require(prevRow >= 0 && prevRow < numRows,
              s"Row index out of range [0, $numRows): $prevRow.")
            nnz += 1
            rowIndices += prevRow
            values += prevVal
          }
          prevRow = i
          prevVal = v
          while (prevCol < j) {
            colPtrs(prevCol + 1) = nnz
            prevCol += 1
          }
        }
      }
    }
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices.result(), values.result())
  }

  /**
   * Generate an Identity Matrix in `SparseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `SparseMatrix` with size `n` x `n` and values of ones on the diagonal
   */
  def speye(n: Int): SparseMatrix = {
    new SparseMatrix(n, n, (0 to n).toArray, (0 until n).toArray, Array.fill(n)(1.0))
  }

  /**
   * Generates the skeleton of a random `SparseMatrix` with a given random number generator.
   * The values of the matrix returned are undefined.
   */
  private def genRandMatrix(
      numRows: Int,
      numCols: Int,
      density: Double,
      rng: Random): SparseMatrix = {
    require(numRows > 0, s"numRows must be greater than 0 but got $numRows")
    require(numCols > 0, s"numCols must be greater than 0 but got $numCols")
    require(density >= 0.0 && density <= 1.0,
      s"density must be a double in the range 0.0 <= d <= 1.0. Currently, density: $density")
    val size = numRows.toLong * numCols
    val expected = size * density
    assert(expected < Int.MaxValue,
      "The expected number of nonzeros cannot be greater than Int.MaxValue.")
    val nnz = math.ceil(expected).toInt
    if (density == 0.0) {
      new SparseMatrix(numRows, numCols, new Array[Int](numCols + 1), Array[Int](), Array[Double]())
    } else if (density == 1.0) {
      val colPtrs = Array.tabulate(numCols + 1)(j => j * numRows)
      val rowIndices = Array.tabulate(size.toInt)(idx => idx % numRows)
      new SparseMatrix(numRows, numCols, colPtrs, rowIndices, new Array[Double](numRows * numCols))
    } else if (density < 0.34) {
      // draw-by-draw, expected number of iterations is less than 1.5 * nnz
      val entries = MHashSet[(Int, Int)]()
      while (entries.size < nnz) {
        entries += ((rng.nextInt(numRows), rng.nextInt(numCols)))
      }
      SparseMatrix.fromCOO(numRows, numCols, entries.map(v => (v._1, v._2, 1.0)))
    } else {
      // selection-rejection method
      var idx = 0L
      var numSelected = 0
      var j = 0
      val colPtrs = new Array[Int](numCols + 1)
      val rowIndices = new Array[Int](nnz)
      while (j < numCols && numSelected < nnz) {
        var i = 0
        while (i < numRows && numSelected < nnz) {
          if (rng.nextDouble() < 1.0 * (nnz - numSelected) / (size - idx)) {
            rowIndices(numSelected) = i
            numSelected += 1
          }
          i += 1
          idx += 1
        }
        colPtrs(j + 1) = numSelected
        j += 1
      }
      new SparseMatrix(numRows, numCols, colPtrs, rowIndices, new Array[Double](nnz))
    }
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
    val mat = genRandMatrix(numRows, numCols, density, rng)
    mat.update(i => rng.nextDouble())
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
    val mat = genRandMatrix(numRows, numCols, density, rng)
    mat.update(i => rng.nextGaussian())
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
        SparseMatrix.fromCOO(n, n, sVec.indices.zip(sVec.values).map(v => (v._1, v._1, v._2)))
      case dVec: DenseVector =>
        val entries = dVec.values.zipWithIndex
        val nnzVals = entries.filter(v => v._1 != 0.0)
        SparseMatrix.fromCOO(n, n, nnzVals.map(v => (v._2, v._2, v._1)))
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
        new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
      case sm: BSM[Double] =>
        // There is no isTranspose flag for sparse matrices in Breeze
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
    if (matrices.isEmpty) {
      return new DenseMatrix(0, 0, Array[Double]())
    } else if (matrices.size == 1) {
      return matrices(0)
    }
    val numRows = matrices(0).numRows
    var hasSparse = false
    var numCols = 0
    matrices.foreach { mat =>
      require(numRows == mat.numRows, "The number of rows of the matrices in this sequence, " +
        "don't match!")
      mat match {
        case sparse: SparseMatrix => hasSparse = true
        case dense: DenseMatrix => // empty on purpose
        case _ => throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${mat.getClass}")
      }
      numCols += mat.numCols
    }
    if (!hasSparse) {
      new DenseMatrix(numRows, numCols, matrices.flatMap(_.toArray))
    } else {
      var startCol = 0
      val entries: Array[(Int, Int, Double)] = matrices.flatMap { mat =>
        val nCols = mat.numCols
        mat match {
          case spMat: SparseMatrix =>
            val data = new Array[(Int, Int, Double)](spMat.values.length)
            var cnt = 0
            spMat.foreachActive { (i, j, v) =>
              data(cnt) = (i, j + startCol, v)
              cnt += 1
            }
            startCol += nCols
            data
          case dnMat: DenseMatrix =>
            val data = new ArrayBuffer[(Int, Int, Double)]()
            dnMat.foreachActive { (i, j, v) =>
              if (v != 0.0) {
                data.append((i, j + startCol, v))
              }
            }
            startCol += nCols
            data
        }
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
    if (matrices.isEmpty) {
      return new DenseMatrix(0, 0, Array[Double]())
    } else if (matrices.size == 1) {
      return matrices(0)
    }
    val numCols = matrices(0).numCols
    var hasSparse = false
    var numRows = 0
    matrices.foreach { mat =>
      require(numCols == mat.numCols, "The number of rows of the matrices in this sequence, " +
        "don't match!")
      mat match {
        case sparse: SparseMatrix => hasSparse = true
        case dense: DenseMatrix => // empty on purpose
        case _ => throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${mat.getClass}")
      }
      numRows += mat.numRows
    }
    if (!hasSparse) {
      val allValues = new Array[Double](numRows * numCols)
      var startRow = 0
      matrices.foreach { mat =>
        var j = 0
        val nRows = mat.numRows
        mat.foreachActive { (i, j, v) =>
          val indStart = j * numRows + startRow
          allValues(indStart + i) = v
        }
        startRow += nRows
      }
      new DenseMatrix(numRows, numCols, allValues)
    } else {
      var startRow = 0
      val entries: Array[(Int, Int, Double)] = matrices.flatMap { mat =>
        val nRows = mat.numRows
        mat match {
          case spMat: SparseMatrix =>
            val data = new Array[(Int, Int, Double)](spMat.values.length)
            var cnt = 0
            spMat.foreachActive { (i, j, v) =>
              data(cnt) = (i + startRow, j, v)
              cnt += 1
            }
            startRow += nRows
            data
          case dnMat: DenseMatrix =>
            val data = new ArrayBuffer[(Int, Int, Double)]()
            dnMat.foreachActive { (i, j, v) =>
              if (v != 0.0) {
                data.append((i + startRow, j, v))
              }
            }
            startRow += nRows
            data
        }
      }
      SparseMatrix.fromCOO(numRows, numCols, entries)
    }
  }
}
