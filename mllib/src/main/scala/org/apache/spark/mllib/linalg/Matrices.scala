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

import scala.collection.mutable.{ArrayBuffer, ArrayBuilder => MArrayBuilder, HashSet => MHashSet}
import scala.language.implicitConversions

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{linalg => newlinalg}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.ArrayImplicits._

/**
 * Trait for a local matrix.
 */
@SQLUserDefinedType(udt = classOf[MatrixUDT])
@Since("1.0.0")
sealed trait Matrix extends Serializable {

  /** Number of rows. */
  @Since("1.0.0")
  def numRows: Int

  /** Number of columns. */
  @Since("1.0.0")
  def numCols: Int

  /** Flag that keeps track whether the matrix is transposed or not. False by default. */
  @Since("1.3.0")
  val isTransposed: Boolean = false

  /** Converts to a dense array in column major. */
  @Since("1.0.0")
  def toArray: Array[Double] = {
    val newArray = new Array[Double](numRows * numCols)
    foreachActive { (i, j, v) =>
      newArray(j * numRows + i) = v
    }
    newArray
  }

  /**
   * Returns an iterator of column vectors.
   * This operation could be expensive, depending on the underlying storage.
   */
  @Since("2.0.0")
  def colIter: Iterator[Vector]

  /**
   * Returns an iterator of row vectors.
   * This operation could be expensive, depending on the underlying storage.
   */
  @Since("2.0.0")
  def rowIter: Iterator[Vector] = this.transpose.colIter

  /** Converts to a breeze matrix. */
  private[mllib] def asBreeze: BM[Double]

  /** Gets the (i, j)-th element. */
  @Since("1.3.0")
  def apply(i: Int, j: Int): Double

  /** Return the index for the (i, j)-th element in the backing array. */
  private[mllib] def index(i: Int, j: Int): Int

  /** Update element at (i, j) */
  private[mllib] def update(i: Int, j: Int, v: Double): Unit

  /** Get a deep copy of the matrix. */
  @Since("1.2.0")
  def copy: Matrix

  /**
   * Transpose the Matrix. Returns a new `Matrix` instance sharing the same underlying data.
   */
  @Since("1.3.0")
  def transpose: Matrix

  /**
   * Convenience method for `Matrix`-`DenseMatrix` multiplication.
   */
  @Since("1.2.0")
  def multiply(y: DenseMatrix): DenseMatrix = {
    val C: DenseMatrix = DenseMatrix.zeros(numRows, y.numCols)
    BLAS.gemm(1.0, this, y, 0.0, C)
    C
  }

  /**
   * Convenience method for `Matrix`-`DenseVector` multiplication. For binary compatibility.
   */
  @Since("1.2.0")
  def multiply(y: DenseVector): DenseVector = {
    multiply(y.asInstanceOf[Vector])
  }

  /**
   * Convenience method for `Matrix`-`Vector` multiplication.
   */
  @Since("1.4.0")
  def multiply(y: Vector): DenseVector = {
    val output = new DenseVector(new Array[Double](numRows))
    BLAS.gemv(1.0, this, y, 0.0, output)
    output
  }

  /** A human readable representation of the matrix */
  override def toString: String = asBreeze.toString()

  /** A human readable representation of the matrix with maximum lines and width */
  @Since("1.4.0")
  def toString(maxLines: Int, maxLineWidth: Int): String = asBreeze.toString(maxLines, maxLineWidth)

  /**
   * Map the values of this matrix using a function. Generates a new matrix. Performs the
   * function on only the backing array. For example, an operation such as addition or
   * subtraction will only be performed on the non-zero values in a `SparseMatrix`.
   */
  private[spark] def map(f: Double => Double): Matrix

  /**
   * Update all the values of this matrix using the function f. Performed in-place on the
   * backing array. For example, an operation such as addition or subtraction will only be
   * performed on the non-zero values in a `SparseMatrix`.
   */
  private[mllib] def update(f: Double => Double): Matrix

  /**
   * Applies a function `f` to all the active elements of dense and sparse matrix. The ordering
   * of the elements are not defined.
   *
   * @param f the function takes three parameters where the first two parameters are the row
   *          and column indices respectively with the type `Int`, and the final parameter is the
   *          corresponding value in the matrix with type `Double`.
   */
  private[spark] def foreachActive(f: (Int, Int, Double) => Unit): Unit

  /**
   * Find the number of non-zero active values.
   */
  @Since("1.5.0")
  def numNonzeros: Int

  /**
   * Find the number of values stored explicitly. These values can be zero as well.
   */
  @Since("1.5.0")
  def numActives: Int

  /**
   * Convert this matrix to the new mllib-local representation.
   * This does NOT copy the data; it copies references.
   */
  @Since("2.0.0")
  def asML: newlinalg.Matrix
}

private[spark] class MatrixUDT extends UserDefinedType[Matrix] {

  override def sqlType: StructType = {
    // type: 0 = sparse, 1 = dense
    // the dense matrix is built by numRows, numCols, values and isTransposed, all of which are
    // set as not nullable, except values since in the future, support for binary matrices might
    // be added for which values are not needed.
    // the sparse matrix needs colPtrs and rowIndices, which are set as
    // null, while building the dense matrix.
    StructType(Array(
      StructField("type", ByteType, nullable = false),
      StructField("numRows", IntegerType, nullable = false),
      StructField("numCols", IntegerType, nullable = false),
      StructField("colPtrs", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("rowIndices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true),
      StructField("isTransposed", BooleanType, nullable = false)
      ))
  }

  override def serialize(obj: Matrix): InternalRow = {
    val row = new GenericInternalRow(7)
    obj match {
      case sm: SparseMatrix =>
        row.setByte(0, 0)
        row.setInt(1, sm.numRows)
        row.setInt(2, sm.numCols)
        row.update(3, UnsafeArrayData.fromPrimitiveArray(sm.colPtrs))
        row.update(4, UnsafeArrayData.fromPrimitiveArray(sm.rowIndices))
        row.update(5, UnsafeArrayData.fromPrimitiveArray(sm.values))
        row.setBoolean(6, sm.isTransposed)

      case dm: DenseMatrix =>
        row.setByte(0, 1)
        row.setInt(1, dm.numRows)
        row.setInt(2, dm.numCols)
        row.setNullAt(3)
        row.setNullAt(4)
        row.update(5, UnsafeArrayData.fromPrimitiveArray(dm.values))
        row.setBoolean(6, dm.isTransposed)
    }
    row
  }

  override def deserialize(datum: Any): Matrix = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 7,
          s"MatrixUDT.deserialize given row with length ${row.numFields} but requires length == 7")
        val tpe = row.getByte(0)
        val numRows = row.getInt(1)
        val numCols = row.getInt(2)
        val values = row.getArray(5).toDoubleArray()
        val isTransposed = row.getBoolean(6)
        tpe match {
          case 0 =>
            val colPtrs = row.getArray(3).toIntArray()
            val rowIndices = row.getArray(4).toIntArray()
            new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
          case 1 =>
            new DenseMatrix(numRows, numCols, values, isTransposed)
        }
    }
  }

  override def userClass: Class[Matrix] = classOf[Matrix]

  override def equals(o: Any): Boolean = {
    o match {
      case v: MatrixUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[MatrixUDT].getName.hashCode()

  override def typeName: String = "matrix"

  override def pyUDT: String = "pyspark.mllib.linalg.MatrixUDT"

  private[spark] override def asNullable: MatrixUDT = this
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
 * @param values matrix entries in column major if not transposed or in row major otherwise
 * @param isTransposed whether the matrix is transposed. If true, `values` stores the matrix in
 *                     row major.
 */
@Since("1.0.0")
@SQLUserDefinedType(udt = classOf[MatrixUDT])
class DenseMatrix @Since("1.3.0") (
    @Since("1.0.0") val numRows: Int,
    @Since("1.0.0") val numCols: Int,
    @Since("1.0.0") val values: Array[Double],
    @Since("1.3.0") override val isTransposed: Boolean) extends Matrix {

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
  @Since("1.0.0")
  def this(numRows: Int, numCols: Int, values: Array[Double]) =
    this(numRows, numCols, values, false)

  override def equals(o: Any): Boolean = o match {
    case m: Matrix => asBreeze == m.asBreeze
    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(numRows: Integer, numCols: Integer, toArray)
  }

  private[mllib] def asBreeze: BM[Double] = {
    if (!isTransposed) {
      new BDM[Double](numRows, numCols, values)
    } else {
      val breezeMatrix = new BDM[Double](numCols, numRows, values)
      breezeMatrix.t
    }
  }

  private[mllib] def apply(i: Int): Double = values(i)

  @Since("1.3.0")
  override def apply(i: Int, j: Int): Double = values(index(i, j))

  private[mllib] def index(i: Int, j: Int): Int = {
    require(i >= 0 && i < numRows, s"Expected 0 <= i < $numRows, got i = $i.")
    require(j >= 0 && j < numCols, s"Expected 0 <= j < $numCols, got j = $j.")
    if (!isTransposed) i + numRows * j else j + numCols * i
  }

  private[mllib] def update(i: Int, j: Int, v: Double): Unit = {
    values(index(i, j)) = v
  }

  @Since("1.4.0")
  override def copy: DenseMatrix = new DenseMatrix(numRows, numCols, values.clone())

  private[spark] def map(f: Double => Double) = new DenseMatrix(numRows, numCols, values.map(f),
    isTransposed)

  private[mllib] def update(f: Double => Double): DenseMatrix = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = f(values(i))
      i += 1
    }
    this
  }

  @Since("1.3.0")
  override def transpose: DenseMatrix = new DenseMatrix(numCols, numRows, values, !isTransposed)

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

  @Since("1.5.0")
  override def numNonzeros: Int = values.count(_ != 0)

  @Since("1.5.0")
  override def numActives: Int = values.length

  /**
   * Generate a `SparseMatrix` from the given `DenseMatrix`. The new matrix will have isTransposed
   * set to false.
   */
  @Since("1.3.0")
  def toSparse: SparseMatrix = {
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

  @Since("2.0.0")
  override def colIter: Iterator[Vector] = {
    if (isTransposed) {
      Iterator.tabulate(numCols) { j =>
        val col = new Array[Double](numRows)
        BLAS.nativeBLAS.dcopy(numRows, values, j, numCols, col, 0, 1)
        new DenseVector(col)
      }
    } else {
      Iterator.tabulate(numCols) { j =>
        new DenseVector(values.slice(j * numRows, (j + 1) * numRows))
      }
    }
  }

  @Since("2.0.0")
  override def asML: newlinalg.DenseMatrix = {
    new newlinalg.DenseMatrix(numRows, numCols, values, isTransposed)
  }
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.DenseMatrix]].
 */
@Since("1.3.0")
object DenseMatrix {

  /**
   * Generate a `DenseMatrix` consisting of zeros.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of zeros
   */
  @Since("1.3.0")
  def zeros(numRows: Int, numCols: Int): DenseMatrix = {
    require(numRows.toLong * numCols <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
            s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, new Array[Double](numRows * numCols))
  }

  /**
   * Generate a `DenseMatrix` consisting of ones.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of ones
   */
  @Since("1.3.0")
  def ones(numRows: Int, numCols: Int): DenseMatrix = {
    require(numRows.toLong * numCols <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
            s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(1.0))
  }

  /**
   * Generate an Identity Matrix in `DenseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `DenseMatrix` with size `n` x `n` and values of ones on the diagonal
   */
  @Since("1.3.0")
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
   * Generate a `DenseMatrix` consisting of `i.i.d.` uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  @Since("1.3.0")
  def rand(numRows: Int, numCols: Int, rng: Random): DenseMatrix = {
    require(numRows.toLong * numCols <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
            s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rng.nextDouble()))
  }

  /**
   * Generate a `DenseMatrix` consisting of `i.i.d.` gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  @Since("1.3.0")
  def randn(numRows: Int, numCols: Int, rng: Random): DenseMatrix = {
    require(numRows.toLong * numCols <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
            s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rng.nextGaussian()))
  }

  /**
   * Generate a diagonal matrix in `DenseMatrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `DenseMatrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
   */
  @Since("1.3.0")
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

  /**
   * Convert new linalg type to spark.mllib type.  Light copy; only copies references
   */
  @Since("2.0.0")
  def fromML(m: newlinalg.DenseMatrix): DenseMatrix = {
    new DenseMatrix(m.numRows, m.numCols, m.values, m.isTransposed)
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
 * @param colPtrs the index corresponding to the start of a new column (if not transposed)
 * @param rowIndices the row index of the entry (if not transposed). They must be in strictly
 *                   increasing order for each column
 * @param values nonzero matrix entries in column major (if not transposed)
 * @param isTransposed whether the matrix is transposed. If true, the matrix can be considered
 *                     Compressed Sparse Row (CSR) format, where `colPtrs` behaves as rowPtrs,
 *                     and `rowIndices` behave as colIndices, and `values` are stored in row major.
 */
@Since("1.2.0")
@SQLUserDefinedType(udt = classOf[MatrixUDT])
class SparseMatrix @Since("1.3.0") (
    @Since("1.2.0") val numRows: Int,
    @Since("1.2.0") val numCols: Int,
    @Since("1.2.0") val colPtrs: Array[Int],
    @Since("1.2.0") val rowIndices: Array[Int],
    @Since("1.2.0") val values: Array[Double],
    @Since("1.3.0") override val isTransposed: Boolean) extends Matrix {

  require(values.length == rowIndices.length, "The number of row indices and values don't match! " +
    s"values.length: ${values.length}, rowIndices.length: ${rowIndices.length}")
  if (isTransposed) {
    require(colPtrs.length == numRows + 1,
      s"Expecting ${numRows + 1} colPtrs when numRows = $numRows but got ${colPtrs.length}")
  } else {
    require(colPtrs.length == numCols + 1,
      s"Expecting ${numCols + 1} colPtrs when numCols = $numCols but got ${colPtrs.length}")
  }
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
  @Since("1.2.0")
  def this(
      numRows: Int,
      numCols: Int,
      colPtrs: Array[Int],
      rowIndices: Array[Int],
      values: Array[Double]) = this(numRows, numCols, colPtrs, rowIndices, values, false)

  override def equals(o: Any): Boolean = o match {
    case m: Matrix => asBreeze == m.asBreeze
    case _ => false
  }

  override def hashCode(): Int = asBreeze.hashCode

  private[mllib] def asBreeze: BM[Double] = {
     if (!isTransposed) {
       new BSM[Double](values, numRows, numCols, colPtrs, rowIndices)
     } else {
       val breezeMatrix = new BSM[Double](values, numCols, numRows, colPtrs, rowIndices)
       breezeMatrix.t
     }
  }

  @Since("1.3.0")
  override def apply(i: Int, j: Int): Double = {
    val ind = index(i, j)
    if (ind < 0) 0.0 else values(ind)
  }

  private[mllib] def index(i: Int, j: Int): Int = {
    require(i >= 0 && i < numRows, s"Expected 0 <= i < $numRows, got i = $i.")
    require(j >= 0 && j < numCols, s"Expected 0 <= j < $numCols, got j = $j.")
    if (!isTransposed) {
      Arrays.binarySearch(rowIndices, colPtrs(j), colPtrs(j + 1), i)
    } else {
      Arrays.binarySearch(rowIndices, colPtrs(i), colPtrs(i + 1), j)
    }
  }

  private[mllib] def update(i: Int, j: Int, v: Double): Unit = {
    val ind = index(i, j)
    if (ind < 0) {
      throw new NoSuchElementException("The given row and column indices correspond to a zero " +
        "value. Only non-zero elements in Sparse Matrices can be updated.")
    } else {
      values(ind) = v
    }
  }

  @Since("1.4.0")
  override def copy: SparseMatrix = {
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.clone())
  }

  private[spark] def map(f: Double => Double) =
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.map(f), isTransposed)

  private[mllib] def update(f: Double => Double): SparseMatrix = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = f(values(i))
      i += 1
    }
    this
  }

  @Since("1.3.0")
  override def transpose: SparseMatrix =
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

  /**
   * Generate a `DenseMatrix` from the given `SparseMatrix`. The new matrix will have isTransposed
   * set to false.
   */
  @Since("1.3.0")
  def toDense: DenseMatrix = {
    new DenseMatrix(numRows, numCols, toArray)
  }

  @Since("1.5.0")
  override def numNonzeros: Int = values.count(_ != 0)

  @Since("1.5.0")
  override def numActives: Int = values.length

  @Since("2.0.0")
  override def colIter: Iterator[Vector] = {
    if (isTransposed) {
      val indicesArray = Array.fill(numCols)(MArrayBuilder.make[Int])
      val valuesArray = Array.fill(numCols)(MArrayBuilder.make[Double])
      var i = 0
      while (i < numRows) {
        var k = colPtrs(i)
        val rowEnd = colPtrs(i + 1)
        while (k < rowEnd) {
          val j = rowIndices(k)
          indicesArray(j) += i
          valuesArray(j) += values(k)
          k += 1
        }
        i += 1
      }
      Iterator.tabulate(numCols) { j =>
        val ii = indicesArray(j).result()
        val vv = valuesArray(j).result()
        new SparseVector(numRows, ii, vv)
      }
    } else {
      Iterator.tabulate(numCols) { j =>
        val colStart = colPtrs(j)
        val colEnd = colPtrs(j + 1)
        val ii = rowIndices.slice(colStart, colEnd)
        val vv = values.slice(colStart, colEnd)
        new SparseVector(numRows, ii, vv)
      }
    }
  }

  @Since("2.0.0")
  override def asML: newlinalg.SparseMatrix = {
    new newlinalg.SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
  }
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.SparseMatrix]].
 */
@Since("1.3.0")
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
  @Since("1.3.0")
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
    (sortedEntries.view :+ ((numRows, numCols, 1.0))).foreach { case (i, j, v) =>
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
  @Since("1.3.0")
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
    assert(expected < ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
      "The expected number of nonzeros cannot be greater than Int.MaxValue - 15.")
    val nnz = math.ceil(expected).toInt
    if (density == 0.0) {
      new SparseMatrix(numRows, numCols, new Array[Int](numCols + 1), Array.empty, Array.empty)
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
   * Generate a `SparseMatrix` consisting of `i.i.d`. uniform random numbers. The number of non-zero
   * elements equal the ceiling of `numRows` x `numCols` x `density`
   *
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  @Since("1.3.0")
  def sprand(numRows: Int, numCols: Int, density: Double, rng: Random): SparseMatrix = {
    val mat = genRandMatrix(numRows, numCols, density, rng)
    mat.update(i => rng.nextDouble())
  }

  /**
   * Generate a `SparseMatrix` consisting of `i.i.d`. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  @Since("1.3.0")
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
  @Since("1.3.0")
  def spdiag(vector: Vector): SparseMatrix = {
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

  /**
   * Convert new linalg type to spark.mllib type.  Light copy; only copies references
   */
  @Since("2.0.0")
  def fromML(m: newlinalg.SparseMatrix): SparseMatrix = {
    new SparseMatrix(m.numRows, m.numCols, m.colPtrs, m.rowIndices, m.values, m.isTransposed)
  }
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.Matrix]].
 */
@Since("1.0.0")
object Matrices {

  /**
   * Creates a column-major dense matrix.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param values matrix entries in column major
   */
  @Since("1.0.0")
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
  @Since("1.2.0")
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
        val nsm = if (sm.rowIndices.length > sm.activeSize) {
          // This sparse matrix has trailing zeros.
          // Remove them by compacting the matrix.
          val csm = sm.copy
          csm.compact()
          csm
        } else {
          sm
        }
        new SparseMatrix(nsm.rows, nsm.cols, nsm.colPtrs, nsm.rowIndices, nsm.data)
      case _ =>
        throw new UnsupportedOperationException(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }

  /**
   * Generate a `Matrix` consisting of zeros.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values of zeros
   */
  @Since("1.2.0")
  def zeros(numRows: Int, numCols: Int): Matrix = DenseMatrix.zeros(numRows, numCols)

  /**
   * Generate a `DenseMatrix` consisting of ones.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values of ones
   */
  @Since("1.2.0")
  def ones(numRows: Int, numCols: Int): Matrix = DenseMatrix.ones(numRows, numCols)

  /**
   * Generate a dense Identity Matrix in `Matrix` format.
   * @param n number of rows and columns of the matrix
   * @return `Matrix` with size `n` x `n` and values of ones on the diagonal
   */
  @Since("1.2.0")
  def eye(n: Int): Matrix = DenseMatrix.eye(n)

  /**
   * Generate a sparse Identity Matrix in `Matrix` format.
   * @param n number of rows and columns of the matrix
   * @return `Matrix` with size `n` x `n` and values of ones on the diagonal
   */
  @Since("1.3.0")
  def speye(n: Int): Matrix = SparseMatrix.speye(n)

  /**
   * Generate a `DenseMatrix` consisting of `i.i.d.` uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  @Since("1.2.0")
  def rand(numRows: Int, numCols: Int, rng: Random): Matrix =
    DenseMatrix.rand(numRows, numCols, rng)

  /**
   * Generate a `SparseMatrix` consisting of `i.i.d.` uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  @Since("1.3.0")
  def sprand(numRows: Int, numCols: Int, density: Double, rng: Random): Matrix =
    SparseMatrix.sprand(numRows, numCols, density, rng)

  /**
   * Generate a `DenseMatrix` consisting of `i.i.d.` gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  @Since("1.2.0")
  def randn(numRows: Int, numCols: Int, rng: Random): Matrix =
    DenseMatrix.randn(numRows, numCols, rng)

  /**
   * Generate a `SparseMatrix` consisting of `i.i.d.` gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  @Since("1.3.0")
  def sprandn(numRows: Int, numCols: Int, density: Double, rng: Random): Matrix =
    SparseMatrix.sprandn(numRows, numCols, density, rng)

  /**
   * Generate a diagonal matrix in `Matrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `Matrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
   */
  @Since("1.2.0")
  def diag(vector: Vector): Matrix = DenseMatrix.diag(vector)

  /**
   * Horizontally concatenate a sequence of matrices. The returned matrix will be in the format
   * the matrices are supplied in. Supplying a mix of dense and sparse matrices will result in
   * a sparse matrix. If the Array is empty, an empty `DenseMatrix` will be returned.
   * @param matrices array of matrices
   * @return a single `Matrix` composed of the matrices that were horizontally concatenated
   */
  @Since("1.3.0")
  def horzcat(matrices: Array[Matrix]): Matrix = {
    if (matrices.isEmpty) {
      return new DenseMatrix(0, 0, Array.emptyDoubleArray)
    } else if (matrices.length == 1) {
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
      val entries: Array[(Int, Int, Double)] = matrices.flatMap { mat: Matrix =>
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
            data.toImmutableArraySeq
          case dnMat: DenseMatrix =>
            val data = new ArrayBuffer[(Int, Int, Double)]()
            dnMat.foreachActive { (i, j, v) =>
              if (v != 0.0) {
                data += Tuple3(i, j + startCol, v)
              }
            }
            startCol += nCols
            data.toSeq
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
  @Since("1.3.0")
  def vertcat(matrices: Array[Matrix]): Matrix = {
    if (matrices.isEmpty) {
      return new DenseMatrix(0, 0, Array.emptyDoubleArray)
    } else if (matrices.length == 1) {
      return matrices(0)
    }
    val numCols = matrices(0).numCols
    var hasSparse = false
    var numRows = 0
    matrices.foreach { mat =>
      require(numCols == mat.numCols, "The number of columns of the matrices in this sequence, " +
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
      val entries: Array[(Int, Int, Double)] = matrices.flatMap { mat: Matrix =>
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
            data.toImmutableArraySeq
          case dnMat: DenseMatrix =>
            val data = new ArrayBuffer[(Int, Int, Double)]()
            dnMat.foreachActive { (i, j, v) =>
              if (v != 0.0) {
                data += Tuple3(i + startRow, j, v)
              }
            }
            startRow += nRows
            data.toSeq
        }
      }
      SparseMatrix.fromCOO(numRows, numCols, entries)
    }
  }

  /**
   * Convert new linalg type to spark.mllib type.  Light copy; only copies references
   */
  @Since("2.0.0")
  def fromML(m: newlinalg.Matrix): Matrix = m match {
    case dm: newlinalg.DenseMatrix =>
      DenseMatrix.fromML(dm)
    case sm: newlinalg.SparseMatrix =>
      SparseMatrix.fromML(sm)
  }
}

/**
 * Implicit methods available in Scala for converting [[org.apache.spark.mllib.linalg.Matrix]] to
 * [[org.apache.spark.ml.linalg.Matrix]] and vice versa.
 */
private[spark] object MatrixImplicits {

  implicit def mllibMatrixToMLMatrix(m: Matrix): newlinalg.Matrix = m.asML

  implicit def mllibDenseMatrixToMLDenseMatrix(m: DenseMatrix): newlinalg.DenseMatrix = m.asML

  implicit def mllibSparseMatrixToMLSparseMatrix(m: SparseMatrix): newlinalg.SparseMatrix = m.asML

  implicit def mlMatrixToMLlibMatrix(m: newlinalg.Matrix): Matrix = Matrices.fromML(m)

  implicit def mlDenseMatrixToMLlibDenseMatrix(m: newlinalg.DenseMatrix): DenseMatrix =
    Matrices.fromML(m).asInstanceOf[DenseMatrix]

  implicit def mlSparseMatrixToMLlibSparseMatrix(m: newlinalg.SparseMatrix): SparseMatrix =
    Matrices.fromML(m).asInstanceOf[SparseMatrix]
}
