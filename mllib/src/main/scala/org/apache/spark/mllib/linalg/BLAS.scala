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

import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}

import org.apache.spark.Logging

/**
 * BLAS routines for MLlib's vectors and matrices.
 */
private[spark] object BLAS extends Serializable with Logging {

  @transient private var _f2jBLAS: NetlibBLAS = _
  @transient private var _nativeBLAS: NetlibBLAS = _

  // For level-1 routines, we use Java implementation.
  private def f2jBLAS: NetlibBLAS = {
    if (_f2jBLAS == null) {
      _f2jBLAS = new F2jBLAS
    }
    _f2jBLAS
  }

  /**
   * y += a * x
   */
  def axpy(a: Double, x: Vector, y: Vector): Unit = {
    require(x.size == y.size)
    y match {
      case dy: DenseVector =>
        x match {
          case sx: SparseVector =>
            axpy(a, sx, dy)
          case dx: DenseVector =>
            axpy(a, dx, dy)
          case _ =>
            throw new UnsupportedOperationException(
              s"axpy doesn't support x type ${x.getClass}.")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"axpy only supports adding to a dense vector but got type ${y.getClass}.")
    }
  }

  /**
   * y += a * x
   */
  private def axpy(a: Double, x: DenseVector, y: DenseVector): Unit = {
    val n = x.size
    f2jBLAS.daxpy(n, a, x.values, 1, y.values, 1)
  }

  /**
   * y += a * x
   */
  private def axpy(a: Double, x: SparseVector, y: DenseVector): Unit = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    if (a == 1.0) {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += xValues(k)
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += a * xValues(k)
        k += 1
      }
    }
  }

  /**
   * dot(x, y)
   */
  def dot(x: Vector, y: Vector): Double = {
    require(x.size == y.size,
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
      " x.size = " + x.size + ", y.size = " + y.size)
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
      case (sx: SparseVector, dy: DenseVector) =>
        dot(sx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        dot(sy, dx)
      case (sx: SparseVector, sy: SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  /**
   * dot(x, y)
   */
  private def dot(x: DenseVector, y: DenseVector): Double = {
    val n = x.size
    f2jBLAS.ddot(n, x.values, 1, y.values, 1)
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.size
    val nnzy = yIndices.size

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }

  /**
   * y = x
   */
  def copy(x: Vector, y: Vector): Unit = {
    val n = y.size
    require(x.size == n)
    y match {
      case dy: DenseVector =>
        x match {
          case sx: SparseVector =>
            val sxIndices = sx.indices
            val sxValues = sx.values
            val dyValues = dy.values
            val nnz = sxIndices.size

            var i = 0
            var k = 0
            while (k < nnz) {
              val j = sxIndices(k)
              while (i < j) {
                dyValues(i) = 0.0
                i += 1
              }
              dyValues(i) = sxValues(k)
              i += 1
              k += 1
            }
            while (i < n) {
              dyValues(i) = 0.0
              i += 1
            }
          case dx: DenseVector =>
            Array.copy(dx.values, 0, dy.values, 0, n)
        }
      case _ =>
        throw new IllegalArgumentException(s"y must be dense in copy but got ${y.getClass}")
    }
  }

  /**
   * x = a * x
   */
  def scal(a: Double, x: Vector): Unit = {
    x match {
      case sx: SparseVector =>
        f2jBLAS.dscal(sx.values.size, a, sx.values, 1)
      case dx: DenseVector =>
        f2jBLAS.dscal(dx.values.size, a, dx.values, 1)
      case _ =>
        throw new IllegalArgumentException(s"scal doesn't support vector type ${x.getClass}.")
    }
  }

  // For level-3 routines, we use the native BLAS.
  private def nativeBLAS: NetlibBLAS = {
    if (_nativeBLAS == null) {
      _nativeBLAS = NativeBLAS
    }
    _nativeBLAS
  }

  /**
   * C := alpha * A * B + beta * C
   * @param transA whether to use the transpose of matrix A (true), or A itself (false).
   * @param transB whether to use the transpose of matrix B (true), or B itself (false).
   * @param alpha a scalar to scale the multiplication A * B.
   * @param A the matrix A that will be left multiplied to B. Size of m x k.
   * @param B the matrix B that will be left multiplied by A. Size of k x n.
   * @param beta a scalar that can be used to scale matrix C.
   * @param C the resulting matrix C. Size of m x n.
   */
  def gemm(
      transA: Boolean,
      transB: Boolean,
      alpha: Double,
      A: Matrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    if (alpha == 0.0) {
      logDebug("gemm: alpha is equal to 0. Returning C.")
    } else {
      A match {
        case sparse: SparseMatrix =>
          gemm(transA, transB, alpha, sparse, B, beta, C)
        case dense: DenseMatrix =>
          gemm(transA, transB, alpha, dense, B, beta, C)
        case _ =>
          throw new IllegalArgumentException(s"gemm doesn't support matrix type ${A.getClass}.")
      }
    }
  }

  /**
   * C := alpha * A * B + beta * C
   *
   * @param alpha a scalar to scale the multiplication A * B.
   * @param A the matrix A that will be left multiplied to B. Size of m x k.
   * @param B the matrix B that will be left multiplied by A. Size of k x n.
   * @param beta a scalar that can be used to scale matrix C.
   * @param C the resulting matrix C. Size of m x n.
   */
  def gemm(
      alpha: Double,
      A: Matrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    gemm(false, false, alpha, A, B, beta, C)
  }

  /**
   * C := alpha * A * B + beta * C
   * For `DenseMatrix` A.
   */
  private def gemm(
      transA: Boolean,
      transB: Boolean,
      alpha: Double,
      A: DenseMatrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    val mA: Int = if (!transA) A.numRows else A.numCols
    val nB: Int = if (!transB) B.numCols else B.numRows
    val kA: Int = if (!transA) A.numCols else A.numRows
    val kB: Int = if (!transB) B.numRows else B.numCols
    val tAstr = if (!transA) "N" else "T"
    val tBstr = if (!transB) "N" else "T"

    require(kA == kB, s"The columns of A don't match the rows of B. A: $kA, B: $kB")
    require(mA == C.numRows, s"The rows of C don't match the rows of A. C: ${C.numRows}, A: $mA")
    require(nB == C.numCols,
      s"The columns of C don't match the columns of B. C: ${C.numCols}, A: $nB")

    nativeBLAS.dgemm(tAstr, tBstr, mA, nB, kA, alpha, A.values, A.numRows, B.values, B.numRows,
      beta, C.values, C.numRows)
  }

  /**
   * C := alpha * A * B + beta * C
   * For `SparseMatrix` A.
   */
  private def gemm(
      transA: Boolean,
      transB: Boolean,
      alpha: Double,
      A: SparseMatrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    val mA: Int = if (!transA) A.numRows else A.numCols
    val nB: Int = if (!transB) B.numCols else B.numRows
    val kA: Int = if (!transA) A.numCols else A.numRows
    val kB: Int = if (!transB) B.numRows else B.numCols

    require(kA == kB, s"The columns of A don't match the rows of B. A: $kA, B: $kB")
    require(mA == C.numRows, s"The rows of C don't match the rows of A. C: ${C.numRows}, A: $mA")
    require(nB == C.numCols,
      s"The columns of C don't match the columns of B. C: ${C.numCols}, A: $nB")

    val Avals = A.values
    val Bvals = B.values
    val Cvals = C.values
    val Arows = if (!transA) A.rowIndices else A.colPtrs
    val Acols = if (!transA) A.colPtrs else A.rowIndices

    // Slicing is easy in this case. This is the optimal multiplication setting for sparse matrices
    if (transA){
      var colCounterForB = 0
      if (!transB) { // Expensive to put the check inside the loop
        while (colCounterForB < nB) {
          var rowCounterForA = 0
          val Cstart = colCounterForB * mA
          val Bstart = colCounterForB * kA
          while (rowCounterForA < mA) {
            var i = Arows(rowCounterForA)
            val indEnd = Arows(rowCounterForA + 1)
            var sum = 0.0
            while (i < indEnd) {
              sum += Avals(i) * Bvals(Bstart + Acols(i))
              i += 1
            }
            val Cindex = Cstart + rowCounterForA
            Cvals(Cindex) = beta * Cvals(Cindex) + sum * alpha
            rowCounterForA += 1
          }
          colCounterForB += 1
        }
      } else {
        while (colCounterForB < nB) {
          var rowCounter = 0
          val Cstart = colCounterForB * mA
          while (rowCounter < mA) {
            var i = Arows(rowCounter)
            val indEnd = Arows(rowCounter + 1)
            var sum = 0.0
            while (i < indEnd) {
              sum += Avals(i) * B(colCounterForB, Acols(i))
              i += 1
            }
            val Cindex = Cstart + rowCounter
            Cvals(Cindex) = beta * Cvals(Cindex) + sum * alpha
            rowCounter += 1
          }
          colCounterForB += 1
        }
      }
    } else {
      // Scale matrix first if `beta` is not equal to 0.0
      if (beta != 0.0) {
        f2jBLAS.dscal(C.values.length, beta, C.values, 1)
      }
      // Perform matrix multiplication and add to C. The rows of A are multiplied by the columns of
      // B, and added to C.
      var colCounterForB = 0 // the column to be updated in C
      if (!transB) { // Expensive to put the check inside the loop
        while (colCounterForB < nB) {
          var colCounterForA = 0 // The column of A to multiply with the row of B
          val Bstart = colCounterForB * kB
          val Cstart = colCounterForB * mA
          while (colCounterForA < kA) {
            var i = Acols(colCounterForA)
            val indEnd = Acols(colCounterForA + 1)
            val Bval = Bvals(Bstart + colCounterForA) * alpha
            while (i < indEnd) {
              Cvals(Cstart + Arows(i)) += Avals(i) * Bval
              i += 1
            }
            colCounterForA += 1
          }
          colCounterForB += 1
        }
      } else {
        while (colCounterForB < nB) {
          var colCounterForA = 0 // The column of A to multiply with the row of B
          val Cstart = colCounterForB * mA
          while (colCounterForA < kA) {
            var i = Acols(colCounterForA)
            val indEnd = Acols(colCounterForA + 1)
            val Bval = B(colCounterForB, colCounterForA) * alpha
            while (i < indEnd) {
              Cvals(Cstart + Arows(i)) += Avals(i) * Bval
              i += 1
            }
            colCounterForA += 1
          }
          colCounterForB += 1
        }
      }
    }
  }

  /**
   * y := alpha * A * x + beta * y
   * @param trans whether to use the transpose of matrix A (true), or A itself (false).
   * @param alpha a scalar to scale the multiplication A * x.
   * @param A the matrix A that will be left multiplied to x. Size of m x n.
   * @param x the vector x that will be left multiplied by A. Size of n x 1.
   * @param beta a scalar that can be used to scale vector y.
   * @param y the resulting vector y. Size of m x 1.
   */
  def gemv(
      trans: Boolean,
      alpha: Double,
      A: Matrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector): Unit = {

    val mA: Int = if (!trans) A.numRows else A.numCols
    val nx: Int = x.size
    val nA: Int = if (!trans) A.numCols else A.numRows

    require(nA == nx, s"The columns of A don't match the number of elements of x. A: $nA, x: $nx")
    require(mA == y.size,
      s"The rows of A don't match the number of elements of y. A: $mA, y:${y.size}}")
    if (alpha == 0.0) {
      logDebug("gemv: alpha is equal to 0. Returning y.")
    } else {
      A match {
        case sparse: SparseMatrix =>
          gemv(trans, alpha, sparse, x, beta, y)
        case dense: DenseMatrix =>
          gemv(trans, alpha, dense, x, beta, y)
        case _ =>
          throw new IllegalArgumentException(s"gemv doesn't support matrix type ${A.getClass}.")
      }
    }
  }

  /**
   * y := alpha * A * x + beta * y
   *
   * @param alpha a scalar to scale the multiplication A * x.
   * @param A the matrix A that will be left multiplied to x. Size of m x n.
   * @param x the vector x that will be left multiplied by A. Size of n x 1.
   * @param beta a scalar that can be used to scale vector y.
   * @param y the resulting vector y. Size of m x 1.
   */
  def gemv(
      alpha: Double,
      A: Matrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector): Unit = {
    gemv(false, alpha, A, x, beta, y)
  }

  /**
   * y := alpha * A * x + beta * y
   * For `DenseMatrix` A.
   */
  private def gemv(
      trans: Boolean,
      alpha: Double,
      A: DenseMatrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector): Unit =  {
    val tStrA = if (!trans) "N" else "T"
    nativeBLAS.dgemv(tStrA, A.numRows, A.numCols, alpha, A.values, A.numRows, x.values, 1, beta,
      y.values, 1)
  }

  /**
   * y := alpha * A * x + beta * y
   * For `SparseMatrix` A.
   */
  private def gemv(
      trans: Boolean,
      alpha: Double,
      A: SparseMatrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector): Unit =  {

    val xValues = x.values
    val yValues = y.values

    val mA: Int = if (!trans) A.numRows else A.numCols
    val nA: Int = if (!trans) A.numCols else A.numRows

    val Avals = A.values
    val Arows = if (!trans) A.rowIndices else A.colPtrs
    val Acols = if (!trans) A.colPtrs else A.rowIndices
    // Slicing is easy in this case. This is the optimal multiplication setting for sparse matrices
    if (trans) {
      var rowCounter = 0
      while (rowCounter < mA) {
        var i = Arows(rowCounter)
        val indEnd = Arows(rowCounter + 1)
        var sum = 0.0
        while (i < indEnd) {
          sum += Avals(i) * xValues(Acols(i))
          i += 1
        }
        yValues(rowCounter) = beta * yValues(rowCounter) + sum * alpha
        rowCounter += 1
      }
    } else {
      // Scale vector first if `beta` is not equal to 0.0
      if (beta != 0.0) {
        scal(beta, y)
      }
      // Perform matrix-vector multiplication and add to y
      var colCounterForA = 0
      while (colCounterForA < nA) {
        var i = Acols(colCounterForA)
        val indEnd = Acols(colCounterForA + 1)
        val xVal = xValues(colCounterForA) * alpha
        while (i < indEnd) {
          val rowIndex = Arows(i)
          yValues(rowIndex) += Avals(i) * xVal
          i += 1
        }
        colCounterForA += 1
      }
    }
  }
}
