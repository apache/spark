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

package org.apache.spark.ml.linalg

import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}

/**
 * BLAS routines for MLlib's vectors and matrices.
 */
private[spark] object BLAS extends Serializable {

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
    val nnz = xIndices.length

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

  /** Y += a * x */
  private[spark] def axpy(a: Double, X: DenseMatrix, Y: DenseMatrix): Unit = {
    require(X.numRows == Y.numRows && X.numCols == Y.numCols, "Dimension mismatch: " +
      s"size(X) = ${(X.numRows, X.numCols)} but size(Y) = ${(Y.numRows, Y.numCols)}.")
    f2jBLAS.daxpy(X.numRows * X.numCols, a, X.values, 1, Y.values, 1)
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
    val nnz = xIndices.length

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
    val nnzx = xIndices.length
    val nnzy = yIndices.length

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
            val nnz = sxIndices.length

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
        f2jBLAS.dscal(sx.values.length, a, sx.values, 1)
      case dx: DenseVector =>
        f2jBLAS.dscal(dx.values.length, a, dx.values, 1)
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
   * Adds alpha * x * x.t to a matrix in-place. This is the same as BLAS's ?SPR.
   *
   * @param U the upper triangular part of the matrix in a [[DenseVector]](column major)
   */
  def spr(alpha: Double, v: Vector, U: DenseVector): Unit = {
    spr(alpha, v, U.values)
  }

  /**
   * y := alpha*A*x + beta*y
   *
   * @param A The upper triangular part of A in a [[DenseVector]] (column major).
   * @param x The [[DenseVector]] transformed by A.
   * @param y The [[DenseVector]] to be modified in place.
   */
  def dspmv(
      n: Int,
      alpha: Double,
      A: DenseVector,
      x: DenseVector,
      beta: Double,
      y: DenseVector): Unit = {
    f2jBLAS.dspmv("U", n, alpha, A.values, x.values, 1, beta, y.values, 1)
  }

  /**
   * Adds alpha * x * x.t to a matrix in-place. This is the same as BLAS's ?SPR.
   *
   * @param U the upper triangular part of the matrix packed in an array (column major)
   */
  def spr(alpha: Double, v: Vector, U: Array[Double]): Unit = {
    val n = v.size
    v match {
      case DenseVector(values) =>
        NativeBLAS.dspr("U", n, alpha, values, 1, U)
      case SparseVector(size, indices, values) =>
        val nnz = indices.length
        var colStartIdx = 0
        var prevCol = 0
        var col = 0
        var j = 0
        var i = 0
        var av = 0.0
        while (j < nnz) {
          col = indices(j)
          // Skip empty columns.
          colStartIdx += (col - prevCol) * (col + prevCol + 1) / 2
          col = indices(j)
          av = alpha * values(j)
          i = 0
          while (i <= j) {
            U(colStartIdx + indices(i)) += av * values(i)
            i += 1
          }
          j += 1
          prevCol = col
        }
    }
  }

  /**
   * A := alpha * x * x^T^ + A
   * @param alpha a real scalar that will be multiplied to x * x^T^.
   * @param x the vector x that contains the n elements.
   * @param A the symmetric matrix A. Size of n x n.
   */
  def syr(alpha: Double, x: Vector, A: DenseMatrix) {
    val mA = A.numRows
    val nA = A.numCols
    require(mA == nA, s"A is not a square matrix (and hence is not symmetric). A: $mA x $nA")
    require(mA == x.size, s"The size of x doesn't match the rank of A. A: $mA x $nA, x: ${x.size}")

    x match {
      case dv: DenseVector => syr(alpha, dv, A)
      case sv: SparseVector => syr(alpha, sv, A)
      case _ =>
        throw new IllegalArgumentException(s"syr doesn't support vector type ${x.getClass}.")
    }
  }

  private def syr(alpha: Double, x: DenseVector, A: DenseMatrix) {
    val nA = A.numRows
    val mA = A.numCols

    nativeBLAS.dsyr("U", x.size, alpha, x.values, 1, A.values, nA)

    // Fill lower triangular part of A
    var i = 0
    while (i < mA) {
      var j = i + 1
      while (j < nA) {
        A(j, i) = A(i, j)
        j += 1
      }
      i += 1
    }
  }

  private def syr(alpha: Double, x: SparseVector, A: DenseMatrix) {
    val mA = A.numCols
    val xIndices = x.indices
    val xValues = x.values
    val nnz = xValues.length
    val Avalues = A.values

    var i = 0
    while (i < nnz) {
      val multiplier = alpha * xValues(i)
      val offset = xIndices(i) * mA
      var j = 0
      while (j < nnz) {
        Avalues(xIndices(j) + offset) += multiplier * xValues(j)
        j += 1
      }
      i += 1
    }
  }

  /**
   * C := alpha * A * B + beta * C
   * @param alpha a scalar to scale the multiplication A * B.
   * @param A the matrix A that will be left multiplied to B. Size of m x k.
   * @param B the matrix B that will be left multiplied by A. Size of k x n.
   * @param beta a scalar that can be used to scale matrix C.
   * @param C the resulting matrix C. Size of m x n. C.isTransposed must be false.
   */
  def gemm(
      alpha: Double,
      A: Matrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    require(!C.isTransposed,
      "The matrix C cannot be the product of a transpose() call. C.isTransposed must be false.")
    if (alpha == 0.0 && beta == 1.0) {
      // gemm: alpha is equal to 0 and beta is equal to 1. Returning C.
      return
    } else if (alpha == 0.0) {
      f2jBLAS.dscal(C.values.length, beta, C.values, 1)
    } else {
      A match {
        case sparse: SparseMatrix => gemm(alpha, sparse, B, beta, C)
        case dense: DenseMatrix => gemm(alpha, dense, B, beta, C)
        case _ =>
          throw new IllegalArgumentException(s"gemm doesn't support matrix type ${A.getClass}.")
      }
    }
  }

  /**
   * C := alpha * A * B + beta * C
   * For `DenseMatrix` A.
   */
  private def gemm(
      alpha: Double,
      A: DenseMatrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    val tAstr = if (A.isTransposed) "T" else "N"
    val tBstr = if (B.isTransposed) "T" else "N"
    val lda = if (!A.isTransposed) A.numRows else A.numCols
    val ldb = if (!B.isTransposed) B.numRows else B.numCols

    require(A.numCols == B.numRows,
      s"The columns of A don't match the rows of B. A: ${A.numCols}, B: ${B.numRows}")
    require(A.numRows == C.numRows,
      s"The rows of C don't match the rows of A. C: ${C.numRows}, A: ${A.numRows}")
    require(B.numCols == C.numCols,
      s"The columns of C don't match the columns of B. C: ${C.numCols}, A: ${B.numCols}")
    nativeBLAS.dgemm(tAstr, tBstr, A.numRows, B.numCols, A.numCols, alpha, A.values, lda,
      B.values, ldb, beta, C.values, C.numRows)
  }

  /**
   * C := alpha * A * B + beta * C
   * For `SparseMatrix` A.
   */
  private def gemm(
      alpha: Double,
      A: SparseMatrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    val mA: Int = A.numRows
    val nB: Int = B.numCols
    val kA: Int = A.numCols
    val kB: Int = B.numRows

    require(kA == kB, s"The columns of A don't match the rows of B. A: $kA, B: $kB")
    require(mA == C.numRows, s"The rows of C don't match the rows of A. C: ${C.numRows}, A: $mA")
    require(nB == C.numCols,
      s"The columns of C don't match the columns of B. C: ${C.numCols}, A: $nB")

    val Avals = A.values
    val Bvals = B.values
    val Cvals = C.values
    val ArowIndices = A.rowIndices
    val AcolPtrs = A.colPtrs

    // Slicing is easy in this case. This is the optimal multiplication setting for sparse matrices
    if (A.isTransposed) {
      var colCounterForB = 0
      if (!B.isTransposed) { // Expensive to put the check inside the loop
        while (colCounterForB < nB) {
          var rowCounterForA = 0
          val Cstart = colCounterForB * mA
          val Bstart = colCounterForB * kA
          while (rowCounterForA < mA) {
            var i = AcolPtrs(rowCounterForA)
            val indEnd = AcolPtrs(rowCounterForA + 1)
            var sum = 0.0
            while (i < indEnd) {
              sum += Avals(i) * Bvals(Bstart + ArowIndices(i))
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
          var rowCounterForA = 0
          val Cstart = colCounterForB * mA
          while (rowCounterForA < mA) {
            var i = AcolPtrs(rowCounterForA)
            val indEnd = AcolPtrs(rowCounterForA + 1)
            var sum = 0.0
            while (i < indEnd) {
              sum += Avals(i) * B(ArowIndices(i), colCounterForB)
              i += 1
            }
            val Cindex = Cstart + rowCounterForA
            Cvals(Cindex) = beta * Cvals(Cindex) + sum * alpha
            rowCounterForA += 1
          }
          colCounterForB += 1
        }
      }
    } else {
      // Scale matrix first if `beta` is not equal to 1.0
      if (beta != 1.0) {
        f2jBLAS.dscal(C.values.length, beta, C.values, 1)
      }
      // Perform matrix multiplication and add to C. The rows of A are multiplied by the columns of
      // B, and added to C.
      var colCounterForB = 0 // the column to be updated in C
      if (!B.isTransposed) { // Expensive to put the check inside the loop
        while (colCounterForB < nB) {
          var colCounterForA = 0 // The column of A to multiply with the row of B
          val Bstart = colCounterForB * kB
          val Cstart = colCounterForB * mA
          while (colCounterForA < kA) {
            var i = AcolPtrs(colCounterForA)
            val indEnd = AcolPtrs(colCounterForA + 1)
            val Bval = Bvals(Bstart + colCounterForA) * alpha
            while (i < indEnd) {
              Cvals(Cstart + ArowIndices(i)) += Avals(i) * Bval
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
            var i = AcolPtrs(colCounterForA)
            val indEnd = AcolPtrs(colCounterForA + 1)
            val Bval = B(colCounterForA, colCounterForB) * alpha
            while (i < indEnd) {
              Cvals(Cstart + ArowIndices(i)) += Avals(i) * Bval
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
   * @param alpha a scalar to scale the multiplication A * x.
   * @param A the matrix A that will be left multiplied to x. Size of m x n.
   * @param x the vector x that will be left multiplied by A. Size of n x 1.
   * @param beta a scalar that can be used to scale vector y.
   * @param y the resulting vector y. Size of m x 1.
   */
  def gemv(
      alpha: Double,
      A: Matrix,
      x: Vector,
      beta: Double,
      y: DenseVector): Unit = {
    require(A.numCols == x.size,
      s"The columns of A don't match the number of elements of x. A: ${A.numCols}, x: ${x.size}")
    require(A.numRows == y.size,
      s"The rows of A don't match the number of elements of y. A: ${A.numRows}, y:${y.size}")
    if (alpha == 0.0 && beta == 1.0) {
      // gemv: alpha is equal to 0 and beta is equal to 1. Returning y.
      return
    } else if (alpha == 0.0) {
      scal(beta, y)
    } else {
      (A, x) match {
        case (smA: SparseMatrix, dvx: DenseVector) =>
          gemv(alpha, smA, dvx, beta, y)
        case (smA: SparseMatrix, svx: SparseVector) =>
          gemv(alpha, smA, svx, beta, y)
        case (dmA: DenseMatrix, dvx: DenseVector) =>
          gemv(alpha, dmA, dvx, beta, y)
        case (dmA: DenseMatrix, svx: SparseVector) =>
          gemv(alpha, dmA, svx, beta, y)
        case _ =>
          throw new IllegalArgumentException(s"gemv doesn't support running on matrix type " +
            s"${A.getClass} and vector type ${x.getClass}.")
      }
    }
  }

  /**
   * y := alpha * A * x + beta * y
   * For `DenseMatrix` A and `DenseVector` x.
   */
  private def gemv(
      alpha: Double,
      A: DenseMatrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector): Unit = {
    val tStrA = if (A.isTransposed) "T" else "N"
    val mA = if (!A.isTransposed) A.numRows else A.numCols
    val nA = if (!A.isTransposed) A.numCols else A.numRows
    nativeBLAS.dgemv(tStrA, mA, nA, alpha, A.values, mA, x.values, 1, beta,
      y.values, 1)
  }

  /**
   * y := alpha * A * x + beta * y
   * For `DenseMatrix` A and `SparseVector` x.
   */
  private def gemv(
      alpha: Double,
      A: DenseMatrix,
      x: SparseVector,
      beta: Double,
      y: DenseVector): Unit = {
    val mA: Int = A.numRows
    val nA: Int = A.numCols

    val Avals = A.values

    val xIndices = x.indices
    val xNnz = xIndices.length
    val xValues = x.values
    val yValues = y.values

    if (A.isTransposed) {
      var rowCounterForA = 0
      while (rowCounterForA < mA) {
        var sum = 0.0
        var k = 0
        while (k < xNnz) {
          sum += xValues(k) * Avals(xIndices(k) + rowCounterForA * nA)
          k += 1
        }
        yValues(rowCounterForA) = sum * alpha + beta * yValues(rowCounterForA)
        rowCounterForA += 1
      }
    } else {
      var rowCounterForA = 0
      while (rowCounterForA < mA) {
        var sum = 0.0
        var k = 0
        while (k < xNnz) {
          sum += xValues(k) * Avals(xIndices(k) * mA + rowCounterForA)
          k += 1
        }
        yValues(rowCounterForA) = sum * alpha + beta * yValues(rowCounterForA)
        rowCounterForA += 1
      }
    }
  }

  /**
   * y := alpha * A * x + beta * y
   * For `SparseMatrix` A and `SparseVector` x.
   */
  private def gemv(
      alpha: Double,
      A: SparseMatrix,
      x: SparseVector,
      beta: Double,
      y: DenseVector): Unit = {
    val xValues = x.values
    val xIndices = x.indices
    val xNnz = xIndices.length

    val yValues = y.values

    val mA: Int = A.numRows
    val nA: Int = A.numCols

    val Avals = A.values
    val Arows = if (!A.isTransposed) A.rowIndices else A.colPtrs
    val Acols = if (!A.isTransposed) A.colPtrs else A.rowIndices

    if (A.isTransposed) {
      var rowCounter = 0
      while (rowCounter < mA) {
        var i = Arows(rowCounter)
        val indEnd = Arows(rowCounter + 1)
        var sum = 0.0
        var k = 0
        while (i < indEnd && k < xNnz) {
          if (xIndices(k) == Acols(i)) {
            sum += Avals(i) * xValues(k)
            k += 1
            i += 1
          } else if (xIndices(k) < Acols(i)) {
            k += 1
          } else {
            i += 1
          }
        }
        yValues(rowCounter) = sum * alpha + beta * yValues(rowCounter)
        rowCounter += 1
      }
    } else {
      if (beta != 1.0) scal(beta, y)

      var colCounterForA = 0
      var k = 0
      while (colCounterForA < nA && k < xNnz) {
        if (xIndices(k) == colCounterForA) {
          var i = Acols(colCounterForA)
          val indEnd = Acols(colCounterForA + 1)

          val xTemp = xValues(k) * alpha
          while (i < indEnd) {
            val rowIndex = Arows(i)
            yValues(Arows(i)) += Avals(i) * xTemp
            i += 1
          }
          k += 1
        }
        colCounterForA += 1
      }
    }
  }

  /**
   * y := alpha * A * x + beta * y
   * For `SparseMatrix` A and `DenseVector` x.
   */
  private def gemv(
      alpha: Double,
      A: SparseMatrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector): Unit = {
    val xValues = x.values
    val yValues = y.values
    val mA: Int = A.numRows
    val nA: Int = A.numCols

    val Avals = A.values
    val Arows = if (!A.isTransposed) A.rowIndices else A.colPtrs
    val Acols = if (!A.isTransposed) A.colPtrs else A.rowIndices
    // Slicing is easy in this case. This is the optimal multiplication setting for sparse matrices
    if (A.isTransposed) {
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
      if (beta != 1.0) scal(beta, y)
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
