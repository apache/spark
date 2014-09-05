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

import org.apache.commons.lang.StringEscapeUtils.escapeJava

/**
 * BLAS routines for MLlib's vectors and matrices.
 */
private[mllib] object BLAS extends Serializable {

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
    val nnz = x.indices.size
    if (a == 1.0) {
      var k = 0
      while (k < nnz) {
        y.values(x.indices(k)) += x.values(k)
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        y.values(x.indices(k)) += a * x.values(k)
        k += 1
      }
    }
  }

  /**
   * dot(x, y)
   */
  def dot(x: Vector, y: Vector): Double = {
    require(x.size == y.size)
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
    val nnz = x.indices.size
    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += x.values(k) * y.values(x.indices(k))
      k += 1
    }
    sum
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    var kx = 0
    val nnzx = x.indices.size
    var ky = 0
    val nnzy = y.indices.size
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = x.indices(kx)
      while (ky < nnzy && y.indices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && y.indices(ky) == ix) {
        sum += x.values(kx) * y.values(ky)
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
            var i = 0
            var k = 0
            val nnz = sx.indices.size
            while (k < nnz) {
              val j = sx.indices(k)
              while (i < j) {
                dy.values(i) = 0.0
                i += 1
              }
              dy.values(i) = sx.values(k)
              i += 1
              k += 1
            }
            while (i < n) {
              dy.values(i) = 0.0
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
   * @param transA specify whether to use matrix A, or the transpose of matrix A. Should be "N" or
   *               "n" to use A, and "T" or "t" to use the transpose of A.
   * @param transB specify whether to use matrix B, or the transpose of matrix B. Should be "N" or
   *               "n" to use B, and "T" or "t" to use the transpose of B.
   * @param alpha a scalar to scale the multiplication A * B.
   * @param A the matrix A that will be left multiplied to B. Size of m x k.
   * @param B the matrix B that will be left multiplied by A. Size of k x n.
   * @param beta a scalar that can be used to scale matrix C.
   * @param C the resulting matrix C. Size of m x n.
   */
  def gemm(
            transA: String,
            transB: String,
            alpha: Double,
            A: Matrix,
            B: DenseMatrix,
            beta: Double,
            C: DenseMatrix) {

    var mA: Int = A.numRows
    var nB: Int = B.numCols
    var kA: Int = A.numCols
    var kB: Int = B.numRows

    if (transA == "T" || transA=="t"){
      mA = A.numCols
      kA = A.numRows
    }
    require(transA == "T" || transA == "t" || transA == "N" || transA == "n",
      s"Invalid argument used for transA: $transA. " +
        escapeJava("Must be \"N\", \"n\", \"T\", or \"t\""))
    if (transB == "T" || transB=="t"){
      nB = B.numRows
      kB = B.numCols
    }
    require(transB == "T" || transB == "t" || transB == "N" || transB == "n",
      s"Invalid argument used for transB: $transB. " +
        escapeJava("Must be \"N\", \"n\", \"T\", or \"t\""))

    require(kA == kB, s"The columns of A don't match the rows of B. A: $kA, B: $kB")
    require(mA == C.numRows, s"The rows of C don't match the rows of A. C: ${C.numRows}, A: $mA")
    require(nB == C.numCols,
      s"The columns of C don't match the columns of B. C: ${C.numCols}, A: $nB")

    A match {
      case sparse: SparseMatrix =>
        gemm(transA, transB, alpha, sparse, B, beta, C, mA, kA, nB)
      case dense: DenseMatrix =>
        gemm(transA, transB, alpha, dense, B, beta, C, mA, kA, nB)
      case _ =>
        throw new IllegalArgumentException(s"gemm doesn't support matrix type ${A.getClass}.")
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
            C: DenseMatrix) {

    gemm("N", "N", alpha, A, B, beta, C)
  }

  /**
   * C := alpha * A * B
   *
   * @param transA specify whether to use matrix A, or the transpose of matrix A. Should be "N" or
   *               "n" to use A, and "T" or "t" to use the transpose of A.
   * @param transB specify whether to use matrix B, or the transpose of matrix B. Should be "N" or
   *               "n" to use B, and "T" or "t" to use the transpose of B.
   * @param alpha a scalar to scale the multiplication A * B.
   * @param A the matrix A that will be left multiplied to B. Size of m x k.
   * @param B the matrix B that will be left multiplied by A. Size of k x n.
   *
   * @return The resulting matrix C. Size of m x n.
   */
  def gemm(transA: String,
            transB: String,
            alpha: Double,
            A: Matrix,
            B: DenseMatrix) : DenseMatrix = {

    var mA: Int = A.numRows
    var nB: Int = B.numCols
    var kA: Int = A.numCols
    var kB: Int = B.numRows

    if (transA == "T" || transA=="T"){
      mA = A.numCols
      kA = A.numRows
    }
    if (transB == "T" || transB=="T"){
      nB = B.numRows
      kB = B.numCols
    }

    val C: DenseMatrix = DenseMatrix.zeros(mA, nB)

    gemm(transA, transB, alpha, A, B, 0.0, C)

    C
  }

  /**
   * C := alpha * A * B
   *
   * @param alpha a scalar to scale the multiplication A * B.
   * @param A the matrix A that will be left multiplied to B. Size of m x k.
   * @param B the matrix B that will be left multiplied by A. Size of k x n.
   *
   * @return The resulting matrix C. Size of m x n.
   */
  def gemm(
     alpha: Double,
     A: Matrix,
     B: DenseMatrix) : DenseMatrix = {

    gemm("N", "N", alpha, A, B)
  }

  /**
   * C := alpha * A * B + beta * C
   * For `DenseMatrix` A.
   */
  private def gemm(
      transA: String,
      transB: String,
      alpha: Double,
      A: DenseMatrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix,
      mA: Int,
      kA: Int,
      nB: Int) {

    nativeBLAS.dgemm(transA,transB, mA, nB, kA, alpha, A.toArray, A.numRows, B.toArray, B.numRows,
      beta, C.toArray, C.numRows)
  }

  /**
   * C := alpha * A * B + beta * C
   * For `SparseMatrix` A.
   */
  private def gemm(
      transA: String,
      transB: String,
      alpha: Double,
      A: SparseMatrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix,
      mA: Int,
      kA: Int,
      nB: Int) {

    val transposeA = A.numCols == mA
    val transposeB = B.numRows == nB

    val Avals = A.toArray
    val Arows = if (!transposeA) A.rowIndices else A.colIndices
    val Acols = if (!transposeA) A.colIndices else A.rowIndices

    // Slicing is easy in this case. This is the optimal multiplication setting for sparse matrices
    if (transposeA){
      var colCounter = 0
      while (colCounter < nB){ // Tests showed that the outer loop being columns was faster
        var rowCounter = 0
        while (rowCounter < mA){
          val indStart = Arows(rowCounter)
          val indEnd = Arows(rowCounter + 1)
          var elementCount = 0 // Loop over non-zero entries in column (actually the row indices,
                               // since this is a transposed multiplication
          var sum = 0.0
          while(indStart + elementCount < indEnd){
            val AcolIndex = Acols(indStart + elementCount)
            val Bval = if (!transposeB) B(AcolIndex, colCounter) else B(colCounter, AcolIndex)
            sum += Avals(indStart + elementCount) * Bval
            elementCount += 1
          }
          C.update(rowCounter, colCounter, beta * C(rowCounter, colCounter) + sum)
          rowCounter += 1
        }
        colCounter += 1
      }
    } else {

      // Scale matrix first if `beta` is not equal to 0.0
      if (beta != 0.0){
        var i = 0
        val CLength = C.numCols * C.numRows
        while ( i < CLength) {
          C.values(i) *= beta
          i += 1
        }
      }

      // Perform matrix multiplication and add to C. The rows of A are multiplied by the columns of
      // B, and added to C.
      var colCounterForA = 0 // The column of A to multiply with the row of B
      while (colCounterForA < kA){
        val indStart = Acols(colCounterForA)
        val indEnd = Acols(colCounterForA + 1)

        var elementCount = 0 // Loop over non-zero entries in column
        while (indStart + elementCount < indEnd){
          var colCounterForB = 0 // the column to be updated in C
          val rowIndex = Arows(indStart + elementCount) // the row to be updated in C
          while (colCounterForB < nB){
            val Bval =
              if (!transposeB){
                B(colCounterForA, colCounterForB)
              } else {
                B(colCounterForB, colCounterForA)
              }

            C.update(rowIndex, colCounterForB,
              C(rowIndex, colCounterForB) + Avals(indStart + elementCount) * Bval)

            colCounterForB += 1
          }
          elementCount += 1
        }
        colCounterForA += 1
      }
    }
  }

  /**
   * y := alpha * A * x + beta * y
   * @param trans specify whether to use matrix A, or the transpose of matrix A. Should be "N" or
   *               "n" to use A, and "T" or "t" to use the transpose of A.
   * @param alpha a scalar to scale the multiplication A * x.
   * @param A the matrix A that will be left multiplied to x. Size of m x n.
   * @param x the vector x that will be left multiplied by A. Size of n x 1.
   * @param beta a scalar that can be used to scale vector y.
   * @param y the resulting vector y. Size of m x 1.
   */
  def gemv(
      trans: String,
      alpha: Double,
      A: Matrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector) {

    var mA: Int = A.numRows
    val nx: Int = x.size
    var nA: Int = A.numCols
    var transposeA: Boolean = false
    if (trans == "T" || trans=="t"){
      mA = A.numCols
      nA = A.numRows
      transposeA = true
    }
    require(trans == "T" || trans == "t" || trans == "N" || trans == "n",
      s"Invalid argument used for trans: $trans. " +
        escapeJava("Must be \"N\", \"n\", \"T\", or \"t\""))

    require(nA == nx, s"The columns of A don't match the number of elements of x. A: $nA, x: $nx")
    require(mA == y.size,
      s"The rows of A don't match the number of elements of y. A: $mA, y:${y.size}}")

    A match {
      case sparse: SparseMatrix =>
        gemv(trans, alpha, sparse, x, beta, y, mA, nA, transposeA)
      case dense: DenseMatrix =>
        gemv(trans, alpha, dense, x, beta, y)
      case _ =>
        throw new IllegalArgumentException(s"gemv doesn't support matrix type ${A.getClass}.")
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
            y: DenseVector) {

    gemv("N", alpha, A, x, beta, y)
  }

  /**
   * y := alpha * A * x
   *
   * @param trans specify whether to use matrix A, or the transpose of matrix A. Should be "N" or
   *               "n" to use A, and "T" or "t" to use the transpose of A.
   * @param alpha a scalar to scale the multiplication A * x.
   * @param A the matrix A that will be left multiplied to x. Size of m x n.
   * @param x the vector x that will be left multiplied by A. Size of n x 1.
   *
   * @return `DenseVector` y, the result of the matrix-vector multiplication. Size of m x 1.
   */
  def gemv(
            trans: String,
            alpha: Double,
            A: Matrix,
            x: DenseVector): DenseVector = {

    val m = if(trans == "N" || trans == "n") A.numRows else A.numCols

    val y: DenseVector = new DenseVector(Array.fill(m)(0.0))
    gemv(trans, alpha, A, x, 0.0, y)

    y
  }

  /**
   * y := alpha * A * x
   *
   * @param alpha a scalar to scale the multiplication A * x.
   * @param A the matrix A that will be left multiplied to x. Size of m x n.
   * @param x the vector x that will be left multiplied by A. Size of n x 1.
   *
   * @return `DenseVector` y, the result of the matrix-vector multiplication. Size of m x 1.
   */
  def gemv(
            alpha: Double,
            A: Matrix,
            x: DenseVector): DenseVector = {

    gemv("N", alpha, A, x)
  }


  /**
   * y := alpha * A * x + beta * y
   * For `DenseMatrix` A.
   */
  private def gemv(
      trans: String,
      alpha: Double,
      A: DenseMatrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector) {

    nativeBLAS.dgemv(trans, A.numRows, A.numCols, alpha, A.toArray, A.numRows, x.toArray, 1, beta,
      y.toArray, 1)
  }

  /**
   * y := alpha * A * x + beta * y
   * For `SparseMatrix` A.
   */
  private def gemv(
      trans: String,
      alpha: Double,
      A: SparseMatrix,
      x: DenseVector,
      beta: Double,
      y: DenseVector,
      mA: Int,
      nA: Int,
      transposeA: Boolean) {

    val Avals = A.toArray
    val Arows = if (!transposeA) A.rowIndices else A.colIndices
    val Acols = if (!transposeA) A.colIndices else A.rowIndices

    // Slicing is easy in this case. This is the optimal multiplication setting for sparse matrices
    if (transposeA){
      var rowCounter = 0
      while (rowCounter < mA){
        val indStart = Arows(rowCounter)
        val indEnd = Arows(rowCounter + 1)
        var elementCount = 0 // Number of elements already multiplied
        var sum = 0.0
        while(indStart + elementCount < indEnd){
          sum += Avals(indStart + elementCount) * x.values(Acols(indStart + elementCount))
          elementCount += 1
        }
        y.values(rowCounter) =  beta * y.values(rowCounter) + sum
        rowCounter += 1
      }
    } else {
      // Scale vector first if `beta` is not equal to 0.0
      if (beta != 0.0){
        var i = 0
        val yLength = y.size
        while (i < yLength) {
          y.values(i) *= beta
          i += 1
        }
      }

      // Perform matrix-vector multiplication and add to y
      var colCounterForA = 0
      while (colCounterForA < nA){
        val indStart = Acols(colCounterForA)
        val indEnd = Acols(colCounterForA + 1)

        var elementCount = 0 // Number of non-zero entries in column
        while (indStart + elementCount < indEnd){
          val rowIndex = Arows(indStart + elementCount)
          y.values(rowIndex) += Avals(indStart + elementCount) * x.values(colCounterForA)
          elementCount += 1
        }
        colCounterForA += 1
      }
    }
  }

}
