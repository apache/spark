package org.apache.spark.mllib.linalg

import com.github.fommil.netlib.F2jBLAS

/**
 * BLAS routines for MLlib's vectors and matrices.
 */
private[mllib] object MLlibBLAS {

  // For level-1 routines, we use Java implementation.
  val f2jBLAS = new F2jBLAS

  /**
   * y += alpha * x
   */
  def daxpy(alpha: Double, x: Vector, y: Vector) {
    require(x.size == y.size)
    y match {
      case dy: DenseVector =>
        x match {
          case sx: SparseVector =>
            daxpy(alpha, sx, dy)
          case dx: DenseVector =>
            daxpy(alpha, dx, dy)
          case _ =>
            throw new UnsupportedOperationException(
              s"daxpy doesn't support x type ${x.getClass}.")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"daxpy only supports adding to a dense vector but got type ${y.getClass}.")
    }
  }

  /**
   * y += alpha * x
   */
  private def daxpy(alpha: Double, x: DenseVector, y: DenseVector) {
    val n = x.size
    f2jBLAS.daxpy(n, alpha, x.values, 1, y.values, 1)
  }

  /**
   * y += alpha * x
   */
  private def daxpy(alpha: Double, x: SparseVector, y: DenseVector) {
    val nnz = x.indices.size
    if (alpha == 1.0) {
      var k = 0
      while (k < nnz) {
        y.values(x.indices(k)) += x.values(k)
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        y.values(x.indices(k)) += alpha * x.values(k)
        k += 1
      }
    }
  }

  /**
   * dot(x, y)
   */
  def ddot(x: Vector, y: Vector): Double = {
    require(x.size == y.size)
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        ddot(dx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        ddot(sy, dx)
      case (sx: SparseVector, dy: DenseVector) =>
        ddot(sx, dy)
      case (sx: SparseVector, sy: SparseVector) =>
        ddot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"ddot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  /**
   * dot(x, y)
   */
  private def ddot(x: DenseVector, y: DenseVector): Double = {
    val n = x.size
    f2jBLAS.ddot(n, x.values, 1, y.values, 1)
  }

  /**
   * dot(x, y)
   */
  private def ddot(x: SparseVector, y: DenseVector): Double = {
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
  private def ddot(x: SparseVector, y: SparseVector): Double = {
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
}
