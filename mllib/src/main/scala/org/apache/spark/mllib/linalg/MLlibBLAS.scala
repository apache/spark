package org.apache.spark.mllib.linalg

import com.github.fommil.netlib.F2jBLAS

private[mllib] object MLlibBLAS {

  val f2jBLAS = new F2jBLAS

  def daxpy(alpha: Double, x: Vector, y: Vector) {
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

  def daxpy(alpha: Double, x: DenseVector, y: DenseVector) {
    val n = x.size
    require(y.size == x.size)
    f2jBLAS.daxpy(n, alpha, x.values, 1, y.values, 1)
  }

  def daxpy(alpha: Double, x: SparseVector, y: DenseVector) {
    require(x.size == y.size)
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
}
