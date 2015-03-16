package breeze.optimize.linear

import breeze.linalg.operators.OpMulMatrix
import breeze.linalg.{DenseMatrix, DenseVector, norm}
import breeze.math.MutableInnerProductModule
import breeze.numerics.abs
import breeze.optimize.proximal.QuadraticMinimizer
import breeze.util.Implicits._
import breeze.util.SerializableLogging

/**
 * Power Method to compute maximum eigen value and companion object to compute minimum eigen value through inverse power
 * iterations
 * @author debasish83
 */
class PowerMethod[T, M](maxIters: Int = 10,tolerance: Double = 1E-5)
                       (implicit space: MutableInnerProductModule[T, Double], mult: OpMulMatrix.Impl2[M, T, T]) extends SerializableLogging {

  import space._

  case class State private[PowerMethod] (eigenValue: Double, eigenVector: T, iter: Int, converged: Boolean)

  //memory allocation for the eigen vector result
  def normalize(y: T) : T = {
    val normInit = norm(y)
    val init = copy(y)
    init *= 1.0/normInit
  }

  def initialState(y: T, A: M): State = {
    val ynorm = normalize(y)
    val ay = mult(A, ynorm)
    val lambda = nextEigen(ynorm, ay)
    State(lambda, ynorm, 0, false)
  }

  //in-place modification of eigen vector
  def nextEigen(eigenVector: T, ay: T) = {
    val lambda = eigenVector dot ay
    eigenVector := ay
    val norm1 = norm(ay)
    eigenVector *= 1.0/norm1
    if (lambda < 0.0) eigenVector *= -1.0
    lambda
  }

  def iterations(y: T,
                 A: M): Iterator[State] = Iterator.iterate(initialState(y, A)) { state =>
    import state._
    val ay = mult(A, eigenVector)
    val lambda = nextEigen(eigenVector, ay)
    val val_dif = abs(lambda - eigenValue)
    if (val_dif <= tolerance || iter > maxIters) State(lambda, eigenVector, iter + 1, true)
    else State(lambda, eigenVector, iter + 1, false)
  }.takeUpToWhere(_.converged)

  def iterateAndReturnState(y: T, A: M): State = {
    iterations(y, A).last
  }

  def eigen(y: T, A: M): Double = {
    iterateAndReturnState(y, A).eigenValue
  }
}

object PowerMethod {
  def inverse(maxIters: Int = 10, tolerance: Double = 1E-5) : PowerMethod[DenseVector[Double], DenseMatrix[Double]] = new PowerMethod[DenseVector[Double], DenseMatrix[Double]](maxIters, tolerance) {
    override def initialState(y: DenseVector[Double], A: DenseMatrix[Double]): State = {
      val ynorm = normalize(y)
      val ay = QuadraticMinimizer.solveTriangular(A, ynorm)
      val lambda = nextEigen(ynorm, ay)
      State(lambda, ynorm, 0, false)
    }
    override def iterations(y: DenseVector[Double],
                            A: DenseMatrix[Double]): Iterator[State] = Iterator.iterate(initialState(y, A)) { state =>
      import state._
      val ay = QuadraticMinimizer.solveTriangular(A, eigenVector)
      val lambda = nextEigen(eigenVector, ay)
      val val_dif = abs(lambda - eigenValue)
      if (val_dif <= tolerance || iter > maxIters) State(lambda, eigenVector, iter + 1, true)
      else State(lambda, eigenVector, iter + 1, false)
    }.takeUpToWhere(_.converged)
  }
}