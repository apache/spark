package breeze.optimize.proximal

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Rand

/**
 * PDCO dense quadratic program generator
 *
 * Reference
 *
 * Generates random instances of Quadratic Programming Problems
 * 0.5x'Px + q'x
 * s.t Ax = b
 *  lb <= x <= ub
 *
 * nGram rank of quadratic problems to be generated
 * @author debasish83
 * @return A is the equality constraint
 * @return b is the equality parameters
 * @return lb is vector of lower bounds (default at 0.0)
 * @return ub is vector of upper bounds (default at 10.0)
 * @return q is linear representation of the function
 * @return H is the quadratic representation of the function
 */
object QpGenerator {
  def getGram(nGram: Int) = {
    val hrand = DenseMatrix.rand[Double](nGram, nGram, Rand.gaussian(0, 1))
    val hrandt = hrand.t
    val hposdef = hrandt * hrand
    val H = hposdef.t + hposdef
    H
  }

  def apply(nGram: Int, nEqualities: Int) = {
    val en = DenseVector.ones[Double](nGram)
    val zn = DenseVector.zeros[Double](nGram)

    val A = DenseMatrix.rand[Double](nEqualities, nGram)
    val x = en

    val b = A * x
    val q = DenseVector.rand[Double](nGram)

    val lb = zn.copy
    val ub = en :* 10.0

    val H = getGram(nGram)

    (A, b, lb, ub, q, H)
  }
}