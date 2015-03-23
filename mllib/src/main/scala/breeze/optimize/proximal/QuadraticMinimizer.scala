package breeze.optimize.proximal

import breeze.linalg._
import breeze.numerics._
import breeze.optimize.linear.{ConjugateGradient, PowerMethod}
import breeze.optimize.proximal.Constraint._
import breeze.optimize.{DiffFunction, LBFGS, OWLQN}
import breeze.stats.distributions.Rand
import breeze.util.SerializableLogging
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW
import spire.syntax.cfor._

import scala.math.{abs, sqrt}

/**
 * Proximal operators and ADMM based Primal-Dual QP Solver 
 * 
 * Reference: http://www.stanford.edu/~boyd/papers/admm/quadprog/quadprog.html
 * 
 * 
 * It solves problem that has the following structure
 * 
 * 1/2 x'Hx + f'x + g(x)
 * s.t Aeqx = beq
 *
 * g(x) represents the following constraints which covers ALS based matrix factorization use-cases
 * 
 * 1. x >= 0
 * 2. lb <= x <= ub
 * 3. L1(x)
 * 4. L2(x)
 * 5. Generic regularization on x
 *
 * @param nGram rank of dense gram matrix
 * @param proximal proximal operator to be used
 * @param Aeq rhs matrix for equality constraints
 * @param beq lhs constants for equality constraints
 * @author debasish83
 */
class QuadraticMinimizer(nGram: Int,
                         proximal: Proximal = null,
                         Aeq: DenseMatrix[Double] = null,
                         beq: DenseVector[Double] = null,
                         maxIters: Int = -1, abstol: Double = 1e-6, reltol: Double = 1e-4)
  extends SerializableLogging {

  type BDM = DenseMatrix[Double]
  type BDV = DenseVector[Double]

  case class State private[QuadraticMinimizer](x: BDV, u: BDV, z: BDV, scale: BDV,
                                               R: BDM, pivot: Array[Int],
                                               xHat: BDV, zOld: BDV, residual: BDV,
                                               s: BDV, iter: Int, converged: Boolean) {
  }

  val linearEquality = if (Aeq != null) Aeq.rows else 0

  if (linearEquality > 0)
    require(beq.length == linearEquality, s"QuadraticMinimizer linear equalities should match beq vector")

  val n = nGram + linearEquality
  val full = n * n

  /**
   * wsH is the workspace for gram matrix / quasi definite system based on the problem definition
   * Quasi definite system is formed if we have a set of affine constraints in quadratic minimization
   * minimize 0.5x'Hx + c'x + g(x)
   * s.t Aeq x = beq
   *
   * Proximal formulations can handle different g(x) as specified above but if Aeq x = beq is not
   * a probability simplex (1'x = s, x >=0) and arbitrary equality constraint, then we form the
   * quasi definite system by adding affine constraint Aeq x = beq in x-update and handling bound
   * constraints like lb <= z <= ub in g(z) update.
   *
   * Capability of adding affine constraints let us support all the features that an interior point
   * method like Matlab QuadProg and Mosek can handle
   *
   * The affine system looks as follows:
   *
   * Aeq is l x rank
   * H is rank x rank
   *
   * wsH is a quasi-definite matrix
   * [ P + rho*I, Aeq' ]
   * [ Aeq        , 0  ]
   *
   * if wsH is positive definite then we use cholesky factorization and cache the factorization
   * if wsH is quasi definite then we use lu factorization and cache the factorization
   *
   * Preparing the wsH workspace is expensive and so memory is allocated at the construct time and
   * an API updateGram is exposed to users so that the workspace can be used by many solves that show
   * up in distributed frameworks like Spark mllib ALS
   */
  private val wsH = DenseMatrix.zeros[Double](n, n)

  val transAeq = if (linearEquality > 0) Aeq.t else null

  val admmIters = if (maxIters < 0) Math.max(400, 20 * n) else maxIters

  def getProximal = proximal

  /**
   * updateGram API is meant to be called iteratively from Normal Equations constructed by the user
   * @param H rank * rank size full gram matrix
   */
  def updateGram(H: DenseMatrix[Double]): Unit = {
    wsH(0 until nGram, 0 until nGram) := H
  }

  /**
   * Public API to get an initialState for solver hot start such that subsequent calls can reuse
   * state memmory
   *
   * @return the state for the optimizer
   */
  def initialize = {
    var pivot: Array[Int] = null
    // Allocate memory for pivot
    if (linearEquality > 0) pivot = Array.fill[Int](n)(0)

    val x = DenseVector.zeros[Double](nGram)
    val z = DenseVector.zeros[Double](nGram)
    val u = DenseVector.zeros[Double](nGram)
    // scale = rho*(z - u) - q
    val scale = DenseVector.zeros[Double](n)

    if (proximal == null) {
      State(x, u, z, scale, null, pivot, null, null, null, null, 0, false)
    } else {
      val xHat = DenseVector.zeros[Double](nGram)
      val zOld = DenseVector.zeros[Double](nGram)

      val residual = DenseVector.zeros[Double](nGram)
      val s = DenseVector.zeros[Double](nGram)

      State(x, u, z, scale, null, pivot, xHat, zOld, residual, s, 0, false)
    }
  }

  //u is the lagrange multiplier
  //z is for the proximal operator application
  private def reset(q: DenseVector[Double], state: State) = {
    import state._
    val nlinear = q.length
    val info = new intW(0)

    //TO DO : Mutate wsH and put the factors in it, the arraycopy is not required
    if (linearEquality > 0) {
      val equality = nlinear + beq.length
      require(wsH.rows == equality && wsH.cols == equality, s"QuadraticMinimizer:reset quasi definite and linear size mismatch")
      // TO DO : Use LDL' for symmetric quasi definite matrix lapack.dsytrf
      wsH(nGram until (nGram + Aeq.rows), 0 until Aeq.cols) := Aeq
      wsH(0 until nGram, nGram until (nGram + Aeq.rows)) := transAeq
      lapack.dgetrf(n, n, wsH.data, scala.math.max(1, n), pivot, info)
    }
    else {
      require(wsH.rows == nlinear && wsH.cols == nlinear, s"QuadraticMinimizer:reset cholesky and linear size mismatch")
      lapack.dpotrf("L", n, wsH.data, scala.math.max(1, n), info)
    }

    x := 0.0
    u := 0.0
    z := 0.0

    State(x, u, z, scale, wsH, pivot, xHat, zOld, residual, s, 0, false)
  }

  private def updatePrimal(q: BDV, x: BDV, u: BDV, z: BDV, scale: BDV, rho: Double, R: BDM, pivot: Array[Int]) = {
    //scale = rho*(z - u) - q
    cforRange(0 until z.length) { i =>
      val entryScale = rho * (z(i) - u(i)) - q(i)
      scale.update(i, entryScale)
    }
    if (linearEquality > 0)
      cforRange(0 until beq.length) { i => scale.update(nGram + i, beq(i)) }

    // TO DO : Use LDL' based solver for quasi definite / sparse gram
    if (linearEquality > 0) {
      // x = U \ (L \ q)
      QuadraticMinimizer.dgetrs(R, pivot, scale)
    } else {
      // x = R \ (R' \ scale)
      // Step 1 : R' * y = scale
      // Step 2 : R * x = y
      QuadraticMinimizer.dpotrs(R, scale)
    }
    cforRange(0 until x.length) { i => x.update(i, scale(i)) }
  }

  /**
   * iterations API gives an advanced control for users who would like to use QuadraticMinimizer in 2 steps, update
   * the gram matrix first using updateGram API and followed by doing the solve by providing a user defined
   * initialState. It also exposes alpha and rho control to users who would like to experiment with rho and alpha
   * parameters of the admm algorithm
   * @param q linear term for the quadratic optimization
   * @param rho rho parameter for ADMM algorithm
   * @param alpha over-relaxation alpha parameter for admm algorithm, default 1.0
   * @param initialState provide a initialState using initialState API
   * @param resetState use true if you want to hot start based on the provided state
   * @return converged state from ADMM algorithm
   */
  def iterations(q: DenseVector[Double],
                 rho: Double,
                 initialState: State,
                 resetState: Boolean = true,
                 alpha: Double = 1.0): State = {
    val startState = if (resetState) reset(q, initialState) else initialState
    import startState._

    // Unconstrained Quadratic Minimization with/without affine constraints does not need
    // proxima update
    if (proximal == null) {
      updatePrimal(q, x, u, z, scale, rho, R, pivot)
      return State(x, u, z, scale, R, pivot, xHat, zOld, residual, s, 1, true)
    }

    //scale will hold q + linearEqualities
    val convergenceScale = sqrt(n)

    var nextIter = 0
    while (nextIter <= admmIters) {
      //primal update
      updatePrimal(q, x, u, z, scale, rho, R, pivot)

      //proximal z-update with relaxation

      //zold = (1-alpha)*z
      //x_hat = alpha*x + zold
      zOld := z
      zOld *= 1 - alpha

      xHat := x
      xHat *= alpha
      xHat += zOld

      //zold = z
      zOld := z

      //z = xHat + u
      z := xHat
      z += u

      //Apply proximal operator
      proximal.prox(z, rho)

      //z has proximal(x_hat)

      //Dual (u) update
      xHat -= z
      u += xHat

      //Convergence checks
      //history.r_norm(k)  = norm(x - z)
      residual := x
      residual -= z
      val residualNorm = norm(residual, 2)

      //history.s_norm(k)  = norm(-rho*(z - zold))
      s := z
      s -= zOld
      s *= -rho
      val sNorm = norm(s, 2)

      residual := z
      residual *= -1.0

      //s = rho*u
      s := u
      s *= rho

      val epsPrimal = convergenceScale * abstol + reltol * max(norm(x, 2), norm(residual, 2))
      val epsDual = convergenceScale * abstol + reltol * norm(s, 2)

      if (residualNorm < epsPrimal && sNorm < epsDual) {
        return State(x, u, z, scale, R, pivot, xHat, zOld, residual, s, nextIter, true)
      }
      nextIter += 1
    }
    State(x, u, z, scale, R, pivot, xHat, zOld, residual, s, nextIter, false)
  }

  private def computeRho(H: DenseMatrix[Double]): Double = {
    proximal match {
      case null => 0.0
      case ProximalL1(lambda: Double) => {
        val eigenMax = QuadraticMinimizer.normColumn(H)
        require(linearEquality <= 0, s"QuadraticMinimizer:computeRho L1 with affine not supported")
        val eigenMin = QuadraticMinimizer.approximateMinEigen(H)
        sqrt(eigenMin * eigenMax)
      }
      case _ => sqrt(QuadraticMinimizer.normColumn(H))
    }
  }

  private def iterations(q: DenseVector[Double], initialState: State): State = {
    val rho = computeRho(wsH)
    cforRange(0 until q.length) { i => wsH.update(i, i, wsH(i, i) + rho) }
    iterations(q, rho, initialState)
  }

  private def iterations(H: DenseMatrix[Double], q: DenseVector[Double], initialState: State): State = {
    updateGram(H)
    iterations(q, initialState)
  }

  /**
   * minimize API allows users to provide a gram matrix and the linear term for solving the quadratic
   * problem with constraints provided as proximal algorithm
   * @param H gram matrix
   * @param q linear term
   * @param initialState provide a workspace for the solver when the problem dimension did not change
   * @return solution of the constrained quadratic problem
   */
  def minimize(H: DenseMatrix[Double], q: DenseVector[Double], initialState: State): DenseVector[Double] = {
    minimizeAndReturnState(H, q, initialState).x
  }

  def minimizeAndReturnState(H: DenseMatrix[Double], q: DenseVector[Double], initialState: State): State = {
    iterations(H, q, initialState)
  }

  /**
   * minimize API allows users to provide a gram matrix and the linear term for solving the quadratic
   * problem with constraints provided as proximal algorithm
   * @param H gram matrix
   * @param q linear term
   * @return solution of the constrained quadratic problem
   */
  def minimize(H: DenseMatrix[Double], q: DenseVector[Double]): DenseVector[Double] = {
    minimizeAndReturnState(H, q).x
  }

  def minimizeAndReturnState(H: DenseMatrix[Double], q: DenseVector[Double]): State = {
    iterations(H, q, initialState=initialize)
  }

  /**
   * minimize API allows user to provide linear term for solving the quadratic problem with constraints provided as
   * proximal algorithm. The gram matrix must be updated using updateGram API before minimization is called
   * @param q linear term
   * @return solution of the constrained quadratic problem
   */
  def minimize(q: DenseVector[Double]): DenseVector[Double]  = {
    minimizeAndReturnState(q).x
  }

  def minimizeAndReturnState(q: DenseVector[Double]): State = {
    iterations(q, initialState=initialize)
  }
}

object QuadraticMinimizer {
  /**
   * y := alpha * A * x + beta * y
   * For `DenseMatrix` A.
   */
  def gemv(alpha: Double,
           A: DenseMatrix[Double],
           x: DenseVector[Double],
           beta: Double,
           y: DenseVector[Double]): Unit = {
    val tStrA = if (A.isTranspose) "T" else "N"
    val mA = if (!A.isTranspose) A.rows else A.cols
    val nA = if (!A.isTranspose) A.rows else A.cols
    blas.dgemv(tStrA, mA, nA, alpha, A.data, mA, x.data, 1, beta, y.data, 1)
  }

  /**
   * Triangular LU solve for finding y such that y := Ax where A is the LU factorization
   *
   * @param A vector representation of LU factorization
   * @param pivot pivot from LU factorization
   * @param x the linear term for the solve which will also host the result
   */
  def dgetrs(A: DenseMatrix[Double],
             pivot: Array[Int],
             x: DenseVector[Double]): Unit = {
    val n = x.length
    require(A.rows == n)
    val nrhs = 1
    val info: intW = new intW(0)

    lapack.dgetrs("No transpose", n, nrhs, A.data, 0, n, pivot, 0, x.data, 0, n, info)
    if (info.`val` > 0) throw new IllegalArgumentException("DGETRS: LU solve unsuccessful")
  }

  /**
   * Triangular Cholesky solve for finding y through backsolves such that y := Ax
   *
   * @param A vector representation of lower triangular cholesky factorization
   * @param x the linear term for the solve which will also host the result
   */
  def dpotrs(A: DenseMatrix[Double], x: DenseVector[Double]): Unit = {
    val n = x.length
    val nrhs = 1
    require(A.rows == n)
    val info: intW = new intW(0)
    lapack.dpotrs("L", n, nrhs, A.data, 0, n, x.data, 0, n, info)

    if (info.`val` > 0) throw new IllegalArgumentException("DPOTRS : Leading minor of order i of A is not positive definite.")
  }

  //upper bound on max eigen value
  def normColumn(H: DenseMatrix[Double]): Double = {
    var absColSum = 0.0
    var maxColSum = 0.0
    for (c <- 0 until H.cols) {
      for (r <- 0 until H.rows) {
        absColSum += abs(H(r, c))
      }
      if (absColSum > maxColSum) maxColSum = absColSum
      absColSum = 0.0
    }
    maxColSum
  }

  //approximate max eigen using inverse power method
  def approximateMaxEigen(H: DenseMatrix[Double]): Double = {
    val pm = new PowerMethod()
    val init = DenseVector.rand[Double](H.rows, Rand.gaussian(0, 1))
    pm.eigen(H, init)
  }

  //approximate min eigen using inverse power method
  def approximateMinEigen(H: DenseMatrix[Double]): Double = {
    val R = cholesky(H).t
    val pmInv = PowerMethod.inverse()
    val init = DenseVector.rand[Double](H.rows, Rand.gaussian(0, 1))
    1.0 / pmInv.eigen(R, init)
  }

  def apply(rank: Int,
            constraint: Constraint,
            lambda: Double = 1.0): QuadraticMinimizer = {
    constraint match {
      case SMOOTH => new QuadraticMinimizer(rank)
      case POSITIVE => new QuadraticMinimizer(rank, ProjectPos())
      case BOX => {
        //Direct QP with bounds
        val lb = DenseVector.zeros[Double](rank)
        val ub = DenseVector.ones[Double](rank)
        new QuadraticMinimizer(rank, ProjectBox(lb, ub))
      }
      case PROBABILITYSIMPLEX => {
        new QuadraticMinimizer(rank, ProjectProbabilitySimplex(lambda))
      }
      case EQUALITY => {
        //Direct QP with equality and positivity constraint
        val Aeq = DenseMatrix.ones[Double](1, rank)
        val beq = DenseVector.ones[Double](1)
        new QuadraticMinimizer(rank, ProjectPos(), Aeq, beq)
      }
      case SPARSE => new QuadraticMinimizer(rank, ProximalL1().setLambda(lambda))
    }
  }
}