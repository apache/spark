/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"), you may not use this file except in compliance with
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

package org.apache.spark.mllib.optimization

import org.jblas.DoubleMatrix
import org.jblas.Decompose
import org.jblas.Solve
import scala.math.min
import scala.math.max
import scala.math.sqrt
import breeze.optimize.{ LBFGS => BreezeLBFGS }
import breeze.linalg.DenseVector
import breeze.optimize.DiffFunction
import breeze.linalg.norm
import org.netlib.lapack.Dpotrs
import org.netlib.lapack.Dgetrf
import org.netlib.lapack.Dgetrs
//For QR factorization
//import org.netlib.lapack.Dgeqrf
import org.jblas.SimpleBlas
import org.netlib.util.intW
import org.jblas.exceptions.LapackArgumentException
import org.jblas.NativeBlas
import org.jblas.Eigen
import org.apache.spark.mllib.optimization.Constraint._
import scala.math.abs

/*
 * Proximal operators and ADMM based Primal-Dual QP Solver 
 * 
 * Reference: http://www.stanford.edu/~boyd/papers/admm/quadprog/quadprog.html
 * 
 * 
 * It solves problem that has the following structure
 * 
 * 1/2 x*'Hx + f*'x + g(x)
 *
 * 1/2 x'Hx + f'x + g(x) 
 * s.t ax = b
 *
 * g(x) represents the following constraints which covers matrix factorization use-cases
 * 
 * 1. x >= 0
 * 2. lb <= x <= ub
 * 3. L1(x)
 * 4. L2(x)
 * 5. Generic regularization on x
 */

class QuadraticMinimizer(nGram: Int,
  lb: Option[DoubleMatrix] = None, ub: Option[DoubleMatrix] = None,
  Aeq: Option[DoubleMatrix] = None, beq: Option[DoubleMatrix] = None,
  addEqualityToGram: Boolean = false) {

  //if addEquality is true, add the contribution of the equality constraint to gram matrix workspace
  val linearEquality = if (Aeq != None) Aeq.get.rows else 0

  val n = nGram + linearEquality
  
  var alpha: Double = 1.0
  var rho: Double = 0.0

  var solveTime: Long = 0
  var iterations: Long = 0

  val wsH = if (addEqualityToGram && linearEquality > 0) {
    //Aeq is l x rank
    //H is rank x rank
    /* wsH is a quasi-definite matrix */
    /* [ P + rho*I, A' ]
	 * [ A        , 0  ]
	 */
    val matAeq = Aeq.get
    val ws = DoubleMatrix.zeros(n, n)
    val transAeq = matAeq.transpose()

    for (row <- 0 until matAeq.rows)
      for (column <- 0 until matAeq.columns)
        ws.put(row + nGram, column, matAeq.get(row, column))

    for (row <- 0 until transAeq.rows)
      for (column <- 0 until transAeq.columns)
        ws.put(row, column + nGram, transAeq.get(row, column))
    ws
  } else {
    DoubleMatrix.zeros(n, n)
  }

  val MAX_ITER = Math.max(400, 20 * n)
  val ABSTOL = 1e-8
  val RELTOL = 1e-4
  val EPS = 1e-4

  var z = DoubleMatrix.zeros(nGram, 1)
  var u = DoubleMatrix.zeros(nGram, 1)

  var xHat = DoubleMatrix.zeros(nGram, 1)
  var zOld = DoubleMatrix.zeros(nGram, 1)

  //scale will hold q + linearEqualities
  var scale = DoubleMatrix.zeros(n, 1)

  var residual = DoubleMatrix.zeros(nGram, 1)
  var s = DoubleMatrix.zeros(nGram, 1)

  /* L1 regularization */
  var lambda: Double = 1.0
  var constraint: Constraint = SMOOTH

  var R: DoubleMatrix = null
  var pivot: Array[Int] = null

  /* If Aeq exists and rows > 1, cache the pseudo-inverse to be used later */
  /* If Aeq exist and rows = 1, cache the transpose */
  val invAeq = if (!addEqualityToGram && Aeq != None && Aeq.get.rows > 1)
    Some(Solve.pinv(Aeq.get))
  else
    None

  /*Regularization for Elastic Net */
  def setLambda(lambda: Double): QuadraticMinimizer = {
    this.lambda = lambda
    this
  }

  //TO DO : This can take a proximal function as input
  //TO DO : alpha needs to be scaled based on Nesterov's acceleration
  def setProximal(constraint: Constraint): QuadraticMinimizer = {
    this.constraint = constraint
    this
  }

  def updateGram(row: Int, col: Int, value: Double) {
    if (row < 0 || row >= n) {
      throw new IllegalArgumentException("QuadraticMinimizer row out of bounds for gram matrix update")
    }
    if (col < 0 || col >= n) {
      throw new IllegalArgumentException("QuadraticMinimizer column out of bounds for gram matrix update")
    }
    wsH.put(row, col, value)
  }

  /* Triangular LU solve for A*X = B */
  def solveTriangularLU(A: DoubleMatrix, pivot: Array[Int], B: DoubleMatrix): DoubleMatrix = {
    A.assertSquare()
    var X = B.dup()

    val n = A.rows
    val nrhs = X.columns
    var info: intW = new intW(0)

    Dgetrs.dgetrs("No transpose", n, nrhs, A.data, 0, A.rows, pivot, 0, X.data, 0, X.rows, info)

    if (info.`val` > 0)
      throw new LapackArgumentException("DGETRS", "LU solve unsuccessful")

    X
  }

  /*Triangular cholesky solve for A*X = B */
  def solveTriangular(A: DoubleMatrix, B: DoubleMatrix): DoubleMatrix = {
    A.assertSquare()
    var X = B.dup()

    val n = A.rows
    val nrhs = X.columns
    var info: intW = new intW(0)

    Dpotrs.dpotrs("U", n, nrhs, A.data, 0, A.rows, X.data, 0, X.rows, info)

    if (info.`val` > 0)
      throw new LapackArgumentException("DPOTRS",
        "Leading minor of order i of A is not positive definite.")
    X
  }

  def project(x: DoubleMatrix): DoubleMatrix = {
    var i = 0
    while (i < x.rows) {
      if (abs(x.data(i)) <= EPS) x.data(i) = 0.0
      i = i + 1
    }
    x
  }

  def solve(q: DoubleMatrix): (DoubleMatrix, Boolean) = {
    //Dense cholesky factorization if the gram matrix is well defined
    if (!addEqualityToGram) {
      R = Decompose.cholesky(wsH)
    } else {
      R = wsH.dup()
      pivot = Array.fill[Int](min(R.rows, R.columns))(0)
      NativeBlas.dgetrf(wsH.rows, wsH.columns, R.data, 0, wsH.rows, pivot, 0)
    }

    val x = DoubleMatrix.zeros(nGram, 1)

    z.fill(0)
    u.fill(0)

    residual.fill(0)
    s.fill(0)

    var k = 0

    //u is the langrange multiplier
    //z is for the proximal operator application

    /* Check if linearEqualities and beq matches */
    if (linearEquality > 0 && beq.get.rows != linearEquality) {
      throw new IllegalArgumentException("QuadraticMinimizer beq vector should match the number of linear equalities")
    }

    //Memory for x and tempR are allocated by Solve.solve calls
    //TO DO : See how this is implemented in breeze, why a workspace can't be used  
    while (k < MAX_ITER) {
      //scale = rho*(z - u) - q
      scale.fill(0)
      for (i <- 0 until z.length) {
        val entryScale = rho * (z.get(i, 0) - u.get(i, 0)) - q.get(i, 0)
        scale.put(i, 0, entryScale)
      }

      if (linearEquality > 0) {
        for (i <- 0 until beq.get.length) scale.put(nGram + i, 0, beq.get.data(i))
      }

      //TO DO : Use LDL' decomposition for efficiency if the Gram matrix is sparse
      //TO DO : Do we need a full newton step or we should take a damped newton step
      val xlambda = if (addEqualityToGram) {
        // If the Gram matrix is positive definite then use Cholesky else use LU Decomposition
        // x = U \ (L \ q),
        solveTriangularLU(R, pivot, scale)
      } else {
        // x = R \ (R' \ scale)
        //Step 1 : R' * y = scale
        //Step 2 : R * x = y
        solveTriangular(R, scale)
      }

      for (i <- 0 until x.length) x.put(i, 0, xlambda.get(i, 0))

      //Unconstrained Quadratic Minimization does need any proximal step
      if (constraint == SMOOTH) return (x, true)

      //z-update with relaxation

      //zold = (1-alpha)*z
      //x_hat = alpha*x + zold
      zOld.fill(0).addi(z).muli(1 - alpha)
      xHat.fill(0).addi(x).muli(alpha).addi(zOld)

      //zold = z
      zOld.fill(0).addi(z)

      //z = xHat + u
      z.fill(0).addi(xHat).addi(u)

      //Apply proximal operator

      //Pick the correct proximal operator based on options
      //We will test the following

      //1. projectPos
      //2. projectBounds
      //3. projectEquality/projectHyperPlane based on the structure of Aeq
      //4. proxL1
      //Other options not tried yet
      //5. proxHuber
      constraint match {
        case POSITIVE => Proximal.projectPos(z.data)
        case BOUNDS => {
          if (lb == None && ub == None)
            throw new IllegalArgumentException("QuadraticMinimizer proximal operator on box needs lower and upper bounds")
          Proximal.projectBox(z.data, lb.get.data, ub.get.data)
        }
        case EQUALITY => {
          if (Aeq == None) throw new IllegalArgumentException("QuadraticMinimizer proximal operator on equality needs Aeq")
          if (beq == None) throw new IllegalArgumentException("QuadraticMinimizer proximal operator on equality needs beq")
          if (Aeq.get.rows > 1) Proximal.projectEquality(z, Aeq.get, invAeq.get, beq.get)
          else Proximal.projectHyperPlane(z, Aeq.get, beq.get.data(0))
        }
        case SPARSE => Proximal.shrinkage(z.data, lambda / rho)
      }

      //z has proximal(x_hat)

      //Dual (u) update
      u.addi(xHat.subi(z))

      //Convergence checks
      //history.r_norm(k)  = norm(x - z),
      residual.fill(0).addi(x).subi(z)
      val residualNorm = residual.norm2()

      //history.s_norm(k)  = norm(-rho*(z - zold)),
      s.fill(0).addi(z).subi(zOld).muli(-rho)
      val sNorm = s.norm2()

      //TO DO : Make sure z.muli(-1) is actually needed in norm calculation
      //residual = -z
      residual.fill(0).addi(z).muli(-1)
      //s = rho*u
      s.fill(0).addi(u).muli(rho)

      val epsPrimal = sqrt(n) * ABSTOL + RELTOL * max(x.norm2(), residual.norm2())
      val epsDual = sqrt(n) * ABSTOL + RELTOL * s.norm2()

      if (residualNorm < epsPrimal && sNorm < epsDual) {
        iterations += k
        return (project(x), true)
      }
      k += 1
    }
    iterations += MAX_ITER
    (project(x), false)
  }

  private def normColumn(H: DoubleMatrix): Double = {
    var absColSum = 0.0
    var maxColSum = 0.0
    for (c <- 0 until H.getColumns()) {
      for (r <- 0 until H.getRows()) {
        absColSum += scala.math.abs(H.get(c) + H.get(r, c))
      }
      if (absColSum > maxColSum) maxColSum = absColSum
      absColSum = 0.0
    }
    maxColSum
  }
  
  //TO DO : Replace eigenMin using inverse power law for 5-10 iterations
  private def computeRho(H: DoubleMatrix): Double = {
    constraint match {
      case SMOOTH => 0.0
      case SPARSE => {
        val eigenMax = normColumn(H)
        val eigenMin = 1 / normColumn(Solve.solvePositive(H, DoubleMatrix.eye(H.rows)))
        sqrt(eigenMin*eigenMax)
      }
      case _ => sqrt(normColumn(H)) 
    }
  }
  
  def solve(H: DoubleMatrix, q: DoubleMatrix): (DoubleMatrix, Boolean) = {
    for (i <- 0 until H.rows)
      for (j <- 0 until H.columns) {
        wsH.put(i, j, H.get(i, j))
      }
    rho = computeRho(wsH)
    for (i <- 0 until H.rows) wsH.put(i, i, wsH.get(i, i) + rho)

    val solveStart = System.nanoTime()
    val result = solve(q)
    solveTime += System.nanoTime() - solveStart
    result
  }
}

/* PDCO dense quadratic program generator
function [A,b,bl,bu,c,d1,d2,H] = toydata( m,n )

%        [A,b,bl,bu,c,d1,d2] = toydata( m,n ),
%        defines an m by n matrix A, rhs vector b, and cost vector c
%        for use with pdco.m.

  rand('state',10),
  density = 0.1,
  rc      = 1e-1,

  em      = ones(m,1),
  en      = ones(n,1),
  zn      = zeros(n,1),
  
  A       = sprand(m,n,density,rc),
  x       = en,

  gamma   = 1e-3,       % Primal regularization
  delta   = 1e-3,       % 1e-3 or 1e-4 for LP,  1 for Least squares.

  d1      = gamma,      % Can be scalar if D1 = d1*I.
  d2      = delta*em,

  b       = full(A*x),
  c       = rand(n,1),

  bl      = zn,         % x > 0
  bu      = 10*en,      % x < 10

  density = 0.05,
  H       = sprand(n,n,density,rc),
  H       = H'*H,
  H       = (H+H')/2,
*/
object QpGenerator {
  /* Generates random instances of Quadratic Programming Problems
   * 0.5x'Px + q'x
   * s.t Ax = b
   * 	lb <= x <= ub
   *  
   *  @param A is the equality constraint
   *  @param b is the equality parameters
   *  @param lb is vector of lower bounds
   *  @param ub is vector of upper bounds
   *  @param q is linear representation of the function
   *  @param H is the quadratic representation of the function 
   */
  def generate(nHessian: Int, nEqualities: Int): (DoubleMatrix, DoubleMatrix, DoubleMatrix, DoubleMatrix, DoubleMatrix, DoubleMatrix) = {
    val em = DoubleMatrix.ones(nEqualities, 1)
    val en = DoubleMatrix.ones(nHessian, 1)
    val zn = DoubleMatrix.zeros(nHessian, 1)

    val A = DoubleMatrix.rand(nEqualities, nHessian)
    val x = en

    val b = A.mmul(x)
    val q = DoubleMatrix.randn(nHessian, 1)

    val lb = zn.dup()
    val ub = en.mul(10.0)

    val hrand = DoubleMatrix.rand(nHessian, nHessian)
    val hrandt = hrand.transpose()
    val hposdef = hrandt.mmul(hrand)
    val H = hposdef.transpose().add(hposdef)
    H.muli(0.5)

    (A, b, lb, ub, q, H)
  }
}

object QuadraticMinimizer {
  def apply(rank: Int, constraint: Constraint, lambda: Double): QuadraticMinimizer = {
    constraint match {
      case SMOOTH => new QuadraticMinimizer(rank)
      case POSITIVE => new QuadraticMinimizer(rank).setProximal(POSITIVE)
      case BOUNDS => {
        //Direct QP with bounds
        val lb = DoubleMatrix.zeros(rank, 1)
        val ub = DoubleMatrix.zeros(rank, 1).addi(1.0)
        new QuadraticMinimizer(rank, Some(lb), Some(ub)).setProximal(BOUNDS)
      }
      case EQUALITY => {
        //Direct QP with equality and positivity constraint
        val Aeq = DoubleMatrix.ones(1, rank)
        val beq = DoubleMatrix.ones(1, 1)
        val qm = new QuadraticMinimizer(rank, None, None, Some(Aeq), Some(beq), true).setProximal(POSITIVE)
        qm
      }
      case SPARSE => {
        val qm = new QuadraticMinimizer(rank).setProximal(SPARSE)
        qm.setLambda(lambda)
        qm
      }
    }
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: QpSolver n m lambda beta")
      println("Test QpSolver with a simple quadratic function of dimension n and m equalities lambda beta for elasticNet")
      sys.exit(1)
    }

    val problemSize = args(0).toInt
    val nequalities = args(1).toInt
    
    val lambda = args(2).toDouble
    val beta = args(3).toDouble
    
    println(s"Test QuadraticMinimizer, breeze LBFGS and jblas solvePositive with n $problemSize m $nequalities")

    val lbfgs = new BreezeLBFGS[DenseVector[Double]](problemSize, 4)

    def optimizeWithLBFGS(init: DenseVector[Double]) = {
      val f = new DiffFunction[DenseVector[Double]] {
        def calculate(x: DenseVector[Double]) = {
          (norm((x - 3.0) :^ 2.0, 1), (x * 2.0) - 6.0)
        }
      }
      val result = lbfgs.minimize(f, init)
      norm(result - 3.0, 2) < 1E-10
    }

    val init = DenseVector.zeros[Double](problemSize)
    init(0 until init.length by 2) := -1.2
    init(1 until init.length by 2) := 1.0

    val startBfgs = System.nanoTime()
    assert(optimizeWithLBFGS(init))
    val bfgsTime = System.nanoTime() - startBfgs

    val jblasH = DoubleMatrix.eye(problemSize).mul(2.0)
    val jblasf = DoubleMatrix.zeros(problemSize, 1).add(6.0)

    val dposvStart = System.nanoTime()
    val dposvResult = Solve.solvePositive(jblasH, jblasf).data
    val dposvTime = System.nanoTime() - dposvStart
    
    val H = DoubleMatrix.eye(problemSize).mul(2.0)
    val f = DoubleMatrix.zeros(problemSize, 1).add(-6.0)

    val qpSolver = new QuadraticMinimizer(problemSize)
    val qpStart = System.nanoTime()
    val result = qpSolver.solve(H, f)
    val qpTime = System.nanoTime() - qpStart
    
    assert(result._1.subi(3.0).norm2() < 1E-4)

    println("dim " + problemSize + " bfgs " + bfgsTime / 1e3 + " dposv " + dposvTime / 1e3 + " directqp " + qpTime / 1e3)

    val n = 5
    val ata = new DoubleMatrix(Array(
      Array(4.377, -3.531, -1.306, -0.139, 3.418),
      Array(-3.531, 4.344, 0.934, 0.305, -2.140),
      Array(-1.306, 0.934, 2.644, -0.203, -0.170),
      Array(-0.139, 0.305, -0.203, 5.883, 1.428),
      Array(3.418, -2.140, -0.170, 1.428, 4.684)))

    val atb = new DoubleMatrix(Array(-1.632, 2.115, 1.094, -1.025, -0.636))

    val goodx = Array(0.13025, 0.54506, 0.2874, 0.0, 0.028628)

    val qpSolverPos = QuadraticMinimizer(n, POSITIVE, 0.0)
    val posResult = qpSolverPos.solve(ata, atb.muli(-1))

    val wsBounds = NNLS.createWorkspace(20)
    val nnlsBounds = NNLS.solve(ata, atb.muli(-1), wsBounds)

    for (i <- 0 until n) {
      println(s"qpSolver ${posResult._1.get(i)} NNLS ${nnlsBounds(i)} golden ${goodx(i)}")
    }
    println(s"qpSolverPos iterations ${qpSolverPos.iterations} NNLS iterations ${wsBounds.iterations}")

    val goodBounds: DoubleMatrix = DoubleMatrix.zeros(n, 1)
    goodBounds.put(0, 0, 0.0)
    goodBounds.put(1, 0, 0.25000000000236045)
    goodBounds.put(2, 0, 0.2499999999945758)
    goodBounds.put(3, 0, 0.0)
    goodBounds.put(4, 0, 0.0)

    val lb = DoubleMatrix.zeros(problemSize, 1)
    val ub = DoubleMatrix.zeros(problemSize, 1).addi(0.25)
    val qpSolverBounds = new QuadraticMinimizer(n, Some(lb), Some(ub), None).setProximal(BOUNDS)
    val boundsResult = qpSolverBounds.solve(ata, atb.muli(-1))
    println("Bounds result check " + (boundsResult._1.subi(goodBounds).norm2() < 1e-4) + " iterations " + qpSolverBounds.iterations)

    val qpSolverL1 = new QuadraticMinimizer(problemSize, None, None, None).setProximal(SPARSE)
    val l1Results = qpSolverL1.solve(H, f)
    println("L1 result check " + (l1Results._1.subi(2.5).norm2() < 1e-3))

    //Equality formulation
    println("Movielens equality test with bounds")

    val Hml = new DoubleMatrix(25, 25, 112.647378, 44.962984, 49.127829, 43.708389, 43.333008, 46.345827, 49.581542, 42.991226, 43.999341, 41.336724, 42.879992, 46.896465, 41.778920, 46.333559, 51.168782, 44.800998, 43.735417, 42.672057, 40.024492, 48.793499, 48.696170, 45.870016, 46.398093, 44.305791, 41.863013, 44.962984, 107.202825, 44.218178, 38.585858, 36.606830, 41.783275, 44.631314, 40.883821, 37.948817, 34.908843, 38.356328, 43.642467, 36.213124, 38.760314, 43.317775, 36.803445, 41.905953, 40.238334, 42.325769, 45.853665, 46.601722, 40.668861, 49.084078, 39.292553, 35.781804, 49.127829, 44.218178, 118.264304, 40.139032, 43.741591, 49.387932, 45.558785, 40.793703, 46.136010, 41.839393, 39.544248, 43.161644, 43.361811, 43.832852, 50.572459, 42.036961, 47.251940, 45.273068, 42.842437, 49.323737, 52.125739, 45.831747, 49.466716, 44.762183, 41.930313, 43.708389, 38.585858, 40.139032, 94.937989, 36.562570, 41.628404, 38.604965, 39.080500, 37.267530, 34.291272, 34.891704, 39.216238, 35.970491, 40.733288, 41.872521, 35.825264, 38.144457, 41.368293, 40.751910, 41.123673, 41.930358, 41.002915, 43.099168, 36.018699, 33.646602, 43.333008, 36.606830, 43.741591, 36.562570, 105.764912, 42.799031, 38.215171, 42.193565, 38.708056, 39.448031, 37.882184, 40.172339, 40.625192, 39.015338, 36.433413, 40.848178, 36.480813, 41.439981, 40.797598, 40.325652, 38.599119, 42.727171, 39.382845, 41.535989, 41.518779, 46.345827, 41.783275, 49.387932, 41.628404, 42.799031, 114.691992, 43.015599, 42.688570, 42.722905, 38.675192, 38.377970, 44.656183, 39.087805, 45.443516, 50.585268, 40.949970, 41.920556, 43.711898, 41.463472, 51.248836, 46.869144, 45.178199, 45.709593, 42.402465, 44.097412, 49.581542, 44.631314, 45.558785, 38.604965, 38.215171, 43.015599, 114.667896, 40.966284, 37.748084, 39.496813, 40.534741, 42.770125, 40.628678, 41.194251, 47.837969, 44.596875, 43.448257, 43.291878, 39.005953, 50.493111, 46.296591, 43.449036, 48.798961, 42.877859, 37.055014, 42.991226, 40.883821, 40.793703, 39.080500, 42.193565, 42.688570, 40.966284, 106.632656, 37.640927, 37.181799, 40.065085, 38.978761, 36.014753, 38.071494, 41.081064, 37.981693, 41.821252, 42.773603, 39.293957, 38.600491, 43.761301, 42.294750, 42.410289, 40.266469, 39.909538, 43.999341, 37.948817, 46.136010, 37.267530, 38.708056, 42.722905, 37.748084, 37.640927, 102.747189, 34.574727, 36.525241, 39.839891, 36.297838, 42.756496, 44.673874, 38.350523, 40.330611, 42.288511, 39.472844, 45.617102, 44.692618, 41.194977, 41.284030, 39.580938, 42.382268, 41.336724, 34.908843, 41.839393, 34.291272, 39.448031, 38.675192, 39.496813, 37.181799, 34.574727, 94.205550, 37.583319, 38.504211, 36.376976, 34.239351, 39.060978, 37.515228, 37.079566, 37.317791, 38.046576, 36.112222, 39.406838, 39.258432, 36.347136, 38.927619, 41.604838, 42.879992, 38.356328, 39.544248, 34.891704, 37.882184, 38.377970, 40.534741, 40.065085, 36.525241, 37.583319, 98.109622, 39.428284, 37.518381, 39.659011, 38.477483, 40.547021, 42.678061, 42.279104, 41.515782, 43.478416, 45.003800, 42.433639, 42.757336, 35.814356, 39.017848, 46.896465, 43.642467, 43.161644, 39.216238, 40.172339, 44.656183, 42.770125, 38.978761, 39.839891, 38.504211, 39.428284, 103.478446, 39.984358, 40.587958, 44.490750, 40.600474, 40.698368, 42.296794, 41.567854, 47.002908, 43.922434, 43.479144, 44.291425, 43.352951, 42.613649, 41.778920, 36.213124, 43.361811, 35.970491, 40.625192, 39.087805, 40.628678, 36.014753, 36.297838, 36.376976, 37.518381, 39.984358, 99.799628, 38.027891, 44.268308, 36.202204, 39.921811, 38.668774, 36.832286, 45.833218, 43.228963, 36.833273, 44.787401, 38.176476, 39.062471, 46.333559, 38.760314, 43.832852, 40.733288, 39.015338, 45.443516, 41.194251, 38.071494, 42.756496, 34.239351, 39.659011, 40.587958, 38.027891, 114.304283, 46.958354, 39.636801, 40.927870, 49.118094, 43.093642, 50.196436, 45.535041, 43.087415, 48.540036, 35.942528, 37.962886, 51.168782, 43.317775, 50.572459, 41.872521, 36.433413, 50.585268, 47.837969, 41.081064, 44.673874, 39.060978, 38.477483, 44.490750, 44.268308, 46.958354, 122.935323, 39.948695, 46.801841, 44.455283, 40.160668, 54.193098, 49.678271, 41.834745, 47.227606, 42.214571, 42.598524, 44.800998, 36.803445, 42.036961, 35.825264, 40.848178, 40.949970, 44.596875, 37.981693, 38.350523, 37.515228, 40.547021, 40.600474, 36.202204, 39.636801, 39.948695, 97.365126, 40.163209, 39.177628, 38.935283, 41.465246, 40.962743, 40.533287, 43.367907, 38.723316, 36.312733, 43.735417, 41.905953, 47.251940, 38.144457, 36.480813, 41.920556, 43.448257, 41.821252, 40.330611, 37.079566, 42.678061, 40.698368, 39.921811, 40.927870, 46.801841, 40.163209, 110.416786, 46.843429, 41.834126, 46.788801, 46.983780, 43.511429, 47.291825, 40.023523, 40.581819, 42.672057, 40.238334, 45.273068, 41.368293, 41.439981, 43.711898, 43.291878, 42.773603, 42.288511, 37.317791, 42.279104, 42.296794, 38.668774, 49.118094, 44.455283, 39.177628, 46.843429, 107.474576, 44.590023, 48.333476, 44.059916, 42.653703, 44.171623, 39.363181, 41.716539, 40.024492, 42.325769, 42.842437, 40.751910, 40.797598, 41.463472, 39.005953, 39.293957, 39.472844, 38.046576, 41.515782, 41.567854, 36.832286, 43.093642, 40.160668, 38.935283, 41.834126, 44.590023, 105.140579, 43.149105, 41.516560, 43.494333, 45.664210, 36.466241, 37.477898, 48.793499, 45.853665, 49.323737, 41.123673, 40.325652, 51.248836, 50.493111, 38.600491, 45.617102, 36.112222, 43.478416, 47.002908, 45.833218, 50.196436, 54.193098, 41.465246, 46.788801, 48.333476, 43.149105, 123.746816, 53.234332, 44.633908, 53.537592, 43.196327, 42.747181, 48.696170, 46.601722, 52.125739, 41.930358, 38.599119, 46.869144, 46.296591, 43.761301, 44.692618, 39.406838, 45.003800, 43.922434, 43.228963, 45.535041, 49.678271, 40.962743, 46.983780, 44.059916, 41.516560, 53.234332, 125.202062, 43.967875, 52.416619, 39.937196, 39.775405, 45.870016, 40.668861, 45.831747, 41.002915, 42.727171, 45.178199, 43.449036, 42.294750, 41.194977, 39.258432, 42.433639, 43.479144, 36.833273, 43.087415, 41.834745, 40.533287, 43.511429, 42.653703, 43.494333, 44.633908, 43.967875, 107.336922, 44.396001, 39.819884, 38.676633, 46.398093, 49.084078, 49.466716, 43.099168, 39.382845, 45.709593, 48.798961, 42.410289, 41.284030, 36.347136, 42.757336, 44.291425, 44.787401, 48.540036, 47.227606, 43.367907, 47.291825, 44.171623, 45.664210, 53.537592, 52.416619, 44.396001, 114.651847, 40.826050, 37.634130, 44.305791, 39.292553, 44.762183, 36.018699, 41.535989, 42.402465, 42.877859, 40.266469, 39.580938, 38.927619, 35.814356, 43.352951, 38.176476, 35.942528, 42.214571, 38.723316, 40.023523, 39.363181, 36.466241, 43.196327, 39.937196, 39.819884, 40.826050, 96.128345, 40.788606, 41.863013, 35.781804, 41.930313, 33.646602, 41.518779, 44.097412, 37.055014, 39.909538, 42.382268, 41.604838, 39.017848, 42.613649, 39.062471, 37.962886, 42.598524, 36.312733, 40.581819, 41.716539, 37.477898, 42.747181, 39.775405, 38.676633, 37.634130, 40.788606, 97.605849)
    val fml = new DoubleMatrix(Array[Double](-1219.296604, -1126.029219, -1202.257728, -1022.064083, -1047.414836, -1155.507387, -1169.502847, -1091.655366, -1063.832607, -1015.829142, -1075.864072, -1101.427162, -1058.907539, -1115.171116, -1205.015211, -1090.627084, -1143.206126, -1140.107801, -1100.285642, -1198.992795, -1246.276120, -1159.678276, -1194.177391, -1056.458015, -1058.791892))
    val ml = 25

    val Aeq = DoubleMatrix.ones(1, ml)
    val beq = DoubleMatrix.ones(1, 1)
    val lbeq = DoubleMatrix.zeros(ml, 1)
    val ubeq = DoubleMatrix.ones(ml, 1)
    val directQpMl = new QuadraticMinimizer(ml, Some(lbeq), Some(ubeq), Some(Aeq), Some(beq), true)
    directQpMl.setProximal(BOUNDS)

    val directQpStart = System.nanoTime()
    val directQpResult = directQpMl.solve(Hml, fml)
    val directQpTime = System.nanoTime() - directQpStart

    println("Sum " + directQpResult._1.sum())
    println("directQpResult " + directQpResult)
    println("directQpTime " + directQpTime / 1e3)

    println("Least Square Equality")
    val lsResult = Solve.solvePositive(Hml, fml)
    println(lsResult)

    println("Movielens L1")

    val Hl1 = new DoubleMatrix(25, 25, 253.535098, 236.477785, 234.421906, 223.374867, 241.007512, 204.695511, 226.465507, 223.351032, 249.179386, 221.411909, 238.679352, 203.579010, 217.564498, 243.852681, 266.607649, 213.994496, 241.620759, 223.602907, 220.038678, 264.704959, 240.341716, 223.672378, 244.323303, 223.216217, 226.074990, 236.477785, 278.862035, 245.756639, 237.489890, 252.783139, 214.077652, 241.816953, 238.790633, 260.536460, 228.121417, 255.103936, 216.608405, 237.150426, 258.933231, 281.958112, 228.971242, 252.508513, 234.242638, 240.308477, 285.416390, 254.792243, 240.176223, 259.048267, 235.566855, 236.277617, 234.421906, 245.756639, 269.162882, 231.416867, 251.321527, 208.134322, 236.567647, 236.558029, 255.805108, 226.535825, 251.514713, 212.770208, 228.565362, 261.748652, 273.946966, 227.411615, 252.767900, 232.823977, 233.084574, 278.315614, 250.872786, 235.227909, 255.104263, 238.931093, 235.402356, 223.374867, 237.489890, 231.416867, 254.771963, 241.703229, 209.028084, 231.517998, 228.768510, 250.805315, 216.548935, 245.473869, 207.687875, 222.036114, 250.906955, 263.018181, 216.128966, 244.445283, 227.436840, 231.369510, 270.721492, 242.475130, 226.471530, 248.130112, 225.826557, 228.266719, 241.007512, 252.783139, 251.321527, 241.703229, 285.702320, 219.051868, 249.442308, 240.400187, 264.970407, 232.503138, 258.819837, 220.160683, 235.621356, 267.743972, 285.795029, 229.667231, 260.870105, 240.751687, 247.183922, 289.044453, 260.715749, 244.210258, 267.159502, 242.992822, 244.070245, 204.695511, 214.077652, 208.134322, 209.028084, 219.051868, 210.164224, 208.151908, 201.539036, 226.373834, 192.056565, 219.950686, 191.459568, 195.982460, 226.739575, 240.677519, 196.116652, 217.352348, 203.533069, 204.581690, 243.603643, 217.785986, 204.205559, 223.747953, 203.586842, 200.165867, 226.465507, 241.816953, 236.567647, 231.517998, 249.442308, 208.151908, 264.007925, 227.080718, 253.174653, 220.322823, 248.619983, 210.100242, 223.279198, 254.807401, 269.896959, 222.927882, 247.017507, 230.484479, 233.358639, 274.935489, 249.237737, 235.229584, 253.029955, 228.601700, 230.512885, 223.351032, 238.790633, 236.558029, 228.768510, 240.400187, 201.539036, 227.080718, 258.773479, 249.471480, 215.664539, 243.078577, 202.337063, 221.020998, 249.979759, 263.356244, 213.470569, 246.182278, 225.727773, 229.873732, 266.295057, 242.954024, 225.510760, 249.370268, 227.813265, 232.141964, 249.179386, 260.536460, 255.805108, 250.805315, 264.970407, 226.373834, 253.174653, 249.471480, 302.360150, 237.902729, 265.769812, 224.947876, 243.088105, 273.690377, 291.076027, 241.089661, 267.772651, 248.459822, 249.662698, 295.935799, 267.460908, 255.668926, 275.902272, 248.495606, 246.827505, 221.411909, 228.121417, 226.535825, 216.548935, 232.503138, 192.056565, 220.322823, 215.664539, 237.902729, 245.154567, 234.956316, 199.557862, 214.774631, 240.339217, 255.161923, 209.328714, 232.277540, 216.298768, 220.296241, 253.817633, 237.638235, 220.785141, 239.098500, 220.583355, 218.962732, 238.679352, 255.103936, 251.514713, 245.473869, 258.819837, 219.950686, 248.619983, 243.078577, 265.769812, 234.956316, 288.133073, 225.087852, 239.810430, 268.406605, 283.289840, 233.858455, 258.306589, 240.263617, 246.844456, 290.492875, 267.212598, 243.218596, 265.681905, 244.615890, 242.543363, 203.579010, 216.608405, 212.770208, 207.687875, 220.160683, 191.459568, 210.100242, 202.337063, 224.947876, 199.557862, 225.087852, 217.501685, 197.897572, 229.825316, 242.175607, 201.123644, 219.820165, 202.894307, 211.468055, 246.048907, 225.135194, 210.076305, 226.806762, 212.014431, 205.123267, 217.564498, 237.150426, 228.565362, 222.036114, 235.621356, 195.982460, 223.279198, 221.020998, 243.088105, 214.774631, 239.810430, 197.897572, 244.439113, 241.621129, 260.400953, 216.482178, 236.805076, 216.680343, 223.816297, 263.188711, 236.311810, 222.950152, 244.636356, 219.121372, 219.911078, 243.852681, 258.933231, 261.748652, 250.906955, 267.743972, 226.739575, 254.807401, 249.979759, 273.690377, 240.339217, 268.406605, 229.825316, 241.621129, 302.928261, 288.344398, 238.549018, 267.239982, 248.073140, 254.230916, 296.789984, 267.158551, 252.226496, 271.170860, 248.325354, 253.694013, 266.607649, 281.958112, 273.946966, 263.018181, 285.795029, 240.677519, 269.896959, 263.356244, 291.076027, 255.161923, 283.289840, 242.175607, 260.400953, 288.344398, 343.457361, 257.368309, 284.795470, 263.122266, 271.239770, 320.209823, 283.933299, 264.416752, 292.035194, 268.764031, 265.345807, 213.994496, 228.971242, 227.411615, 216.128966, 229.667231, 196.116652, 222.927882, 213.470569, 241.089661, 209.328714, 233.858455, 201.123644, 216.482178, 238.549018, 257.368309, 239.295031, 234.913508, 218.066855, 219.648997, 257.969951, 231.243624, 224.657569, 238.158714, 217.174368, 215.933866, 241.620759, 252.508513, 252.767900, 244.445283, 260.870105, 217.352348, 247.017507, 246.182278, 267.772651, 232.277540, 258.306589, 219.820165, 236.805076, 267.239982, 284.795470, 234.913508, 289.709239, 241.312315, 247.249491, 286.702147, 264.252852, 245.151647, 264.582984, 240.842689, 245.837476, 223.602907, 234.242638, 232.823977, 227.436840, 240.751687, 203.533069, 230.484479, 225.727773, 248.459822, 216.298768, 240.263617, 202.894307, 216.680343, 248.073140, 263.122266, 218.066855, 241.312315, 255.363057, 230.209787, 271.091482, 239.220241, 225.387834, 247.486715, 226.052431, 224.119935, 220.038678, 240.308477, 233.084574, 231.369510, 247.183922, 204.581690, 233.358639, 229.873732, 249.662698, 220.296241, 246.844456, 211.468055, 223.816297, 254.230916, 271.239770, 219.648997, 247.249491, 230.209787, 264.014907, 271.938970, 246.664305, 227.889045, 249.908085, 232.035369, 229.010298, 264.704959, 285.416390, 278.315614, 270.721492, 289.044453, 243.603643, 274.935489, 266.295057, 295.935799, 253.817633, 290.492875, 246.048907, 263.188711, 296.789984, 320.209823, 257.969951, 286.702147, 271.091482, 271.938970, 352.825726, 286.200221, 267.716897, 297.182554, 269.776351, 266.721561, 240.341716, 254.792243, 250.872786, 242.475130, 260.715749, 217.785986, 249.237737, 242.954024, 267.460908, 237.638235, 267.212598, 225.135194, 236.311810, 267.158551, 283.933299, 231.243624, 264.252852, 239.220241, 246.664305, 286.200221, 294.042749, 246.504021, 269.570596, 243.980697, 242.690997, 223.672378, 240.176223, 235.227909, 226.471530, 244.210258, 204.205559, 235.229584, 225.510760, 255.668926, 220.785141, 243.218596, 210.076305, 222.950152, 252.226496, 264.416752, 224.657569, 245.151647, 225.387834, 227.889045, 267.716897, 246.504021, 259.897656, 251.730847, 229.335712, 229.759185, 244.323303, 259.048267, 255.104263, 248.130112, 267.159502, 223.747953, 253.029955, 249.370268, 275.902272, 239.098500, 265.681905, 226.806762, 244.636356, 271.170860, 292.035194, 238.158714, 264.582984, 247.486715, 249.908085, 297.182554, 269.570596, 251.730847, 303.872223, 251.585636, 247.878402, 223.216217, 235.566855, 238.931093, 225.826557, 242.992822, 203.586842, 228.601700, 227.813265, 248.495606, 220.583355, 244.615890, 212.014431, 219.121372, 248.325354, 268.764031, 217.174368, 240.842689, 226.052431, 232.035369, 269.776351, 243.980697, 229.335712, 251.585636, 257.544914, 228.810942, 226.074990, 236.277617, 235.402356, 228.266719, 244.070245, 200.165867, 230.512885, 232.141964, 246.827505, 218.962732, 242.543363, 205.123267, 219.911078, 253.694013, 265.345807, 215.933866, 245.837476, 224.119935, 229.010298, 266.721561, 242.690997, 229.759185, 247.878402, 228.810942, 253.353769)
    val fl1 = new DoubleMatrix(25, 1, -892.842851, -934.071560, -932.936015, -888.124343, -961.050207, -791.191087, -923.711397, -904.289301, -988.384984, -883.909133, -959.465030, -798.551172, -871.622303, -997.463289, -1043.912620, -863.013719, -976.975712, -897.033693, -898.694786, -1069.245497, -963.491924, -901.263474, -983.768031, -899.865392, -902.283567)

    val qpSolverMlL1 = QuadraticMinimizer(25, SPARSE, 2.0)

    val qpMlL1Result = qpSolverMlL1.solve(Hl1, fl1)

    val octaveL1 = DenseVector(0.18611, 0.00000, 0.06317, -0.10417, 0.11262,
      -0.20495, 0.52668, 0.32790, 0.19421, 0.72180,
      0.06309, -0.41326, -0.00000, 0.52078, -0.00000,
      0.18040, 0.62915, 0.16329, -0.06424, 0.37539,
      0.01659, 0.00000, 0.11215, 0.24778, 0.04082)

    println("Standalone results")
    println(qpMlL1Result)

    println("Octave results")
    println(octaveL1)

    println(s"L1 iterations ${qpSolverMlL1.iterations}")

    //Movielens Positive/Bounds high iterations
    println(s"Movielens Example")
    val P1 = new DoubleMatrix(20, 20, 539.101887, 494.598042, 505.700671, 505.846716, 504.700928, 516.629473, 507.958246, 514.096818, 514.801371, 505.735357, 507.322795, 522.547578, 498.320793, 502.829895, 505.847128, 488.934012, 516.116942, 501.906569, 505.627629, 496.409513, 494.598042, 565.723334, 513.239749, 519.155649, 514.070934, 524.154020, 521.694985, 523.512877, 521.122745, 513.862711, 518.653059, 530.426712, 511.054588, 510.096410, 521.373582, 503.132142, 531.736861, 514.161101, 515.005997, 500.799198, 505.700671, 513.239749, 602.920633, 524.780488, 547.978722, 558.807137, 526.999189, 553.273432, 552.657103, 547.690555, 537.912646, 563.616990, 527.634170, 541.947698, 524.060188, 507.650395, 534.403391, 534.406246, 546.625588, 535.221534, 505.846716, 519.155649, 524.780488, 585.686194, 522.548624, 537.124362, 534.911663, 530.505003, 533.364761, 525.544862, 530.149606, 543.063850, 518.884670, 517.707324, 531.252004, 511.635097, 541.217141, 522.706817, 526.063019, 513.574796, 504.700928, 514.070934, 547.978722, 522.548624, 602.711620, 557.824564, 523.454584, 556.453086, 551.975932, 548.308951, 537.908414, 562.811202, 530.685949, 544.687533, 523.961961, 507.966023, 534.868428, 535.823631, 546.491009, 534.209994, 516.629473, 524.154020, 558.807137, 537.124362, 557.824564, 624.863357, 539.305550, 566.795282, 566.357133, 560.044835, 550.021868, 578.832952, 538.509515, 554.979692, 535.459614, 518.812719, 546.530562, 546.168894, 558.181118, 548.792405, 507.958246, 521.694985, 526.999189, 534.911663, 523.454584, 539.305550, 591.413305, 532.696713, 536.852122, 527.232598, 531.751544, 545.578374, 520.832790, 521.212336, 533.128446, 513.908219, 544.084087, 525.037440, 527.089172, 515.549361, 514.096818, 523.512877, 553.273432, 530.505003, 556.453086, 566.795282, 532.696713, 621.615462, 562.996181, 554.493454, 545.658444, 574.476466, 538.757428, 555.556328, 533.365769, 517.189079, 543.997925, 544.795736, 554.632016, 544.157448, 514.801371, 521.122745, 552.657103, 533.364761, 551.975932, 566.357133, 536.852122, 562.996181, 621.891951, 552.016433, 544.239294, 573.926455, 532.639189, 553.095240, 530.294369, 516.269094, 544.643533, 537.922422, 549.757806, 545.057129, 505.735357, 513.862711, 547.690555, 525.544862, 548.308951, 560.044835, 527.232598, 554.493454, 552.016433, 603.860301, 539.638699, 564.591708, 529.636837, 543.558799, 524.759472, 506.968931, 534.506284, 537.156481, 547.911779, 536.271289, 507.322795, 518.653059, 537.912646, 530.149606, 537.908414, 550.021868, 531.751544, 545.658444, 544.239294, 539.638699, 589.023204, 554.623574, 528.460278, 532.243589, 530.559000, 512.720156, 539.864087, 531.591296, 538.972277, 527.533747, 522.547578, 530.426712, 563.616990, 543.063850, 562.811202, 578.832952, 545.578374, 574.476466, 573.926455, 564.591708, 554.623574, 638.367746, 544.269457, 561.616144, 541.245807, 524.820018, 552.498719, 552.102404, 563.397777, 554.806495, 498.320793, 511.054588, 527.634170, 518.884670, 530.685949, 538.509515, 520.832790, 538.757428, 532.639189, 529.636837, 528.460278, 544.269457, 576.086269, 524.503702, 521.427126, 503.797286, 531.685079, 525.230922, 529.783815, 520.222911, 502.829895, 510.096410, 541.947698, 517.707324, 544.687533, 554.979692, 521.212336, 555.556328, 553.095240, 543.558799, 532.243589, 561.616144, 524.503702, 597.742352, 520.329969, 504.306186, 530.688065, 531.679956, 541.233986, 531.982099, 505.847128, 521.373582, 524.060188, 531.252004, 523.961961, 535.459614, 533.128446, 533.365769, 530.294369, 524.759472, 530.559000, 541.245807, 521.427126, 520.329969, 586.613767, 513.362642, 542.438988, 524.690396, 526.354706, 511.243067, 488.934012, 503.132142, 507.650395, 511.635097, 507.966023, 518.812719, 513.908219, 517.189079, 516.269094, 506.968931, 512.720156, 524.820018, 503.797286, 504.306186, 513.362642, 550.229150, 523.966242, 506.244433, 507.769334, 495.459277, 516.116942, 531.736861, 534.403391, 541.217141, 534.868428, 546.530562, 544.084087, 543.997925, 544.643533, 534.506284, 539.864087, 552.498719, 531.685079, 530.688065, 542.438988, 523.966242, 606.957114, 533.719037, 534.295092, 522.409881, 501.906569, 514.161101, 534.406246, 522.706817, 535.823631, 546.168894, 525.037440, 544.795736, 537.922422, 537.156481, 531.591296, 552.102404, 525.230922, 531.679956, 524.690396, 506.244433, 533.719037, 583.853405, 536.033903, 522.009144, 505.627629, 515.005997, 546.625588, 526.063019, 546.491009, 558.181118, 527.089172, 554.632016, 549.757806, 547.911779, 538.972277, 563.397777, 529.783815, 541.233986, 526.354706, 507.769334, 534.295092, 536.033903, 599.942302, 535.001472, 496.409513, 500.799198, 535.221534, 513.574796, 534.209994, 548.792405, 515.549361, 544.157448, 545.057129, 536.271289, 527.533747, 554.806495, 520.222911, 531.982099, 511.243067, 495.459277, 522.409881, 522.009144, 535.001472, 593.140288)
    val q1 = new DoubleMatrix(20, 1, -1880.240029, -1920.949941, -2030.476172, -1956.642164, -2021.502985, -2090.503157, -1965.934820, -2072.855628, -2098.075034, -2035.059185, -1999.005923, -2121.515181, -1944.759586, -2035.397706, -1939.872057, -1888.635008, -1986.031605, -1973.738457, -2024.468051, -2003.765736)

    val qpIters = QuadraticMinimizer(20, POSITIVE, 0.0)
    val (qpItersResult, converged) = qpIters.solve(P1, q1)

    val ws = NNLS.createWorkspace(20)
    val nnlsResult = NNLS.solve(P1, q1.muli(-1), ws)
    q1.muli(-1)

    println(s"QpIters ${qpIters.iterations} solveTime ${qpIters.solveTime / 1e6} NNLS iters ${ws.iterations}")

    //NNLS test
    val P2 = new DoubleMatrix(20, 20, 333907.312770, -60814.043975, 207935.829941, -162881.367739, -43730.396770, 17511.428983, -243340.496449, -225245.957922, 104700.445881, 32430.845099, 336378.693135, -373497.970207, -41147.159621, 53928.060360, -293517.883778, 53105.278068, 0.000000, -85257.781696, 84913.970469, -10584.080103, -60814.043975, 13826.806664, -38032.612640, 33475.833875, 10791.916809, -1040.950810, 48106.552472, 45390.073380, -16310.282190, -2861.455903, -60790.833191, 73109.516544, 9826.614644, -8283.992464, 56991.742991, -6171.366034, 0.000000, 19152.382499, -13218.721710, 2793.734234, 207935.829941, -38032.612640, 129661.677608, -101682.098412, -27401.299347, 10787.713362, -151803.006149, -140563.601672, 65067.935324, 20031.263383, 209521.268600, -232958.054688, -25764.179034, 33507.951918, -183046.845592, 32884.782835, 0.000000, -53315.811196, 52770.762546, -6642.187643, -162881.367739, 33475.833875, -101682.098412, 85094.407608, 25422.850782, -5437.646141, 124197.166330, 116206.265909, -47093.484134, -11420.168521, -163429.436848, 189574.783900, 23447.172314, -24087.375367, 148311.355507, -20848.385466, 0.000000, 46835.814559, -38180.352878, 6415.873901, -43730.396770, 10791.916809, -27401.299347, 25422.850782, 8882.869799, 15.638084, 35933.473986, 34186.371325, -10745.330690, -974.314375, -43537.709621, 54371.010558, 7894.453004, -5408.929644, 42231.381747, -3192.010574, 0.000000, 15058.753110, -8704.757256, 2316.581535, 17511.428983, -1040.950810, 10787.713362, -5437.646141, 15.638084, 2794.949847, -9681.950987, -8258.171646, 7754.358930, 4193.359412, 18052.143842, -15456.096769, -253.356253, 4089.672804, -12524.380088, 5651.579348, 0.000000, -1513.302547, 6296.461898, 152.427321, -243340.496449, 48106.552472, -151803.006149, 124197.166330, 35933.473986, -9681.950987, 182931.600236, 170454.352953, -72361.174145, -19270.461728, -244518.179729, 279551.060579, 33340.452802, -37103.267653, 219025.288975, -33687.141423, 0.000000, 67347.950443, -58673.009647, 8957.800259, -225245.957922, 45390.073380, -140563.601672, 116206.265909, 34186.371325, -8258.171646, 170454.352953, 159322.942894, -66074.960534, -16839.743193, -226173.967766, 260421.044094, 31624.194003, -33839.612565, 203889.695169, -30034.828909, 0.000000, 63525.040745, -53572.741748, 8575.071847, 104700.445881, -16310.282190, 65067.935324, -47093.484134, -10745.330690, 7754.358930, -72361.174145, -66074.960534, 35869.598076, 13378.653317, 106033.647837, -111831.682883, -10455.465743, 18537.392481, -88370.612394, 20344.288488, 0.000000, -22935.482766, 29004.543704, -2409.461759, 32430.845099, -2861.455903, 20031.263383, -11420.168521, -974.314375, 4193.359412, -19270.461728, -16839.743193, 13378.653317, 6802.081898, 33256.395091, -30421.985199, -1296.785870, 7026.518692, -24443.378205, 9221.982599, 0.000000, -4088.076871, 10861.014242, -25.092938, 336378.693135, -60790.833191, 209521.268600, -163429.436848, -43537.709621, 18052.143842, -244518.179729, -226173.967766, 106033.647837, 33256.395091, 339200.268106, -375442.716811, -41027.594509, 54636.778527, -295133.248586, 54177.278365, 0.000000, -85237.666701, 85996.957056, -10503.209968, -373497.970207, 73109.516544, -232958.054688, 189574.783900, 54371.010558, -15456.096769, 279551.060579, 260421.044094, -111831.682883, -30421.985199, -375442.716811, 427793.208465, 50528.074431, -57375.986301, 335203.382015, -52676.385869, 0.000000, 102368.307670, -90679.792485, 13509.390393, -41147.159621, 9826.614644, -25764.179034, 23447.172314, 7894.453004, -253.356253, 33340.452802, 31624.194003, -10455.465743, -1296.785870, -41027.594509, 50528.074431, 7255.977434, -5281.636812, 39298.355527, -3440.450858, 0.000000, 13717.870243, -8471.405582, 2071.812204, 53928.060360, -8283.992464, 33507.951918, -24087.375367, -5408.929644, 4089.672804, -37103.267653, -33839.612565, 18537.392481, 7026.518692, 54636.778527, -57375.986301, -5281.636812, 9735.061160, -45360.674033, 10634.633559, 0.000000, -11652.364691, 15039.566630, -1202.539106, -293517.883778, 56991.742991, -183046.845592, 148311.355507, 42231.381747, -12524.380088, 219025.288975, 203889.695169, -88370.612394, -24443.378205, -295133.248586, 335203.382015, 39298.355527, -45360.674033, 262923.925938, -42012.606885, 0.000000, 79810.919951, -71657.856143, 10464.327491, 53105.278068, -6171.366034, 32884.782835, -20848.385466, -3192.010574, 5651.579348, -33687.141423, -30034.828909, 20344.288488, 9221.982599, 54177.278365, -52676.385869, -3440.450858, 10634.633559, -42012.606885, 13238.686902, 0.000000, -8739.845698, 16511.872845, -530.252003, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 118.430000, 0.000000, 0.000000, 0.000000, -85257.781696, 19152.382499, -53315.811196, 46835.814559, 15058.753110, -1513.302547, 67347.950443, 63525.040745, -22935.482766, -4088.076871, -85237.666701, 102368.307670, 13717.870243, -11652.364691, 79810.919951, -8739.845698, 0.000000, 26878.133950, -18588.407734, 3894.934299, 84913.970469, -13218.721710, 52770.762546, -38180.352878, -8704.757256, 6296.461898, -58673.009647, -53572.741748, 29004.543704, 10861.014242, 85996.957056, -90679.792485, -8471.405582, 15039.566630, -71657.856143, 16511.872845, 0.000000, -18588.407734, 23649.538538, -1951.083671, -10584.080103, 2793.734234, -6642.187643, 6415.873901, 2316.581535, 152.427321, 8957.800259, 8575.071847, -2409.461759, -25.092938, -10503.209968, 13509.390393, 2071.812204, -1202.539106, 10464.327491, -530.252003, 0.000000, 3894.934299, -1951.083671, 738.955915)
    val q2 = new DoubleMatrix(20, 1, 31755.057100, -13047.148129, 20191.244430, -25993.775800, -11963.550172, -4272.425977, -33569.856044, -33451.387021, 2320.764250, -5333.136834, 30633.764272, -49513.939049, -10351.230305, 872.276714, -37634.078430, -4628.338543, -0.000000, -18109.093788, 1856.725521, -3397.693211)

    val nnlsResult2 = NNLS.solve(P2, q2.mul(-1), ws)
    println(s"NNLS iters ${ws.iterations} result ${nnlsResult2.toList.mkString(",")}")

    val (posResult2, posConverged2) = qpIters.solve(P2, q2)
    println(s"QuadraticMinimizer iters ${qpIters.iterations} result ${posResult2.toString()}")

    //Movielens Sparse high iterations
    println(s"Movielens Sparse High Iterations")

    //Ill conditioned problems
    //ALS: Diagnosing userOrProduct 34 lambdaL2 6.500000000000006E-4 lambdaL1 0.06435
    val Psparse = new DoubleMatrix(20, 20, 0.172933, 0.121672, 0.083271, 0.081677, 0.053990, 0.060374, 0.122440, 0.057874, 0.202366, 0.058154, 0.221297, 0.098373, 0.027677, 0.208701, 0.174751, 0.133861, 0.143158, 0.221107, 0.180647, 0.059440, 0.121672, 0.107064, 0.044376, 0.055041, 0.020705, 0.040884, 0.080482, 0.049619, 0.105660, 0.054398, 0.195126, 0.069424, 0.024926, 0.116618, 0.075244, 0.115016, 0.125965, 0.148266, 0.150126, 0.043844, 0.083271, 0.044376, 0.057854, 0.042197, 0.040014, 0.023171, 0.074746, 0.018888, 0.114484, 0.012764, 0.071563, 0.057906, 0.007638, 0.137569, 0.114753, 0.043443, 0.046860, 0.106226, 0.069941, 0.033199, 0.081677, 0.055041, 0.042197, 0.041480, 0.028744, 0.029312, 0.059437, 0.026284, 0.103280, 0.025698, 0.099548, 0.046935, 0.012378, 0.104832, 0.091798, 0.060753, 0.064436, 0.107269, 0.082718, 0.028049, 0.053990, 0.020705, 0.040014, 0.028744, 0.036006, 0.022218, 0.043569, 0.010584, 0.102747, 0.006677, 0.034772, 0.030206, 0.003981, 0.094862, 0.103568, 0.024249, 0.022693, 0.079784, 0.036521, 0.016478, 0.060374, 0.040884, 0.023171, 0.029312, 0.022218, 0.041842, 0.021271, 0.025349, 0.110247, 0.030934, 0.087805, 0.012667, 0.012995, 0.053306, 0.089922, 0.058710, 0.056007, 0.099385, 0.063017, 0.008061, 0.122440, 0.080482, 0.074746, 0.059437, 0.043569, 0.021271, 0.120124, 0.031163, 0.115960, 0.022294, 0.127774, 0.098052, 0.013227, 0.190554, 0.114633, 0.071859, 0.083771, 0.136461, 0.119578, 0.057799, 0.057874, 0.049619, 0.018888, 0.026284, 0.010584, 0.025349, 0.031163, 0.027251, 0.062063, 0.029397, 0.096479, 0.025963, 0.012966, 0.048467, 0.044052, 0.058668, 0.062030, 0.077193, 0.071494, 0.016748, 0.202366, 0.105660, 0.114484, 0.103280, 0.102747, 0.110247, 0.115960, 0.062063, 0.385655, 0.064201, 0.210133, 0.076002, 0.029096, 0.267930, 0.351138, 0.143235, 0.134931, 0.318630, 0.172262, 0.043164, 0.058154, 0.054398, 0.012764, 0.025698, 0.006677, 0.030934, 0.022294, 0.029397, 0.064201, 0.038538, 0.110689, 0.018742, 0.015550, 0.033089, 0.039631, 0.068239, 0.070888, 0.081952, 0.077663, 0.013057, 0.221297, 0.195126, 0.071563, 0.099548, 0.034772, 0.087805, 0.127774, 0.096479, 0.210133, 0.110689, 0.375374, 0.109388, 0.049333, 0.187174, 0.143602, 0.223740, 0.240412, 0.283103, 0.278892, 0.070417, 0.098373, 0.069424, 0.057906, 0.046935, 0.030206, 0.012667, 0.098052, 0.025963, 0.076002, 0.018742, 0.109388, 0.085273, 0.011164, 0.149983, 0.074667, 0.059915, 0.071766, 0.103003, 0.101268, 0.049539, 0.027677, 0.024926, 0.007638, 0.012378, 0.003981, 0.012995, 0.013227, 0.012966, 0.029096, 0.015550, 0.049333, 0.011164, 0.008698, 0.019803, 0.019141, 0.030083, 0.031669, 0.037444, 0.035675, 0.007410, 0.208701, 0.116618, 0.137569, 0.104832, 0.094862, 0.053306, 0.190554, 0.048467, 0.267930, 0.033089, 0.187174, 0.149983, 0.019803, 0.342885, 0.268144, 0.111535, 0.122612, 0.258983, 0.181204, 0.086551, 0.174751, 0.075244, 0.114753, 0.091798, 0.103568, 0.089922, 0.114633, 0.044052, 0.351138, 0.039631, 0.143602, 0.074667, 0.019141, 0.268144, 0.338802, 0.101363, 0.092574, 0.275155, 0.129493, 0.040859, 0.133861, 0.115016, 0.043443, 0.060753, 0.024249, 0.058710, 0.071859, 0.058668, 0.143235, 0.068239, 0.223740, 0.059915, 0.030083, 0.111535, 0.101363, 0.137992, 0.143845, 0.178542, 0.165663, 0.038686, 0.143158, 0.125965, 0.046860, 0.064436, 0.022693, 0.056007, 0.083771, 0.062030, 0.134931, 0.070888, 0.240412, 0.071766, 0.031669, 0.122612, 0.092574, 0.143845, 0.156765, 0.182358, 0.180036, 0.046104, 0.221107, 0.148266, 0.106226, 0.107269, 0.079784, 0.099385, 0.136461, 0.077193, 0.318630, 0.081952, 0.283103, 0.103003, 0.037444, 0.258983, 0.275155, 0.178542, 0.182358, 0.314775, 0.224864, 0.061995, 0.180647, 0.150126, 0.069941, 0.082718, 0.036521, 0.063017, 0.119578, 0.071494, 0.172262, 0.077663, 0.278892, 0.101268, 0.035675, 0.181204, 0.129493, 0.165663, 0.180036, 0.224864, 0.218079, 0.063453, 0.059440, 0.043844, 0.033199, 0.028049, 0.016478, 0.008061, 0.057799, 0.016748, 0.043164, 0.013057, 0.070417, 0.049539, 0.007410, 0.086551, 0.040859, 0.038686, 0.046104, 0.061995, 0.063453, 0.031610)
    val qsparse = new DoubleMatrix(20, 1, -2.631990, -1.894166, -1.225159, -1.254382, -0.816634, -1.018898, -1.763319, -0.929908, -3.243997, -0.969543, -3.519254, -1.402682, -0.451124, -3.055922, -2.747532, -2.151673, -2.272190, -3.494880, -2.813748, -0.852818)

    val qpSparse = QuadraticMinimizer(20, SPARSE, 0.06435)

    val (qpSparseGold, convergedGold) = qpSparse.solve(Psparse, qsparse)
    println(s"sparseQpIteres ${qpSparse.iterations}")
    println(qpSparseGold.toString())

    qpSparse.setProximal(POSITIVE).setLambda(0.0)

    for (i <- 0 until 10) {
      qpSparse.solve(P1, q1)
      println(s"Positive QP iters ${qpSparse.iterations}")
    }

    qpSparse.setProximal(SPARSE).setLambda(0.06435)
    
    for (i <- 0 until 10) {
      val (qpSparseResult, convergedSparse) = qpSparse.solve(Psparse, qsparse)
      println(s"sparseQpIters ${qpSparse.iterations} solveTime ${qpSparse.solveTime / 1e6}")
      assert(qpSparseResult.sub(qpSparseGold).norm2() < 1E-8)
    }

    println(s"Generating randomized QPs with rank ${problemSize} equalities ${nequalities}")
    val (aeq, b, bl, bu, q, h) = QpGenerator.generate(problemSize, nequalities)
    
    val lambdaL1 = lambda*beta
    val lambdaL2 = lambda*(1-beta)
    
    val L2regularization = DoubleMatrix.eye(h.rows).muli(lambdaL2)
    
    val sparseQp = QuadraticMinimizer(h.rows, SPARSE, lambdaL1)
    val (sparseQpResult, sparseQpConverged) = sparseQp.solve(h.add(L2regularization), q)
    
    println(s"sparseQp ${sparseQp.solveTime/1e6} ms iterations ${sparseQp.iterations} converged $sparseQpConverged")
    
    val posQp = new QuadraticMinimizer(h.rows).setProximal(POSITIVE)
    val (posQpResult, posQpConverged) = posQp.solve(h,q)
    
    println(s"posQp ${posQp.solveTime/1e6} ms iterations ${posQp.iterations} converged $posQpConverged")
    
    val boundsQp = new QuadraticMinimizer(h.rows, Some(bl), Some(bu)).setProximal(BOUNDS)
    val (boundsQpResult, boundsQpConverged) = boundsQp.solve(h, q)
    
    println(s"boundsQp ${boundsQp.solveTime/1e6} ms iterations ${boundsQp.iterations} converged $boundsQpConverged")
    
    val qpEquality = new QuadraticMinimizer(h.rows, None, None, Some(Aeq), Some(beq), true).setProximal(POSITIVE)
    val (qpEqualityResult, qpEqualityConverged) = qpEquality.solve(h, q)
    
    println(s"Qp Equality ${qpEquality.solveTime / 1e6} ms iterations ${qpEquality.iterations} converged $qpEqualityConverged")
  }
}