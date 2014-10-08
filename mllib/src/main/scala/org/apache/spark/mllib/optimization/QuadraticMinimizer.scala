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
import breeze.linalg.CSCMatrix
import org.jblas.SimpleBlas
import org.netlib.util.intW
import org.jblas.exceptions.LapackArgumentException
import org.jblas.NativeBlas
import org.apache.spark.mllib.optimization.Constraint._
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
 * g(x) represents the following constraints which covers matrix factorization usecases
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

  //Default value of alpha and rho, these are cross-validation parameters
  //Based on the iteration difference within interior point method and ADMM they need to be modified
  var alpha: Double = 0.0
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

  var z = DoubleMatrix.zeros(nGram, 1)
  var u = DoubleMatrix.zeros(nGram, 1)

  var zOld = DoubleMatrix.zeros(nGram, 1)
  var xHat = DoubleMatrix.zeros(nGram, 1)
  
  //scale will hold q + linearEqualities
  var scale = DoubleMatrix.zeros(n, 1)

  var residual = DoubleMatrix.zeros(nGram, 1)
  var s = DoubleMatrix.zeros(nGram, 1)

  var lambda: Double = 1.0
  var constraint: Constraint = SMOOTH
  
  var R: DoubleMatrix = null
  var pivot: Array[Int] = null

  /* If Aeq exists and rows > 1, cache the pseudo-inverse to be used later */
  /* If Aeq exist and rows = 1, cache the transpose */
  val invAeq = if (Aeq != None && Aeq.get.rows > 1) Some(Solve.pinv(Aeq.get))
  else None

  def setLambda(lambda: Double): QuadraticMinimizer = {
    this.lambda = lambda
    this
  }

  //TO DO : This can take a proximal function as input
  //TO DO : alpha needs to be scaled based on Nesterov's acceleration
  def setProximal(constraint: Constraint): QuadraticMinimizer = {
    this.constraint = constraint
    //Splitting method for optimal control Boyd et al
    //For all instances below we chose alpha = 50 and relaxation parameter as 2.0
    if (addEqualityToGram) {
      rho = 50.0
    } else {
      rho = 1.0
    }
    alpha = 1.0
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
        // x = U \ (L \ q);
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

      //rho update based on norm
      /*
      if(residualNorm > 10*sNorm) rho = 2*rho
      else if(sNorm > 10*residualNorm) rho = rho/2.0
      */

      //TO DO : Make sure z.muli(-1) is actually needed in norm calculation
      //residual = -z
      residual.fill(0).addi(z).muli(-1)
      //s = rho*u
      s.fill(0).addi(u).muli(rho)

      val epsPrimal = sqrt(n) * ABSTOL + RELTOL * max(x.norm2(), residual.norm2())
      val epsDual = sqrt(n) * ABSTOL + RELTOL * s.norm2()

      if (residualNorm < epsPrimal && sNorm < epsDual) {
        iterations += k
        return (x, true)
      }
      k += 1
    }
    iterations += MAX_ITER
    (z, false)
  }

  def solve(H: DoubleMatrix, q: DoubleMatrix): (DoubleMatrix, Boolean) = {
    for (i <- 0 until H.rows)
      for (j <- 0 until H.columns) {
        val h = H.get(i, j)
        if (i == j) wsH.put(i, j, rho + h)
        else wsH.put(i, j, h)
      }
    val solveStart = System.nanoTime()
    val result = solve(q)
    solveTime += System.nanoTime() - solveStart
    result
  }
}

/*
function [A,b,bl,bu,c,d1,d2,H] = toydata( m,n )

%        [A,b,bl,bu,c,d1,d2] = toydata( m,n );
%        defines an m by n matrix A, rhs vector b, and cost vector c
%        for use with pdco.m.

  rand('state',10);
  density = 0.1;
  rc      = 1e-1;

  em      = ones(m,1);
  en      = ones(n,1);
  zn      = zeros(n,1);
  
  A       = sprand(m,n,density,rc);
  x       = en;

  gamma   = 1e-3;       % Primal regularization
  delta   = 1e-3;       % 1e-3 or 1e-4 for LP;  1 for Least squares.

  d1      = gamma;      % Can be scalar if D1 = d1*I.
  d2      = delta*em;

  b       = full(A*x);
  c       = rand(n,1);

  bl      = zn;         % x > 0
  bu      = 10*en;      % x < 10

  density = 0.05;
  H       = sprand(n,n,density,rc);
  H       = H'*H;
  H       = (H+H')/2;
*/
object QpGenerator {
  /* Generates random instances of Quadratic Programming Problems
   * 0.5x'Px + q'x
   * s.t Ax = b
   * 	lb <= x <= ub
   *  
   *  @param1 A
   *  @param2 b
   *  @param3 lb
   *  @param4 ub
   *  @param5 q
   *  @param6 H
   */
  def generate(nHessian: Int, nEqualities: Int): (DoubleMatrix, DoubleMatrix, DoubleMatrix, DoubleMatrix, DoubleMatrix, DoubleMatrix) = {
    val em = DoubleMatrix.ones(nEqualities, 1)
    val en = DoubleMatrix.ones(nHessian, 1)
    val zn = DoubleMatrix.zeros(nHessian, 1)

    val A = DoubleMatrix.rand(nEqualities, nHessian)
    val x = en

    val b = A.mmul(x)
    val q = DoubleMatrix.rand(nHessian, 1)

    val lb = zn
    val ub = en.mul(10)

    val hrand = DoubleMatrix.rand(nHessian, nHessian)
    val hrandt = hrand.transpose()
    val hposdef = hrandt.mmul(hrand)
    val H = hposdef.transpose().add(hposdef)
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
    if (args.length < 2) {
      println("Usage: QpSolver n m")
      println("Test QpSolver with a simple quadratic function of dimension n and m equalities")
      sys.exit(1)
    }

    val problemSize = args(0).toInt
    val nequalities = args(1).toInt

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

    println("dposvResult " + DenseVector(dposvResult))

    val H = DoubleMatrix.eye(problemSize).mul(2.0)
    val f = DoubleMatrix.zeros(problemSize, 1).add(-6.0)

    val alpha = 1.0
    val rho = 1.0

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

    val qpSolverPos = new QuadraticMinimizer(n, None, None, None).setProximal(POSITIVE)
    val posResult = qpSolverPos.solve(ata, atb.muli(-1))

    for (i <- 0 until n) {
      println(posResult._1.get(i) + " " + goodx(i))
      assert(Math.abs(posResult._1.get(i) - goodx(i)) < 1e-4)
    }

    val goodBounds: DoubleMatrix = DoubleMatrix.zeros(n, 1)
    goodBounds.put(0, 0, 0.0)
    goodBounds.put(1, 0, 0.25000000000236045)
    goodBounds.put(2, 0, 0.2499999999945758)
    goodBounds.put(3, 0, 0.0)
    goodBounds.put(4, 0, 0.0)

    val lb = DoubleMatrix.zeros(problemSize, 1)
    val ub = DoubleMatrix.zeros(problemSize, 1).addi(0.25)
    val qpSolverBounds = new QuadraticMinimizer(n, Some(lb), Some(ub), None).setProximal(BOUNDS)
    val boundsResult = qpSolverBounds.solve(ata, atb)
    println("Bounds result check " + (boundsResult._1.subi(goodBounds).norm2() < 1e-4))

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

    println("Spark tests")
    val Htest = DoubleMatrix.zeros(1, 1)

    Htest.put(0, 0, 1.123621)
    val ftest = DoubleMatrix.zeros(1, 1)
    ftest.put(0, 0, -0.521311)

    val testSolver = new QuadraticMinimizer(1)
    val testResult = testSolver.solve(Htest, ftest)
    println(testResult)
    println(Solve.solvePositive(Htest, ftest.mul(-1)))

    val testResult1 = testSolver.solve(Htest, ftest)
    println(testResult1)

    println("Movielens L1")

    val Hl1 = new DoubleMatrix(25, 25, 253.535098, 236.477785, 234.421906, 223.374867, 241.007512, 204.695511, 226.465507, 223.351032, 249.179386, 221.411909, 238.679352, 203.579010, 217.564498, 243.852681, 266.607649, 213.994496, 241.620759, 223.602907, 220.038678, 264.704959, 240.341716, 223.672378, 244.323303, 223.216217, 226.074990, 236.477785, 278.862035, 245.756639, 237.489890, 252.783139, 214.077652, 241.816953, 238.790633, 260.536460, 228.121417, 255.103936, 216.608405, 237.150426, 258.933231, 281.958112, 228.971242, 252.508513, 234.242638, 240.308477, 285.416390, 254.792243, 240.176223, 259.048267, 235.566855, 236.277617, 234.421906, 245.756639, 269.162882, 231.416867, 251.321527, 208.134322, 236.567647, 236.558029, 255.805108, 226.535825, 251.514713, 212.770208, 228.565362, 261.748652, 273.946966, 227.411615, 252.767900, 232.823977, 233.084574, 278.315614, 250.872786, 235.227909, 255.104263, 238.931093, 235.402356, 223.374867, 237.489890, 231.416867, 254.771963, 241.703229, 209.028084, 231.517998, 228.768510, 250.805315, 216.548935, 245.473869, 207.687875, 222.036114, 250.906955, 263.018181, 216.128966, 244.445283, 227.436840, 231.369510, 270.721492, 242.475130, 226.471530, 248.130112, 225.826557, 228.266719, 241.007512, 252.783139, 251.321527, 241.703229, 285.702320, 219.051868, 249.442308, 240.400187, 264.970407, 232.503138, 258.819837, 220.160683, 235.621356, 267.743972, 285.795029, 229.667231, 260.870105, 240.751687, 247.183922, 289.044453, 260.715749, 244.210258, 267.159502, 242.992822, 244.070245, 204.695511, 214.077652, 208.134322, 209.028084, 219.051868, 210.164224, 208.151908, 201.539036, 226.373834, 192.056565, 219.950686, 191.459568, 195.982460, 226.739575, 240.677519, 196.116652, 217.352348, 203.533069, 204.581690, 243.603643, 217.785986, 204.205559, 223.747953, 203.586842, 200.165867, 226.465507, 241.816953, 236.567647, 231.517998, 249.442308, 208.151908, 264.007925, 227.080718, 253.174653, 220.322823, 248.619983, 210.100242, 223.279198, 254.807401, 269.896959, 222.927882, 247.017507, 230.484479, 233.358639, 274.935489, 249.237737, 235.229584, 253.029955, 228.601700, 230.512885, 223.351032, 238.790633, 236.558029, 228.768510, 240.400187, 201.539036, 227.080718, 258.773479, 249.471480, 215.664539, 243.078577, 202.337063, 221.020998, 249.979759, 263.356244, 213.470569, 246.182278, 225.727773, 229.873732, 266.295057, 242.954024, 225.510760, 249.370268, 227.813265, 232.141964, 249.179386, 260.536460, 255.805108, 250.805315, 264.970407, 226.373834, 253.174653, 249.471480, 302.360150, 237.902729, 265.769812, 224.947876, 243.088105, 273.690377, 291.076027, 241.089661, 267.772651, 248.459822, 249.662698, 295.935799, 267.460908, 255.668926, 275.902272, 248.495606, 246.827505, 221.411909, 228.121417, 226.535825, 216.548935, 232.503138, 192.056565, 220.322823, 215.664539, 237.902729, 245.154567, 234.956316, 199.557862, 214.774631, 240.339217, 255.161923, 209.328714, 232.277540, 216.298768, 220.296241, 253.817633, 237.638235, 220.785141, 239.098500, 220.583355, 218.962732, 238.679352, 255.103936, 251.514713, 245.473869, 258.819837, 219.950686, 248.619983, 243.078577, 265.769812, 234.956316, 288.133073, 225.087852, 239.810430, 268.406605, 283.289840, 233.858455, 258.306589, 240.263617, 246.844456, 290.492875, 267.212598, 243.218596, 265.681905, 244.615890, 242.543363, 203.579010, 216.608405, 212.770208, 207.687875, 220.160683, 191.459568, 210.100242, 202.337063, 224.947876, 199.557862, 225.087852, 217.501685, 197.897572, 229.825316, 242.175607, 201.123644, 219.820165, 202.894307, 211.468055, 246.048907, 225.135194, 210.076305, 226.806762, 212.014431, 205.123267, 217.564498, 237.150426, 228.565362, 222.036114, 235.621356, 195.982460, 223.279198, 221.020998, 243.088105, 214.774631, 239.810430, 197.897572, 244.439113, 241.621129, 260.400953, 216.482178, 236.805076, 216.680343, 223.816297, 263.188711, 236.311810, 222.950152, 244.636356, 219.121372, 219.911078, 243.852681, 258.933231, 261.748652, 250.906955, 267.743972, 226.739575, 254.807401, 249.979759, 273.690377, 240.339217, 268.406605, 229.825316, 241.621129, 302.928261, 288.344398, 238.549018, 267.239982, 248.073140, 254.230916, 296.789984, 267.158551, 252.226496, 271.170860, 248.325354, 253.694013, 266.607649, 281.958112, 273.946966, 263.018181, 285.795029, 240.677519, 269.896959, 263.356244, 291.076027, 255.161923, 283.289840, 242.175607, 260.400953, 288.344398, 343.457361, 257.368309, 284.795470, 263.122266, 271.239770, 320.209823, 283.933299, 264.416752, 292.035194, 268.764031, 265.345807, 213.994496, 228.971242, 227.411615, 216.128966, 229.667231, 196.116652, 222.927882, 213.470569, 241.089661, 209.328714, 233.858455, 201.123644, 216.482178, 238.549018, 257.368309, 239.295031, 234.913508, 218.066855, 219.648997, 257.969951, 231.243624, 224.657569, 238.158714, 217.174368, 215.933866, 241.620759, 252.508513, 252.767900, 244.445283, 260.870105, 217.352348, 247.017507, 246.182278, 267.772651, 232.277540, 258.306589, 219.820165, 236.805076, 267.239982, 284.795470, 234.913508, 289.709239, 241.312315, 247.249491, 286.702147, 264.252852, 245.151647, 264.582984, 240.842689, 245.837476, 223.602907, 234.242638, 232.823977, 227.436840, 240.751687, 203.533069, 230.484479, 225.727773, 248.459822, 216.298768, 240.263617, 202.894307, 216.680343, 248.073140, 263.122266, 218.066855, 241.312315, 255.363057, 230.209787, 271.091482, 239.220241, 225.387834, 247.486715, 226.052431, 224.119935, 220.038678, 240.308477, 233.084574, 231.369510, 247.183922, 204.581690, 233.358639, 229.873732, 249.662698, 220.296241, 246.844456, 211.468055, 223.816297, 254.230916, 271.239770, 219.648997, 247.249491, 230.209787, 264.014907, 271.938970, 246.664305, 227.889045, 249.908085, 232.035369, 229.010298, 264.704959, 285.416390, 278.315614, 270.721492, 289.044453, 243.603643, 274.935489, 266.295057, 295.935799, 253.817633, 290.492875, 246.048907, 263.188711, 296.789984, 320.209823, 257.969951, 286.702147, 271.091482, 271.938970, 352.825726, 286.200221, 267.716897, 297.182554, 269.776351, 266.721561, 240.341716, 254.792243, 250.872786, 242.475130, 260.715749, 217.785986, 249.237737, 242.954024, 267.460908, 237.638235, 267.212598, 225.135194, 236.311810, 267.158551, 283.933299, 231.243624, 264.252852, 239.220241, 246.664305, 286.200221, 294.042749, 246.504021, 269.570596, 243.980697, 242.690997, 223.672378, 240.176223, 235.227909, 226.471530, 244.210258, 204.205559, 235.229584, 225.510760, 255.668926, 220.785141, 243.218596, 210.076305, 222.950152, 252.226496, 264.416752, 224.657569, 245.151647, 225.387834, 227.889045, 267.716897, 246.504021, 259.897656, 251.730847, 229.335712, 229.759185, 244.323303, 259.048267, 255.104263, 248.130112, 267.159502, 223.747953, 253.029955, 249.370268, 275.902272, 239.098500, 265.681905, 226.806762, 244.636356, 271.170860, 292.035194, 238.158714, 264.582984, 247.486715, 249.908085, 297.182554, 269.570596, 251.730847, 303.872223, 251.585636, 247.878402, 223.216217, 235.566855, 238.931093, 225.826557, 242.992822, 203.586842, 228.601700, 227.813265, 248.495606, 220.583355, 244.615890, 212.014431, 219.121372, 248.325354, 268.764031, 217.174368, 240.842689, 226.052431, 232.035369, 269.776351, 243.980697, 229.335712, 251.585636, 257.544914, 228.810942, 226.074990, 236.277617, 235.402356, 228.266719, 244.070245, 200.165867, 230.512885, 232.141964, 246.827505, 218.962732, 242.543363, 205.123267, 219.911078, 253.694013, 265.345807, 215.933866, 245.837476, 224.119935, 229.010298, 266.721561, 242.690997, 229.759185, 247.878402, 228.810942, 253.353769)
    val fl1 = new DoubleMatrix(25, 1, -892.842851, -934.071560, -932.936015, -888.124343, -961.050207, -791.191087, -923.711397, -904.289301, -988.384984, -883.909133, -959.465030, -798.551172, -871.622303, -997.463289, -1043.912620, -863.013719, -976.975712, -897.033693, -898.694786, -1069.245497, -963.491924, -901.263474, -983.768031, -899.865392, -902.283567)

    val qpSolverMlL1 = new QuadraticMinimizer(25, None, None, None).setProximal(SPARSE)
    qpSolverMlL1.setLambda(2.0)

    val qpL1Result = DenseVector(0.18611399665838277, 7.616544482575045E-7, 0.06316840028986725, -0.10416598791666153, 0.11261509172555788, -0.20495026976219588, 0.5266828053262747, 0.32790232894458576, 0.19421456042798912, 0.721805263420604, 0.06308767698159996, -0.41324884565800457, -6.674589161988433E-7, 0.5207826198398771, -5.7526636592184316E-6, 0.18039857485532956, 0.6291589535140261, 0.16327989074951899, -0.06423068666024713, 0.37538997084005027, 0.01655831175814407, -1.1303748315315749E-7, 0.11215692158788952, 0.24778419107179467, 0.04080340730788238)
    val directQpL1Result = DenseVector(0.2110442250290121, 0.012013746756335921, 0.037775937729678466, -0.18192333284133352, 0.14774726026468285, -0.2715795745916918, 0.5503132411617213, 0.3439716863073115, 0.2173877651958189, 0.736535860834321, 0.1025879217959749, -0.4861963873156229, -0.039609745301812625, 0.5596109891609025, -0.0805115340875651, 0.23427896778334065, 0.6566598874646263, 0.17922881648831285, -0.13149274336775535, 0.4086746258178984, 0.03183522804262245, -0.03799368327690542, 0.13098125108364211, 0.29795692565633647, 0.050388790294447164)

    val qpMlL1Result = qpSolverMlL1.solve(Hl1, fl1)

    val octaveL1 = DenseVector(0.18611, 0.00000, 0.06317, -0.10417, 0.11262,
      -0.20495, 0.52668, 0.32790, 0.19421, 0.72180,
      0.06309, -0.41326, -0.00000, 0.52078, -0.00000,
      0.18040, 0.62915, 0.16329, -0.06424, 0.37539,
      0.01659, 0.00000, 0.11215, 0.24778, 0.04082)

    println("Standalone results")
    println(qpMlL1Result)

    println("Spark results")
    println(directQpL1Result)

    println("Octave results")
    println(octaveL1)

    println("QpL1 results")
    println(qpL1Result)

    val (aeq, b, bl, bu, q, h) = QpGenerator.generate(problemSize, nequalities)

    //val directQpTest = new QuadraticMinimizer(h.rows, Some(bl), Some(bu), Some(aeq), true)
    val directQpTest = new QuadraticMinimizer(h.rows, Some(bl), Some(bu))
    directQpTest.setProximal(BOUNDS)
    //directQpTest.solve(h, q, Some(b))
    directQpTest.solve(h, q)
    println(s"Qp Test ${directQpTest.solveTime}ns ${directQpTest.solveTime / 1e6} ms iterations ${directQpTest.iterations}")
  }
}