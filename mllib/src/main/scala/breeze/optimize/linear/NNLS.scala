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

// TO DO: This file will move to breeze after initial stress testing inside ml

package breeze.optimize.linear

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.util.Implicits._
import breeze.util.SerializableLogging
import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import spire.syntax.cfor._

/**
 * NNLS solves nonnegative least squares problems using a modified
 * projected gradient method.
 * @param maxIters user defined maximum iterations
 * @author debasish83, coderxiang
 */

class NNLS(val maxIters: Int = -1) extends SerializableLogging {
  type BDM = DenseMatrix[Double]
  type BDV = DenseVector[Double]

  @transient private var _f2jBLAS: NetlibBLAS = _

  // For level-1 routines, we use Java implementation.
  private def f2jBLAS: NetlibBLAS = {
    if (_f2jBLAS == null) {
      _f2jBLAS = new F2jBLAS
    }
    _f2jBLAS
  }

  case class State private[NNLS](x: BDV, grad: BDV, dir: BDV,
                                 lastDir: BDV, res: BDV, tmp: BDV,
                                 lastNorm: Double, lastWall: Int, iter: Int,
                                 converged: Boolean, solveTime: Long) {
    def reset() = {
      x := 0.0
      grad := 0.0
      dir := 0.0
      lastDir := 0.0
      res := 0.0
      tmp := 0.0
      State(x, grad, dir, lastDir, res, tmp, 0.0, 0, 0, false, 0L)
    }
  }

  /**
   * dot(x, y)
   */
  private def dot(x: DenseVector[Double], y: DenseVector[Double]): Double = {
    val n = x.size
    f2jBLAS.ddot(n, x.data, 1, y.data, 1)
  }

  /**
   * y += a * x
   */
  private def axpy(a: Double, x: DenseVector[Double], y: DenseVector[Double]): Unit = {
    val n = x.size
    f2jBLAS.daxpy(n, a, x.data, 1, y.data, 1)
  }

  /**
   * y := alpha * A * x + beta * y
   * For `DenseMatrix` A.
   */
  private def gemv(alpha: Double,
                   A: DenseMatrix[Double],
                   x: DenseVector[Double],
                   beta: Double,
                   y: DenseVector[Double]): Unit =  {
    val tStrA = if (A.isTranspose) "T" else "N"
    val mA = if (!A.isTranspose) A.rows else A.cols
    val nA = if (!A.isTranspose) A.rows else A.cols
    blas.dgemv(tStrA, mA, nA, alpha, A.data, mA, x.data, 1, beta, y.data, 1)
  }

  // find the optimal unconstrained step
  private def steplen(ata: BDM, dir: BDV, res: BDV,
              tmp: BDV): Double = {
    val top = dot(dir, res)
    gemv(1.0, ata, dir, 0.0, tmp)
    // Push the denominator upward very slightly to avoid infinities and silliness
    top / (dot(tmp, dir) + 1e-20)
  }

  // stopping condition
  private def stop(step: Double, ndir: Double, nx: Double): Boolean = {
    ((step.isNaN) // NaN
      || (step < 1e-7) // too small or negative
      || (step > 1e40) // too small; almost certainly numerical problems
      || (ndir < 1e-12 * nx) // gradient relatively too small
      || (ndir < 1e-32) // gradient absolutely too small; numerical issues may lurk
      )
  }

  /**
   * Solve a least squares problem, possibly with nonnegativity constraints, by a modified
   * projected gradient method.  That is, find x minimising ||Ax - b||_2 given A^T A and A^T b.
   *
   * We solve the problem
   * min_x      1/2 x' ata x' - x'atb
   * subject to x >= 0
   *
   * The method used is similar to one described by Polyak (B. T. Polyak, The conjugate gradient
   * method in extremal problems, Zh. Vychisl. Mat. Mat. Fiz. 9(4)(1969), pp. 94-112) for bound-
   * constrained nonlinear programming.  Polyak unconditionally uses a conjugate gradient
   * direction, however, while this method only uses a conjugate gradient direction if the last
   * iteration did not cause a previously-inactive constraint to become active.
   */
  def initialState(n: Int): State = {
    val grad = DenseVector.zeros[Double](n)
    val x = DenseVector.zeros[Double](n)
    val dir = DenseVector.zeros[Double](n)
    val lastDir = DenseVector.zeros[Double](n)
    val res = DenseVector.zeros[Double](n)
    val tmp = DenseVector.zeros[Double](n)
    val lastNorm = 0.0
    val lastWall = 0
    State(x, grad, dir, lastDir, res, tmp, lastNorm, lastWall, 0, false, 0L)
  }

  def iterations(ata: DenseMatrix[Double],
                 atb: DenseVector[Double]): Iterator[State] = {
    val init = initialState(atb.length)
    iterations(ata, atb, init)
  }

  def validate(ata: DenseMatrix[Double],
                atb: DenseVector[Double],
                state: State) = {
    require(ata.cols == ata.rows, s"NNLS:iterations gram matrix must be symmetric")
    require(ata.rows == state.x.length, s"NNLS:iterations gram and linear dimension mismatch")
    state
  }

  def iterations(ata: DenseMatrix[Double],
                 atb: DenseVector[Double], initialState: State): Iterator[State] =
    Iterator.iterate(validate(ata, atb, initialState)) { state =>
      import state._

      val startTime = System.nanoTime()

      val n = atb.length

      val iterMax = if (maxIters < 0) Math.max(400, 20 * n) else maxIters

      // find the residual
      gemv(1.0, ata, x, 0.0, res)
      axpy(-1.0, atb, res)
      grad := res

      // project the gradient
      cforRange(0 until n) { i =>
        if (grad(i) > 0.0 && x(i) == 0.0) {
          grad(i) = 0.0
        }
      }

      val ngrad = dot(grad, grad)
      dir := grad

      // use a CG direction under certain conditions
      var step = steplen(ata, grad, res, tmp)
      var ndir = 0.0
      val nx = dot(x, x)

      if (iter > lastWall + 1) {
        val alpha = ngrad / lastNorm
        axpy(alpha, lastDir, dir)
        val dstep = steplen(ata, dir, res, tmp)
        ndir = dot(dir, dir)
        if (stop(dstep, ndir, nx)) {
          // reject the CG step if it could lead to premature termination
          dir := grad
          ndir = dot(dir, dir)
        } else {
          step = dstep
        }
      } else {
        ndir = dot(dir, dir)
      }

      // terminate?
      if (stop(step, ndir, nx) || iter > iterMax) {
        State(x, grad, dir, lastDir, res, tmp, lastNorm, lastWall, iter + 1, true,
          solveTime + (System.nanoTime() - startTime))
      }
      else {
        // don't run through the walls
        cforRange(0 until n){ i =>
          if (step * dir(i) > x(i)) {
            step = x(i) / dir(i)
          }
        }
        var nextWall = lastWall
        // take the step
        cforRange(0 until n) { i =>
          if (step * dir(i) > x(i) * (1 - 1e-14)) {
            x(i) = 0
            nextWall = iter
          } else {
            x(i) -= step * dir(i)
          }
        }
        lastDir := dir
        val nextNorm = ngrad
        State(x, grad, dir, lastDir, res, tmp, nextNorm, nextWall, iter + 1, false,
          solveTime + (System.nanoTime() - startTime))
      }
    }.takeUpToWhere(_.converged)

  def minimizeAndReturnState(ata: DenseMatrix[Double],
                             atb: DenseVector[Double]): State = {
    iterations(ata, atb).last
  }

  def minimize(ata: DenseMatrix[Double], atb: DenseVector[Double]): DenseVector[Double] = {
    minimizeAndReturnState(ata, atb).x
  }

  def minimizeAndReturnState(ata: DenseMatrix[Double],
                             atb: DenseVector[Double], init: State): State = {
    iterations(ata, atb, init).last
  }

  def minimize(ata: DenseMatrix[Double],
               atb: DenseVector[Double],
               init: State): DenseVector[Double] = {
    minimizeAndReturnState(ata, atb, init).x
  }
}


