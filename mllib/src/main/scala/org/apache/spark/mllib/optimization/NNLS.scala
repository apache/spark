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

import java.{util => ju}

import com.github.fommil.netlib.BLAS.{getInstance => blas}

/**
 * Object used to solve nonnegative least squares problems using a modified
 * projected gradient method.
 */
private[spark] object NNLS {
  class Workspace(val n: Int) {
    val scratch = new Array[Double](n)
    val grad = new Array[Double](n)
    val x = new Array[Double](n)
    val dir = new Array[Double](n)
    val lastDir = new Array[Double](n)
    val res = new Array[Double](n)

    def wipe(): Unit = {
      ju.Arrays.fill(scratch, 0.0)
      ju.Arrays.fill(grad, 0.0)
      ju.Arrays.fill(x, 0.0)
      ju.Arrays.fill(dir, 0.0)
      ju.Arrays.fill(lastDir, 0.0)
      ju.Arrays.fill(res, 0.0)
    }
  }

  def createWorkspace(n: Int): Workspace = {
    new Workspace(n)
  }

  /**
   * Solve a least squares problem, possibly with nonnegativity constraints, by a modified
   * projected gradient method.  That is, find x minimising ||Ax - b||_2 given A^T A and A^T b.
   *
   * We solve the problem
   *   min_x      1/2 x^T ata x^T - x^T atb
   *   subject to x >= 0
   *
   * The method used is similar to one described by Polyak (B. T. Polyak, The conjugate gradient
   * method in extremal problems, Zh. Vychisl. Mat. Mat. Fiz. 9(4)(1969), pp. 94-112) for bound-
   * constrained nonlinear programming.  Polyak unconditionally uses a conjugate gradient
   * direction, however, while this method only uses a conjugate gradient direction if the last
   * iteration did not cause a previously-inactive constraint to become active.
   */
  def solve(ata: Array[Double], atb: Array[Double], ws: Workspace): Array[Double] = {
    ws.wipe()

    val n = atb.length
    val scratch = ws.scratch

    // find the optimal unconstrained step
    def steplen(dir: Array[Double], res: Array[Double]): Double = {
      val top = blas.ddot(n, dir, 1, res, 1)
      blas.dgemv("N", n, n, 1.0, ata, n, dir, 1, 0.0, scratch, 1)
      // Push the denominator upward very slightly to avoid infinities and silliness
      top / (blas.ddot(n, scratch, 1, dir, 1) + 1e-20)
    }

    // stopping condition
    def stop(step: Double, ndir: Double, nx: Double): Boolean = {
        ((step.isNaN) // NaN
      || (step < 1e-7) // too small or negative
      || (step > 1e40) // too small; almost certainly numerical problems
      || (ndir < 1e-12 * nx) // gradient relatively too small
      || (ndir < 1e-32) // gradient absolutely too small; numerical issues may lurk
      )
    }

    val grad = ws.grad
    val x = ws.x
    val dir = ws.dir
    val lastDir = ws.lastDir
    val res = ws.res
    val iterMax = Math.max(400, 20 * n)
    var lastNorm = 0.0
    var iterno = 0
    var lastWall = 0 // Last iteration when we hit a bound constraint.
    var i = 0
    while (iterno < iterMax) {
      // find the residual
      blas.dgemv("N", n, n, 1.0, ata, n, x, 1, 0.0, res, 1)
      blas.daxpy(n, -1.0, atb, 1, res, 1)
      blas.dcopy(n, res, 1, grad, 1)

      // project the gradient
      i = 0
      while (i < n) {
        if (grad(i) > 0.0 && x(i) == 0.0) {
          grad(i) = 0.0
        }
        i = i + 1
      }
      val ngrad = blas.ddot(n, grad, 1, grad, 1)

      blas.dcopy(n, grad, 1, dir, 1)

      // use a CG direction under certain conditions
      var step = steplen(grad, res)
      var ndir = 0.0
      val nx = blas.ddot(n, x, 1, x, 1)
      if (iterno > lastWall + 1) {
        val alpha = ngrad / lastNorm
        blas.daxpy(n, alpha, lastDir, 1, dir, 1)
        val dstep = steplen(dir, res)
        ndir = blas.ddot(n, dir, 1, dir, 1)
        if (stop(dstep, ndir, nx)) {
          // reject the CG step if it could lead to premature termination
          blas.dcopy(n, grad, 1, dir, 1)
          ndir = blas.ddot(n, dir, 1, dir, 1)
        } else {
          step = dstep
        }
      } else {
        ndir = blas.ddot(n, dir, 1, dir, 1)
      }

      // terminate?
      if (stop(step, ndir, nx)) {
        return x.clone
      }

      // don't run through the walls
      i = 0
      while (i < n) {
        if (step * dir(i) > x(i)) {
          step = x(i) / dir(i)
        }
        i = i + 1
      }

      // take the step
      i = 0
      while (i < n) {
        if (step * dir(i) > x(i) * (1 - 1e-14)) {
          x(i) = 0
          lastWall = iterno
        } else {
          x(i) -= step * dir(i)
        }
        i = i + 1
      }

      iterno = iterno + 1
      blas.dcopy(n, dir, 1, lastDir, 1)
      lastNorm = ngrad
    }
    x.clone
  }
}
