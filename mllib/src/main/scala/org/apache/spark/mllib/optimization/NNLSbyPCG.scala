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

import org.jblas.{DoubleMatrix, SimpleBlas}

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Object used to solve nonnegative least squares problems using a modified
 * projected gradient method.
 */
@DeveloperApi
private[mllib] object NNLSbyPCG {
  /**
   * Solve a least squares problem, possibly with nonnegativity constraints, by a modified
   * projected gradient method.  That is, find x minimising ||Ax - b||_2 given A^T A and A^T b.
   *
   * We solve the problem
   *   min_x      1/2 x^T ata x^T - x^T atb
   *   subject to x >= 0 (if nonnegative == true)
   *
   * The method used is similar to one described by Polyak (B. T. Polyak, The conjugate gradient
   * method in extremal problems, Zh. Vychisl. Mat. Mat. Fiz. 9(4)(1969), pp. 94-112) for bound-
   * constrained nonlinear programming.  Polyak unconditionally uses a conjugate gradient
   * direction, however, while this method only uses a conjugate gradient direction if the last
   * iteration did not cause a previously-inactive constraint to become active.
   */
  def solve(ata: DoubleMatrix, atb: DoubleMatrix, nonnegative: Boolean): Array[Double] = {
    val n = atb.rows
    val scratch = new DoubleMatrix(n, 1)

    // find the optimal unconstrained step
    def steplen(dir: DoubleMatrix, res: DoubleMatrix): Double = {
      val top = SimpleBlas.dot(dir, res)
      SimpleBlas.gemv(1.0, ata, dir, 0.0, scratch)
      // Push the denominator upward very slightly to avoid infinities and silliness
      top / (SimpleBlas.dot(scratch, dir) + 1e-20)
    }

    // stopping condition
    def stop(step: Double, ndir: Double, nx: Double): Boolean = {
        ((step != step) // NaN
      || (step < 1e-6) // too small or negative
      || (step > 1e40) // too small; almost certainly numerical problems
      || (ndir < 1e-12 * nx) // gradient relatively too small
      || (ndir < 1e-32) // gradient absolutely too small; numerical issues may lurk
      )
    }

    val grad = new DoubleMatrix(n, 1)
    val x = new DoubleMatrix(n, 1)
    val dir = new DoubleMatrix(n, 1)
    val lastDir = new DoubleMatrix(n, 1)
    val res = new DoubleMatrix(n, 1)
    var lastNorm = 0.0
    var iterno = 0
    var lastWall = 0 // Last iteration when we hit a bound constraint.
    var i = 0
    while (iterno < 40000) {
      // find the residual
      SimpleBlas.gemv(1.0, ata, x, 0.0, res)
      SimpleBlas.axpy(-1.0, atb, res)
      SimpleBlas.copy(res, grad)

      // project the gradient
      if (nonnegative) {
        i = 0
        while (i < n) {
          if (grad.data(i) > 0.0 && x.data(i) == 0.0) {
            grad.data(i) = 0.0
          }
          i = i + 1
        }
      }
      val ngrad = SimpleBlas.dot(grad, grad)

      SimpleBlas.copy(grad, dir)

      // use a CG direction under certain conditions
      var step = steplen(grad, res)
      var ndir = 0.0
      val nx = SimpleBlas.dot(x, x)
      if (iterno > lastWall + 1) {
        val alpha = ngrad / lastNorm
        SimpleBlas.axpy(alpha, lastDir, dir)
        val dstep = steplen(dir, res)
        ndir = SimpleBlas.dot(dir, dir)
        if (stop(dstep, ndir, nx)) {
          // reject the CG step if it could lead to premature termination
          SimpleBlas.copy(grad, dir)
          ndir = SimpleBlas.dot(dir, dir)
        } else {
          step = dstep
        }
      } else {
        ndir = SimpleBlas.dot(dir, dir)
      }

      // terminate?
      if (stop(step, ndir, nx)) {
        return x.data
      }

      // don't run through the walls
      if (nonnegative) {
        i = 0
        while (i < n) {
          if (step * dir.data(i) > x.data(i)) {
            step = Math.min(step, x.data(i) / dir.data(i))
          }
          i = i + 1
        }
      }

      // take the step
      i = 0
      while (i < n) {
        if (nonnegative) {
          if (step * dir.data(i) > x.data(i) * (1 - 1e-14)) {
            x.data(i) = 0
            lastWall = iterno
          } else x.data(i) -= step * dir.data(i)
        } else {
          x.data(i) -= step * dir.data(i)
        }
        i = i + 1
      }

      iterno = iterno + 1
      SimpleBlas.copy(dir, lastDir)
      lastNorm = ngrad
    }
    x.data
  }
}
