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

/* 
 * Library of Proximal Algorithms adapted from https://github.com/cvxgrp/proximal
 */

package org.apache.spark.mllib.optimization

import scala.math.max
import scala.math.min
import scala.math.sqrt
import scala.math.abs
import scala.Double.NegativeInfinity
import scala.Double.PositiveInfinity
import org.jblas.DoubleMatrix
import org.jblas.Solve

//TO DO : BLAS the functions
object Proximal {
  // ==================== PROJECTIONS ====================

  // f = I(l <= x <= u)
  def projectBox(x: Array[Double], l: Array[Double], u: Array[Double]) {
    var i = 0
    while (i < x.length) {
      x.update(i, max(l(i), min(x(i), u(i))))
      i = i + 1
    }
  }

  // f = I(x >= 0)
  def projectPos(x: Array[Double]) {
    var i = 0
    while (i < x.length) {
      x.update(i, max(0, x(i)))
      i = i + 1
    }
  }

  def projectSoc(x: Array[Double], n: Int) {
    var nx: Double = 0.0
    var i: Int = 1

    while (i < n) {
      nx += x(i) * x(i)
      i = i + 1
    }
    nx = sqrt(nx);

    if (nx > x(0)) {
      if (nx <= -x(0)) {
        i = 0
        while (i < n) {
          x(i) = 0
          i = i + 1
        }
      } else {
        val alpha = 0.5 * (1 + x(0) / nx)
        x.update(0, alpha * nx)
        i = 1
        while (i < n) {
          x.update(i, alpha * x(i))
          i = i + 1
        }
      }
    }
  }
  
  //Projection onto Affine set
  //Let C = { x \in R^{n} | Ax = b } where A \in R^{m x n}
  //If A is full rank matrix then the projection is given by v - A'(Av - b) where A' is the cached Moore-Penrose pseudo-inverse of A  
  def projectEquality(x: DoubleMatrix, Aeq: DoubleMatrix, invAeq: DoubleMatrix, beq: DoubleMatrix) {
    val Av = Aeq.mul(x)
    Av.subi(beq)
    x.subi(Av.muli(invAeq))
  }
  
  //Projection onto hyper-plane is a special case of projection onto affine set and is given by
  //x + ((b - a'x)/||a||_2^2)a
  def projectHyperPlane(x: DoubleMatrix, a: DoubleMatrix, b: Double) {
    val at = a.transpose()
    val atx = at.dot(x)
    val anorm = a.norm2()
    val scale = (b - atx)/(anorm*anorm)
    val ascaled = a.mul(scale)
    x.addi(ascaled)
  }
  
  def shrinkage(x: Array[Double], scale: Double) {
    for (i <- 0 until x.length) {
      x.update(i, max(0, x(i) - scale) - max(0, -x(i) - scale))
    }
  }
  // ==================== SEPARABLE FUNCTION ====================

  def proxScalar(v: Double, rho: Double, oracle: Double => Double, l: Double, u: Double, x0: Double): Double = {
    var MAX_ITER = 1000
    var tol = 1e-8
    var g: Double = 0.0
    var x = max(l, min(x0, u));

    var lIter = l
    var uIter = u
    var iter = 0

    while (iter < MAX_ITER && u - l > tol) {
      g = -1 / x + rho * (x - v)

      if (g > 0) {
        lIter = max(lIter, x - g / rho)
        uIter = x
      } else if (g < 0) {
        lIter = x
        uIter = min(uIter, x - g / rho)
      }
      x = (lIter + uIter) / 2
      iter = iter + 1
    }
    x
  }

  def proxSeparable(x: Array[Double], rho: Double, oracle: Double => Double, l: Double, u: Double) {
    var i = 0
    while (i < x.length) {
      x.update(i, proxScalar(x(i), rho, oracle, l, u, 0))
      i = i + 1
    }
  }

  // ==================== NORMS ====================

  // f = ||.||_1
  def proxL1(x: Array[Double], rho: Double) {
    var i = 0
    while (i < x.length) {
      x.update(i, max(0, x(i) - 1.0 / rho) - max(0, -x(i) - 1.0 / rho))
      i = i + 1
    }
  }

  // f = ||.||_2
  def proxL2(x: Array[Double], rho: Double) {
    var normSquare: Double = 0.0
    var i = 0

    while (i < x.length) {
      normSquare = normSquare + x(i) * x(i)
      i = i + 1
    }

    val norm = sqrt(normSquare)
    i = 0
    while (i < x.length) {
      if (norm >= 1/ rho) x.update(i, x(i) * (1 - 1/(rho * norm)))
      else x.update(i, 0)
      i = i + 1
    }
  }

  // ==================== OTHER ====================

  // f = (1/2)||.||_2^2
  def proxSumSquare(x: Array[Double], rho: Double) {
    var i = 0
    while (i < x.length) {
      x.update(i, x(i) * (rho / (1 + rho)))
      i = i + 1
    }
  }

  // f = -sum(log(x))
  def proxLogBarrier(x: Array[Double], rho: Double) {
    var i = 0
    while (i < x.length) {
      x.update(i, 0.5 * (x(i) + sqrt(x(i) * x(i) + 4/rho)))
    }
  }

  // f = huber = x^2 if |x|<=1, 2|x| - 1 otherwise
  def subgradHuber(x: Double): Double = {
    if (abs(x) <= 1) {
      2 * x
    } else {
      val projx = if (x > 0) x else -x
      2 * projx
    }
  }

  def proxHuber(x: Array[Double], rho: Double) {
    proxSeparable(x, rho, subgradHuber, NegativeInfinity, PositiveInfinity);
  }

  // f = c'*x
  def proxLinear(x: Array[Double], rho: Double, c: Array[Double]) {
    var i = 0
    while (i < x.length) {
      x.update(i, x(i) - c(i) / rho)
      i = i + 1
    }
  }

  // f = c'*x + I(x >= 0)
  def proxLp(x: Array[Double], rho: Double, c: Array[Double]) {
    var i = 0
    while (i < x.length) {
      x.update(i, max(0, x(i) - c(i) / rho))
      i = i + 1
    }
  }
}