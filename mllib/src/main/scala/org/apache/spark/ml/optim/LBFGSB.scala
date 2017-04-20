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

package org.apache.spark.ml.optim

import breeze.linalg.{DenseVector => BDV, Vector}
import breeze.optimize.{DiffFunction, FirstOrderException, LBFGSB => BreezeLBFGSB, StrongWolfeLineSearch}
import scala.math._

class BoundStrongWolfeLineSearch(maxZoomIter: Int, maxLineSearchIter: Int)
    extends StrongWolfeLineSearch(maxZoomIter, maxLineSearchIter) {

  def minimizeWithBound(f: DiffFunction[Double], init: Double, bound: Double): Double = {

    require(init > 0 && init <= bound, "bound should >= init stepsize")

    def phi(t: Double): Bracket = {
      val (pval, pdd) = f.calculate(t)
      Bracket(t = t, dd = pdd, fval = pval)
    }

    var t = init // Search's current multiple of pk
    var low = phi(0.0)
    val fval = low.fval
    val dd = low.dd

    if (dd > 0) {
      throw new FirstOrderException("Line search invoked with non-descent direction: " + dd)
    }

    /**
     * Assuming a point satisfying the strong wolfe conditions exists within
     * the passed interval, this method finds it by iteratively refining the
     * interval. Nocedal & Wright give the following invariants for zoom's loop:
     *
     *  - The interval bounded by low.t and hi.t contains a point satisfying the
     *    strong Wolfe conditions.
     *  - Among all points evaluated so far that satisfy the "sufficient decrease"
     *    condition, low.t is the one with the smallest fval.
     *  - hi.t is chosen so that low.dd * (hi.t - low.t) < 0.
     */
    def zoom(linit: Bracket, rinit: Bracket): Double = {

      var low = linit
      var hi = rinit

      for (i <- 0 until maxZoomIter) {
        // Interp assumes left less than right in t value, so flip if needed
        val t = if (low.t > hi.t) interp(hi, low) else interp(low, hi)

        // Evaluate objective at t, and build bracket
        val c = phi(t)
        //logger.debug("ZOOM:\n c: " + c + " \n l: " + low + " \nr: " + hi)
        logger.info("Line search t: " + t + " fval: " + c.fval +
          " rhs: " + (fval + c1 * c.t * dd) + " cdd: " + c.dd)

        ///////////////
        /// Update left or right bracket, or both

        if (c.fval > fval + c1 * c.t * dd || c.fval >= low.fval) {
          // "Sufficient decrease" condition not satisfied by c. Shrink interval at right
          hi = c
          logger.debug("hi=c")
        } else {

          // Zoom exit condition is the "curvature" condition
          // Essentially that the directional derivative is large enough
          if (abs(c.dd) <= c2 * abs(dd)) {
            return c.t
          }

          // If the signs don't coincide, flip left to right before updating l to c
          if (c.dd * (hi.t - low.t) >= 0) {
            logger.debug("flipping")
            hi = low
          }

          logger.debug("low=c")
          // If curvature condition not satisfied, move the left hand side of the
          // interval further away from t=0.
          low = c
        }
      }

      throw new FirstOrderException(s"Line search zoom failed")
    }

    ///////////////////////////////////////////////////////////////////

    for (i <- 0 until maxLineSearchIter) {
      val c = phi(t)

      // If phi has a bounded domain, inf or nan usually indicates we took
      // too large a step.
      if (java.lang.Double.isInfinite(c.fval) || java.lang.Double.isNaN(c.fval)) {
        t /= 2.0
        logger.error("Encountered bad values in function evaluation. Decreasing step size to " + t)
      } else {

        // Zoom if "sufficient decrease" condition is not satisfied
        if ((c.fval > fval + c1 * t * dd) ||
          (c.fval >= low.fval && i > 0)) {
          logger.debug("Line search t: " + t + " fval: " + c.fval + " cdd: " + c.dd)
          return zoom(low, c)
        }

        // We don't need to zoom at all
        // if the strong wolfe condition is satisfied already.
        if (abs(c.dd) <= c2 * abs(dd)) {
          return c.t
        }

        // If c.dd is positive, we zoom on the inverted interval.
        // Occurs if we skipped over the nearest local minimum
        // over to the next one.
        if (c.dd >= 0) {
          logger.debug("Line search t: " + t + " fval: " + c.fval +
            " rhs: " + (fval + c1 * t * dd) + " cdd: " + c.dd)
          return zoom(c, low)
        }

        low = c

        if (t == bound) {
          logger.debug("Reach bound, satisfy sufficent decrease condition," +
            " but not curvature condition satisfied.")
          return bound
        } else {
          t *= 1.5
          if (t > bound) {
            t = bound
          }
          logger.debug("Sufficent Decrease condition but not curvature condition satisfied." +
            " Increased t to: " + t)
        }
      }
    }

    throw new FirstOrderException("Line search failed")
  }
}

class LBFGSB(
    lowerBounds: BDV[Double],
    upperBounds: BDV[Double],
    maxIter: Int = 100,
    m: Int = 5,
    tolerance: Double = 1E-8,
    maxZoomIter: Int = 64,
    maxLineSearchIter: Int = 64)
  extends BreezeLBFGSB(
    lowerBounds, upperBounds, maxIter, m, tolerance, maxZoomIter, maxLineSearchIter) {

  override protected def determineStepSize(
      state: State,
      f: DiffFunction[BDV[Double]],
      direction: BDV[Double]): Double = {

    val ff = new DiffFunction[Double] {
      def calculate(alpha: Double) = {
        val newX = takeStep(state, direction, alpha)
        val (ff, grad) = f.calculate(newX)
        ff -> (grad dot direction)
      }
    }

    val wolfeRuleSearch = new BoundStrongWolfeLineSearch(maxZoomIter, maxLineSearchIter)

    var minStepBound = Double.PositiveInfinity
    var i = 0
    while (i < lowerBounds.length) {
      val dir = direction(i)
      if (dir != 0.0) {
        val bound = if (dir < 0.0) lowerBounds(i) else upperBounds(i)
        val stepBound = (bound - state.x(i)) / dir

        assert(stepBound > 0.0)

        if (stepBound < minStepBound) {
          minStepBound = stepBound
        }
      }
      i += 1
    }

    wolfeRuleSearch.minimizeWithBound(ff, 1.0, minStepBound)
  }

  override protected def takeStep(state: State, dir: BDV[Double], stepSize: Double) = {
    val newX = state.x + (dir :* stepSize)
    adjustWithinBound(newX)
    newX
  }

  def adjustWithinBound(point: BDV[Double]): Unit = {
    var i = 0
    while (i < point.length) {
      if (point(i) > upperBounds(i)) {
        point(i) = upperBounds(i)
      }
      if (point(i) < lowerBounds(i)) {
        point(i) = lowerBounds(i)
      }
      i += 1
    }
  }

  /**
   * @param xCauchy generalize cauchy point
   * @param du gradient directiong
   * @param freeVarIndex
   * @return starAlpha = max{a : a <= 1 and  l_i-xc_i <= a*d_i <= u_i-xc_i}
   */
  override protected def findAlpha(
      xCauchy: BDV[Double],
      du: Vector[Double],
      freeVarIndex: Array[Int]) = {
    var starAlpha = 1.0
    for ((vIdx, i) <- freeVarIndex.zipWithIndex) {
      if (0 < du(i)) {
        starAlpha = math.min(starAlpha, (upperBounds(vIdx) - xCauchy(vIdx)) / du(i))
      } else if (0 > du(i)) {
        starAlpha = math.min(starAlpha, (lowerBounds(vIdx) - xCauchy(vIdx)) / du(i))
      }
    }
    assert(starAlpha >= 0.0 && starAlpha <= 1.0)
    starAlpha
  }

  override protected def chooseDescentDirection(
      state: State,
      f: DiffFunction[BDV[Double]]): BDV[Double] = {
    val x = state.x
    val g = state.grad

    // step2:compute the cauchy point by algorithm CP
    val (cauchyPoint, c) = getGeneralizedCauchyPoint(state.history, x, g)

    adjustWithinBound(cauchyPoint)

    val dirk = if (0 == state.iter) {
      cauchyPoint - x
    } else {
      // step3:compute a search direction d_k by the primal method
      val subspaceMin = subspaceMinimization(state.history, cauchyPoint, x, c, g)

      adjustWithinBound(subspaceMin)

      subspaceMin - x
    }

    dirk
  }
}
