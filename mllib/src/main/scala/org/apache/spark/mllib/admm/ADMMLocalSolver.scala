package org.apache.spark.mllib.admm

import scala.util.Random
import breeze.linalg.axpy

abstract class ADMMLocalSolver(val points: Array[BV], val labels: Array[Double]) {
  var lagrangian: BV = points(0)*0
  var w_prev: BV = lagrangian.copy

  def solveLocal(theta_avg: BV, rho: Double, epsilon: Double = 0.01, initial_w: BV = null): BV
}

class ADMMSGDLocalSolver(points: Array[BV],
                         labels: Array[Double],
                         val gradient: BVGradient,
                         val eta_0: Double,
                         val maxIterations: Int = Integer.MAX_VALUE) extends ADMMLocalSolver(points, labels) {
  def solveLocal(w_avg: BV, rho: Double, epsilon: Double = 0.01, initial_w: BV = null): BV = {
    lagrangian += (w_avg - w_prev) * rho

    var i = 0
    var residual = Double.MaxValue

    var w = if(initial_w != null) {
      initial_w.copy
    } else {
      w_avg.copy
    }

    // TODO: fix residual
    while(i < maxIterations && residual > epsilon) {
      val pointIndex = Random.nextInt(points.length)
      val x = points(pointIndex)
      val y = labels(pointIndex)

      val point_gradient = gradient(x, y, w)

      point_gradient += lagrangian + (w - w_avg)*rho

      val eta_t = eta_0/(i+1)
      axpy(eta_t, point_gradient, w)
    }

    w_prev = w
    w
  }
}
