package org.apache.spark.mllib.admm

import breeze.linalg.{norm, axpy}

import scala.util.Random


abstract class ADMMLocalSolver(val points: Array[BV], val labels: Array[Double]) {
  // Initialize the lagrangian multiplier and initial weight vector at [0.0, ..., 0.0]
  var lagrangianMultiplier: BV = points(0) * 0.0
  var w: BV = points(0) * 0.0

  def solveLocal(theta_avg: BV, rho: Double, epsilon: Double = 0.01, initial_w: BV = null): BV
}

class ADMMSGDLocalSolver(points: Array[BV],
                         labels: Array[Double],
                         val gradient: BVGradient,
                         val eta_0: Double,
                         val maxIterations: Int = Integer.MAX_VALUE) extends ADMMLocalSolver(points, labels) {
  def solveLocal(w_avg: BV, rho: Double, epsilon: Double = 0.01, initial_w: BV = null): BV = {
    // Update the lagrangian Multiplier by taking a gradient step
    lagrangianMultiplier += (w - w_avg) * rho

    var t = 0
    var residual = Double.MaxValue

    // var w: BV =  if(initial_w != null) initial_w.copy else w_avg.copy
    while(t < maxIterations && residual > epsilon) {
      val pointIndex = Random.nextInt(points.length)
      val x = points(pointIndex)
      val y = labels(pointIndex)
      // Compute the gradient of the full L
      val point_gradient = gradient(x, y, w) + lagrangianMultiplier + (w - w_avg) * rho
      // Set the learning rate
      val eta_t = eta_0 / (t + 1)
      // w = w + eta_t * point_gradient
      axpy(-eta_t, point_gradient, w)
      // Compute residual
      residual = eta_t * norm(point_gradient, 2.0)
      t += 1
    }
    // Check the local prediction error:
    val propCorrect =
      points.zip(labels).map { case (x,y) => if (x.dot(w) * (y * 2.0 - 1.0) > 0.0) 1 else 0 }
        .reduce(_ + _).toDouble / points.length
    println(s"Local prop correct: $propCorrect")

    println(s"Local iterations: ${t}")

    // Return the final weight vector
    w.copy
  }
}
