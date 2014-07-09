package org.apache.spark.mllib.admm

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.optimization.Gradient
import org.apache.spark.mllib.linalg.Vector

import breeze.linalg.{axpy => brzAxpy}


object LocalPegasosSGD {
  def solve(input: RDD[LabeledPoint],
            lambda: Double,
            dim: Int,
            iterations: Int,
            initial_t: Int,
            w: Vector,
            grad: Gradient) {
    for (t <- initial_t until initial_t+iterations) {
      val point = input.takeSample(false, 1).apply(0)
      val x = point.features
      val y = point.label

      val (point_gradient, loss) = grad.compute(x, y, w)

      val eta = 1./(lambda * (t+1))
      brzAxpy(eta, point_gradient.toBreeze, w.toBreeze)
    }
    w
  }

}
