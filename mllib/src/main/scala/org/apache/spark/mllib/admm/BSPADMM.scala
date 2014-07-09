package org.apache.spark.mllib.admm

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

case class SubProblem(val points: Array[BV], val labels: Array[Double])

class BSPADMMwithSGD {
  def train(input: RDD[LabeledPoint],
            numADMMIterations: Int,
            rho: Double,
            gradient: BVGradient) = {
    val subProblems: RDD[SubProblem] = input.mapPartitions{ iter =>
      val localData = iter.toArray
      val points = localData.map { pt => pt.features.toBreeze }
      val labels = localData.map { pt => pt.label }
      Iterator(SubProblem(points, labels))
    }

    val solvers = subProblems.map { s =>
      val regularizer = 0.1
      val gradient = new PegasosBVGradient(regularizer)
      new ADMMSGDLocalSolver(s.points, s.labels, gradient, eta_0 = regularizer, maxIterations = 5)
    }.cache()

    val numSolvers = solvers.partitions.length

    var primalResidual = Double.MaxValue
    var dualResidual = Double.MaxValue
    val epsilon = 0.01
    var iter = 0

    var w_avg: BV = input.take(1)(0).features.toBreeze*0

    while (iter < numADMMIterations && primalResidual < epsilon && dualResidual < epsilon) {
      val local_w = solvers.map { s => s.solveLocal(w_avg, rho, epsilon) } . collect()
      val new_w_avg: BV = local_w.sum / numSolvers

      primalResidual = Math.pow(local_w.foldLeft(0.0){ (sum , w) => (w - new_w_avg).norm() }, 2)
      dualResidual = rho * Math.pow((new_w_avg - w_avg).norm(), 2)

      w_avg = new_w_avg
      iter += 1
    }

    w_avg
  }
}
