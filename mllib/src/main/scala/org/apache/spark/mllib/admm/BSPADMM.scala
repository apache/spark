package org.apache.spark.mllib.admm

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector

import breeze.util.Implicits._


case class SubProblem(points: Array[BV], labels: Array[Double])

object BSPADMMwithSGD {
  def train(input: RDD[(Double, Vector)],
            numADMMIterations: Int,
            rho: Double,
            gradient: BVGradient,
            initialWeights: Vector) = {
    val subProblems: RDD[SubProblem] = input.mapPartitions{ iter =>
      val localData = iter.toArray
      val points = localData.map { case (y, x) => x.toBreeze }
      val labels = localData.map { case (y, x) => y }
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

    // Make a zero vector
    var w_avg: BV = input.first()._2.toBreeze*0

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
