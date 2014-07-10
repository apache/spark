package org.apache.spark.mllib.admm

import breeze.linalg.norm
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD


case class SubProblem(points: Array[BV], labels: Array[Double])

object BSPADMMwithSGD {
  def train(input: RDD[(Double, Vector)],
            numADMMIterations: Int,
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
      println("Building Solver")
      new ADMMSGDLocalSolver(s.points, s.labels, gradient, eta_0 = regularizer, maxIterations = 5)
    }.cache()
    println(s"number of solver ${solvers.count()}")

    val numSolvers = solvers.partitions.length

    var primalResidual = Double.MaxValue
    var dualResidual = Double.MaxValue
    val epsilon = 0.01
    var iter = 0
    var rho  = 0.0

    // Make a zero vector
    var w_avg: BV = input.first()._2.toBreeze * 0.0
    while (iter < numADMMIterations || primalResidual > epsilon || dualResidual > epsilon) {
      println(s"Starting iteration ${iter}.")
      val local_w = solvers.map { s => s.solveLocal(w_avg, rho, epsilon) } . collect()
      val new_w_avg: BV = local_w.reduce(_ + _) / numSolvers.toDouble

      // Update the residuals
      // primalResidual = sum( ||w_i - w_avg||_2^2 )
      primalResidual = Math.pow(local_w.foldLeft(0.0){ (sum , w) => norm(w - new_w_avg, 2.0) }, 2)
      dualResidual = rho * Math.pow(norm(new_w_avg - w_avg, 2.0), 2)

      // Rho upate from Boyd text
      if (rho == 0.0) {
        rho = epsilon
      } else if (primalResidual > 10.0 * dualResidual) {
        rho = 2.0 * rho
        println("Increasing rho")
      } else if (dualResidual > 10.0 * primalResidual) {
        rho = rho / 2.0
        println("Decreasing rho")
      }

      w_avg = new_w_avg

      println(s"Iteration: ${iter}")
      println(s"(Primal Resid, Dual Resid, Rho): ${primalResidual}, \t ${dualResidual}, \t ${rho}")

      iter += 1
    }

    w_avg
  }
}
