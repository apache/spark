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

import org.apache.spark.ml.feature.{Instance, OffsetInstance}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.OptionalInstrumentation
import org.apache.spark.rdd.RDD

/**
 * Model fitted by [[IterativelyReweightedLeastSquares]].
 * @param coefficients model coefficients
 * @param intercept model intercept
 * @param diagInvAtWA diagonal of matrix (A^T * W * A)^-1 in the last iteration
 * @param numIterations number of iterations
 */
private[ml] class IterativelyReweightedLeastSquaresModel(
    val coefficients: DenseVector,
    val intercept: Double,
    val diagInvAtWA: DenseVector,
    val numIterations: Int) extends Serializable

/**
 * Implements the method of iteratively reweighted least squares (IRLS) which is used to solve
 * certain optimization problems by an iterative method. In each step of the iterations, it
 * involves solving a weighted least squares (WLS) problem by [[WeightedLeastSquares]].
 * It can be used to find maximum likelihood estimates of a generalized linear model (GLM),
 * find M-estimator in robust regression and other optimization problems.
 *
 * @param initialModel the initial guess model.
 * @param reweightFunc the reweight function which is used to update working labels and weights
 *                     at each iteration.
 * @param fitIntercept whether to fit intercept.
 * @param regParam L2 regularization parameter used by WLS.
 * @param maxIter maximum number of iterations.
 * @param tol the convergence tolerance.
 *
 * @see <a href="http://www.jstor.org/stable/2345503">P. J. Green, Iteratively
 * Reweighted Least Squares for Maximum Likelihood Estimation, and some Robust
 * and Resistant Alternatives, Journal of the Royal Statistical Society.
 * Series B, 1984.</a>
 */
private[ml] class IterativelyReweightedLeastSquares(
    val initialModel: WeightedLeastSquaresModel,
    val reweightFunc: (OffsetInstance, WeightedLeastSquaresModel) => (Double, Double),
    val fitIntercept: Boolean,
    val regParam: Double,
    val maxIter: Int,
    val tol: Double) extends Serializable {

  def fit(
      instances: RDD[OffsetInstance],
      instr: OptionalInstrumentation = OptionalInstrumentation.create(
        classOf[IterativelyReweightedLeastSquares])): IterativelyReweightedLeastSquaresModel = {

    var converged = false
    var iter = 0

    var model: WeightedLeastSquaresModel = initialModel
    var oldModel: WeightedLeastSquaresModel = null

    while (iter < maxIter && !converged) {

      oldModel = model

      // Update working labels and weights using reweightFunc
      val newInstances = instances.map { instance =>
        val (newLabel, newWeight) = reweightFunc(instance, oldModel)
        Instance(newLabel, newWeight, instance.features)
      }

      // Estimate new model
      model = new WeightedLeastSquares(fitIntercept, regParam, elasticNetParam = 0.0,
        standardizeFeatures = false, standardizeLabel = false)
        .fit(newInstances, instr = instr)

      // Check convergence
      val oldCoefficients = oldModel.coefficients
      val coefficients = model.coefficients
      BLAS.axpy(-1.0, coefficients, oldCoefficients)
      val maxTolOfCoefficients = oldCoefficients.toArray.foldLeft(0.0) { (x, y) =>
        math.max(math.abs(x), math.abs(y))
      }
      val maxTol = math.max(maxTolOfCoefficients, math.abs(oldModel.intercept - model.intercept))

      if (maxTol < tol) {
        converged = true
        instr.logInfo(s"IRLS converged in $iter iterations.")
      }

      instr.logInfo(s"Iteration $iter : relative tolerance = $maxTol")
      iter = iter + 1

      if (iter == maxIter) {
        instr.logInfo(s"IRLS reached the max number of iterations: $maxIter.")
      }

    }

    new IterativelyReweightedLeastSquaresModel(
      model.coefficients, model.intercept, model.diagInvAtWA, iter)
  }
}
