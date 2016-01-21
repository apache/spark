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

import org.apache.spark.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

/**
 * Model fitted by [[IterativelyReweightedLeastSquares]].
 * @param coefficients model coefficients
 * @param intercept model intercept
 */
private[ml] class IterativelyReweightedLeastSquaresModel(
    val coefficients: DenseVector,
    val intercept: Double) extends Serializable

/**
 * Implements the method of iteratively reweighted least squares (IRLS) which is used to solve
 * certain optimization problems by an iterative method. In each step of the iterations, it
 * involves solving a weighted lease squares (WLS) problem by [[WeightedLeastSquares]].
 * It can be used to find maximum likelihood estimates of a generalized linear model (GLM),
 * find M-estimator in robust regression and some other optimization problems.
 *
 * @param initialModel the initial guess model.
 * @param reweightFunc the reweight function which is used to update offsets and weights
 *                     at each iteration.
 * @param fitIntercept whether to fit intercept.
 * @param regParam L2 regularization parameter used by WLS.
 * @param maxIter maximum number of iterations.
 * @param tol the convergence tolerance.
 */
private[ml] class IterativelyReweightedLeastSquares(
    val initialModel: WeightedLeastSquaresModel,
    val reweightFunc: (Instance, WeightedLeastSquaresModel) => (Double, Double),
    val fitIntercept: Boolean,
    val regParam: Double,
    val maxIter: Int,
    val tol: Double) extends Logging with Serializable {

  def fit(instances: RDD[Instance]): IterativelyReweightedLeastSquaresModel = {

    var converged = false
    var iter = 0

    var offsetsAndWeights: RDD[(Double, Double)] = null
    var model: WeightedLeastSquaresModel = initialModel
    var oldModel: WeightedLeastSquaresModel = initialModel

    while (iter < maxIter && !converged) {

      oldModel = model

      // Update offsets and weights using reweightFunc
      offsetsAndWeights = instances.map { instance => reweightFunc(instance, oldModel) }

      // Estimate new model
      val wls = new WeightedLeastSquares(fitIntercept, regParam, false, false)
      val newInstances = instances.zip(offsetsAndWeights).map {
        case (instance, (offset, weight)) => Instance(offset, weight, instance.features)
      }
      model = wls.fit(newInstances)

      val oldParameters = Array.concat(Array(oldModel.intercept), oldModel.coefficients.toArray)
      val parameters = Array.concat(Array(model.intercept), model.coefficients.toArray)
      val deltaArray = oldParameters.zip(parameters).map { case (x: Double, y: Double) =>
        math.abs(x - y)
      }
      if (!deltaArray.exists(_ > tol)) {
        converged = true
        logInfo(s"IRLS converged in $iter iterations.")
      }

      logInfo(s"Iteration $iter : relative tolerance = ${deltaArray.max}")
      iter = iter + 1

      if (iter == maxIter) {
        logInfo(s"IRLS reached the max number of iterations: $maxIter.")
      }

    }

    new IterativelyReweightedLeastSquaresModel(model.coefficients, model.intercept)
  }
}
