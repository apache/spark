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
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Model fitted by [[IterativelyReweightedLeastSquares]].
 * @param coefficients model coefficients
 * @param intercept model intercept
 */
private[ml] class IterativelyReweightedLeastSquaresModel(
    val coefficients: DenseVector,
    val intercept: Double) extends Serializable

/**
 * Fits a generalized linear model (GLM) for a given family using
 * iteratively reweighted least squares (IRLS).
 */
private[ml] class IterativelyReweightedLeastSquares(
    val family: Family,
    val fitIntercept: Boolean,
    val regParam: Double,
    val standardizeFeatures: Boolean,
    val standardizeLabel: Boolean,
    val maxIter: Int,
    val tol: Double) extends Logging with Serializable {

  def fit(instances: RDD[Instance]): IterativelyReweightedLeastSquaresModel = {

    val y = instances.map(_.label).persist(StorageLevel.MEMORY_AND_DISK)
    val yMean = y.mean()
    var mu = y.map { yi => family.startingMu(yi, yMean) }
    var eta: RDD[Double] = null
    var dev = family.deviance(y, mu)

    var converged = false
    val nullDev = dev
    var oldDev = dev
    var deltaDev = 1.0
    var iter = 0

    var zw: RDD[(Double, Double)] = null
    var model: WeightedLeastSquaresModel = null

    while (iter < maxIter && !converged) {

      zw = y.zip(mu).map { case (y, mu) =>
        val eta = family.predict(mu)
        val z = family.adjusted(y, mu, eta)
        val w = family.weights(mu)
        (z, w)
      }
      val wls = new WeightedLeastSquares(fitIntercept, regParam,
        standardizeFeatures, standardizeLabel)
      val newInstances = instances.zip(zw).map { case (instance, (z, w)) =>
        Instance(z, w * instance.weight, instance.features)
      }
      model = wls.fit(newInstances)
      eta = newInstances.map { instance =>
        dot(instance.features, model.coefficients) + model.intercept
      }
      mu = eta.map { eta => family.fitted(eta) }

      oldDev = dev
      dev = family.deviance(y, mu)
      deltaDev = dev - oldDev
      if (math.abs(deltaDev) / (0.1 + math.abs(dev)) < tol) {
        converged = true
      }
      iter = iter + 1
      logInfo(s"Iteration $iter : $deltaDev")
    }

    y.unpersist(blocking = false)

    new IterativelyReweightedLeastSquaresModel(model.coefficients, model.intercept)
  }
}
