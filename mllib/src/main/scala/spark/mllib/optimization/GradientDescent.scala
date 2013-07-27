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

package spark.mllib.optimization

import spark.{Logging, RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer


object GradientDescent {

  /**
   * Run gradient descent in parallel using mini batches.
   * Based on Matlab code written by John Duchi.
   *
   * @param data - Input data for SGD. RDD of form (label, [feature values]).
   * @param gradient - Gradient object that will be used to compute the gradient.
   * @param updater - Updater object that will be used to update the model.
   * @param stepSize - stepSize to be used during update.
   * @param numIters - number of iterations that SGD should be run.
   * @param regParam - regularization parameter
   * @param miniBatchFraction - fraction of the input data set that should be used for
   *                            one iteration of SGD. Default value 1.0.
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the stochastic
   *         loss computed for every iteration.
   */
  def runMiniBatchSGD(
    data: RDD[(Double, Array[Double])],
    gradient: Gradient,
    updater: Updater,
    stepSize: Double,
    numIters: Int,
    regParam: Double,
    initialWeights: Array[Double],
    miniBatchFraction: Double=1.0) : (Array[Double], Array[Double]) = {

    val stochasticLossHistory = new ArrayBuffer[Double](numIters)

    val nexamples: Long = data.count()
    val miniBatchSize = nexamples * miniBatchFraction

    // Initialize weights as a column vector
    var weights = new DoubleMatrix(initialWeights.length, 1, initialWeights:_*)
    var reg_val = 0.0

    for (i <- 1 to numIters) {
      val (gradientSum, lossSum) = data.sample(false, miniBatchFraction, 42+i).map {
        case (y, features) =>
          val featuresRow = new DoubleMatrix(features.length, 1, features:_*)
          val (grad, loss) = gradient.compute(featuresRow, y, weights)
          (grad, loss)
      }.reduce((a, b) => (a._1.addi(b._1), a._2 + b._2))

      stochasticLossHistory.append(lossSum / miniBatchSize + reg_val)
      val update = updater.compute(weights, gradientSum.div(miniBatchSize), stepSize, i, regParam)
      weights = update._1
      reg_val = update._2
      stochasticLossHistory.append(lossSum / miniBatchSize + reg_val)
      /***
      Xinghao: The loss here is sum of lossSum computed using the weights before applying updater,
      and reg_val using weights after applying updater
      ***/
    }

    (weights.toArray, stochasticLossHistory.toArray)
  }
}
