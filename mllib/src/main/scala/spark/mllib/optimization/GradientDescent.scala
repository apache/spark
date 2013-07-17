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
   * @param miniBatchFraction - fraction of the input data set that should be used for
   *                            one iteration of SGD. Default value 1.0.
   *
   * @return weights - Column matrix containing weights for every feature.
   * @return lossHistory - Array containing the loss computed for every iteration.
   */
  def runMiniBatchSGD(
    data: RDD[(Double, Array[Double])],
    gradient: Gradient,
    updater: Updater,
    stepSize: Double,
    numIters: Int,
    initialWeights: Array[Double],
    miniBatchFraction: Double=1.0) : (DoubleMatrix, Array[Double]) = {

    val lossHistory = new ArrayBuffer[Double](numIters)

    val nexamples: Long = data.count()
    val miniBatchSize = nexamples * miniBatchFraction

    // Initialize weights as a column matrix
    var weights = new DoubleMatrix(1, initialWeights.length,
      initialWeights:_*)
    var reg_val = 0.0

    for (i <- 1 to numIters) {
      val (gradientSum, lossSum) = data.sample(false, miniBatchFraction, 42+i).map {
        case (y, features) =>
          val featuresRow = new DoubleMatrix(features.length, 1, features:_*)
          val (grad, loss) = gradient.compute(featuresRow, y, weights)
          (grad, loss)
      }.reduce((a, b) => (a._1.addi(b._1), a._2 + b._2))

      lossHistory.append(lossSum / miniBatchSize + reg_val)
      val update = updater.compute(weights, gradientSum.div(miniBatchSize), stepSize, i)
      weights = update._1
      reg_val = update._2
    }

    (weights, lossHistory.toArray)
  }
}
