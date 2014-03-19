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

package org.apache.spark.mllib.optimization

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.jblas.DoubleMatrix

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD



/**
 * Class used to solve an optimization problem using Gradient Descent.
 * @param gradient Gradient function to be used.
 * @param updater Updater to be used to update weights after every iteration.
 */
class GradientDescentWithLocalUpdate(gradient: Gradient, updater: Updater)
  extends GradientDescent(gradient, updater) with Logging {
  private var numLocalIterations: Int = 1

  /**
   * Set the number of local iterations. Default 1.
   */
  def setNumLocalIterations(numLocalIter: Int): this.type = {
    this.numLocalIterations = numLocalIter
    this
  }

  override def optimize(data: RDD[(Double, Array[Double])], initialWeights: Array[Double])
    : Array[Double] = {

    val (weights, stochasticLossHistory) = GradientDescentWithLocalUpdate.runMiniBatchSGD(
        data,
        gradient,
        updater,
        stepSize,
        numIterations,
        numLocalIterations,
        regParam,
        miniBatchFraction,
        initialWeights)
    weights
  }

}

// Top-level method to run gradient descent.
object GradientDescentWithLocalUpdate extends Logging {
   /**
   * Run gradient descent with local update in parallel using mini batches. Unlike the
   * [[GradientDescent]], here gradient descent takes place not only among jobs, but also inner
   * jobs, i.e. on an executor.
   *
   * @param data - Input data for SGD. RDD of form (label, [feature values]).
   * @param gradient - Gradient object that will be used to compute the gradient.
   * @param updater - Updater object that will be used to update the model.
   * @param stepSize - stepSize to be used during update.
   * @param numOuterIterations - number of outer iterations that SGD should be run.
   * @param numInnerIterations - number of inner iterations that SGD should be run.
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
      numOuterIterations: Int,
      numInnerIterations: Int,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Array[Double]): (Array[Double], Array[Double]) = {

    val stochasticLossHistory = new ArrayBuffer[Double](numOuterIterations)

    val numExamples: Long = data.count()
    val numPartition = data.partitions.length
    val miniBatchSize = numExamples * miniBatchFraction / numPartition

    // Initialize weights as a column vector
    var weights = new DoubleMatrix(initialWeights.length, 1, initialWeights: _*)
    var regVal = 0.0

    for (i <- 1 to numOuterIterations) {
      val weightsAndLosses = data.mapPartitions { iter =>
        var iterReserved = iter
        val localLossHistory = new ArrayBuffer[Double](numInnerIterations)

        for (j <- 1 to numInnerIterations) {
          val (iterCurrent, iterNext) = iterReserved.duplicate
          val rand = new Random(42 + i * numOuterIterations + j)
          val sampled = iterCurrent.filter(x => rand.nextDouble() <= miniBatchFraction)
          val (gradientSum, lossSum) = sampled.map { case (y, features) =>
            val featuresCol = new DoubleMatrix(features.length, 1, features: _*)
            gradient.compute(featuresCol, y, weights)
          }.reduceOption((a, b) => (a._1.addi(b._1), a._2 + b._2))
            .getOrElse((DoubleMatrix.zeros(0), 0.0))

          localLossHistory += lossSum / miniBatchSize + regVal

          val update = updater.compute(weights, gradientSum.div(miniBatchSize),
            stepSize, (i - 1) + numOuterIterations + j, regParam)

          weights = update._1
          regVal = update._2

          iterReserved = iterNext
        }

        List((weights, localLossHistory.toArray)).iterator
      }

      val c = weightsAndLosses.collect()
      val (ws, ls) = c.unzip

      stochasticLossHistory.append(ls.head.reduce(_ + _) / ls.head.size)

      val weightsSum = ws.reduce(_ addi _)
      weights = weightsSum.divi(c.size)
    }

    logInfo("GradientDescentWithLocalUpdate finished. Last a few stochastic losses %s".format(
      stochasticLossHistory.takeRight(10).mkString(", ")))

    (weights.toArray, stochasticLossHistory.toArray)
  }
}
