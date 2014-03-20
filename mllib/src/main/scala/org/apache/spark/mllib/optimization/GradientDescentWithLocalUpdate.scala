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
        20,
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
   * @param numIterations - number of outer iterations that SGD should be run.
   * @param numLocalIterations - number of inner iterations that SGD should be run.
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
      numIterations: Int,
      numLocalIterations: Int,
      regParam: Double,
      miniBatchFraction: Double,
      tinyBatchSize: Int = 20,
      initialWeights: Array[Double]): (Array[Double], Array[Double]) = {

    val stochasticLossHistory = new ArrayBuffer[Double](numIterations)

    // Initialize weights as a column vector
    var weights = new DoubleMatrix(initialWeights.length, 1, initialWeights: _*)
    val regVal = updater
      .compute(weights, new DoubleMatrix(initialWeights.length, 1), 0, 1, regParam)._2


    for (i <- 1 to numIterations) {
      val weightsAndLosses = data.mapPartitions { iter =>
        val rand = new Random(42 + i * numIterations)
        val sampled = iter.filter(x => rand.nextDouble() <= miniBatchFraction)

        val ((weightsAvg, lossAvg, regValAvg), _) = sampled.grouped(tinyBatchSize).map {
          case tinyDataSets =>
            val dataMatrices = tinyDataSets.map {
              case (y, features)  => (y, new DoubleMatrix(features.length, 1, features: _*))
            }

            Iterator.iterate((weights, 0.0, regVal)) {
              case (lastWeights, lastLoss, lastRegVal) =>

                val (localGrad, localLoss) = dataMatrices.map { case (y, featuresCol) =>
                  gradient.compute(featuresCol, y, lastWeights)
                }.reduce((a, b) => (a._1.addi(b._1), a._2+b._2))

                val grad = localGrad.divi(dataMatrices.size)
                val loss = localLoss / dataMatrices.size

                val (newWeights, newRegVal) = updater.compute(
                  lastWeights, grad.div(1.0), stepSize, i*numIterations, regParam)

                (newWeights, loss+lastLoss, newRegVal+lastRegVal)

            }.drop(numLocalIterations).next()

        }.foldLeft(((DoubleMatrix.zeros(initialWeights.length), 0.0, 0.0), 0.0)) {
          case (((lMatrix, lLoss, lRegVal), count), (rMatrix, rLoss, rRegVal)) =>
            ((lMatrix.muli(count).addi(rMatrix).divi(count+1),
              (lLoss*count+rLoss)/(count+1),
              (lRegVal*count+rRegVal)/(count+1)
              ),
              count+1
             )
        }

        val localLossHistory = (lossAvg + regValAvg) / numLocalIterations

        List((weightsAvg, localLossHistory)).iterator
      }

      val c = weightsAndLosses.collect()
      val (ws, ls) = c.unzip

      stochasticLossHistory.append(ls.reduce(_ + _) / ls.size)

      weights = ws.reduce(_ addi _).divi(c.size)
    }

    logInfo("GradientDescentWithLocalUpdate finished. Last a few stochastic losses %s".format(
      stochasticLossHistory.takeRight(10).mkString(", ")))

    (weights.toArray, stochasticLossHistory.toArray)
  }
}
