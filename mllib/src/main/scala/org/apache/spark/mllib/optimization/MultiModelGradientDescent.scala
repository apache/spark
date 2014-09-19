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

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.annotation.{Experimental, DeveloperApi}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.rdd.RDDFunctions._

class MultiModelGradientDescent private[mllib] (
    private var gradient: MultiModelGradient,
    private var updater: Array[MultiModelUpdater]) extends Optimizer[Matrix] with Logging {

  private var stepSize: Array[Double] = Array(1.0, 0.1)
  private var numIterations: Array[Int] = Array(100)
  private var regParam: Array[Double] = Array(0.0, 0.1, 1.0)
  private var miniBatchFraction: Double = 1.0

  /**
   * Set the initial step size of SGD for the first step. Default (1.0, 0.1).
   * In subsequent steps, the step size will decrease with stepSize/sqrt(t)
   */
  def setStepSize(step: Array[Double]): this.type = {
    this.stepSize = step
    this
  }

  /**
   * :: Experimental ::
   * Set fraction of data to be used for each SGD iteration.
   * Default 1.0 (corresponding to deterministic/classical gradient descent)
   */
  @Experimental
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Array[Int]): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the regularization parameter. Default (0.0, 0.1, 1.0).
   */
  def setRegParam(regParam: Array[Double]): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for SGD.
   */
  def setGradient(gradient: MultiModelGradient): this.type = {
    this.gradient = gradient
    this
  }


  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Array[MultiModelUpdater]): this.type = {
    this.updater = updater
    this
  }

  /**
   * :: DeveloperApi ::
   * Runs gradient descent on the given training data.
   * @param data training data
   * @param initialWeights initial weights
   * @return solution vector
   */
  @DeveloperApi
  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Matrix = {
    val (weights, _) = MultiModelGradientDescent.runMiniBatchMMSGD(
      data,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFraction,
      initialWeights)
    weights
  }

}

/**
 * :: DeveloperApi ::
 * Top-level method to run gradient descent.
 */
@DeveloperApi
object MultiModelGradientDescent extends Logging {
  /**
   * Run stochastic gradient descent (SGD) in parallel using mini batches.
   * In each iteration, we sample a subset (fraction miniBatchFraction) of the total data
   * in order to compute a gradient estimate.
   * Sampling, and averaging the subgradients over this subset is performed using one standard
   * spark map-reduce in each iteration.
   *
   * @param data - Input data for SGD. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example)
   * @param updater - Updater function to actually perform a gradient step in a given direction.
   * @param stepSize - initial step size for the first step
   * @param numIterations - number of iterations that SGD should be run.
   * @param regParam - regularization parameter
   * @param miniBatchFraction - fraction of the input data set that should be used for
   *                            one iteration of SGD. Default value 1.0.
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the
   *         stochastic loss computed for every iteration.
   */
  def runMiniBatchMMSGD(
      data: RDD[(Double, Vector)],
      gradient: MultiModelGradient,
      updater: Array[MultiModelUpdater],
      stepSize: Array[Double],
      numIterations: Array[Int],
      regParam: Array[Double],
      miniBatchFraction: Double,
      initialWeights: Vector,
      batchSize: Int = 64,
      useSparse: Boolean = true,
      buildSparseThreshold: Double = 0.2): (Matrix, Array[Vector]) = {

    val maxNumIter = numIterations.max
    val stochasticLossHistory = new ArrayBuffer[Vector](maxNumIter)

    val numExamples = data.count()
    val miniBatchSize = numExamples * miniBatchFraction
    val numModels = stepSize.length * regParam.length
    val numFeatures = initialWeights.size
    val numRegularizers = updater.length
    val updaterCounter = 0 until numRegularizers
    // Initialize weights as a column vector
    var weights = updaterCounter.map { i =>
      new DenseMatrix(numFeatures, 1, initialWeights.toArray).
        multiply(DenseMatrix.ones(1, numModels))
    }

    var finalWeights: Matrix = new DenseMatrix(numFeatures, 0, Array.empty[Double])

    // if no data, return initial weights to avoid NaNs
    if (numExamples == 0) {

      logInfo("GradientDescent.runMiniBatchSGD returning initial weights, no data found")
      return (Matrices.horzCat(weights), stochasticLossHistory.toArray)

    }
    val stepSizeMatrix = new DenseMatrix(1, numModels,
      stepSize.flatMap{ ss =>
        for (i <- 1 to regParam.length) yield ss
      }
    )
    val regMatrix = SparseMatrix.diag(Vectors.dense(stepSize.flatMap{ ss =>
      for (reg <- regParam) yield reg
    }))

    val bcMetaData =
      if (useSparse) {
        data.context.broadcast(Matrices.getSparsityData(data, batchSize))
      } else {
        val emptyData: Array[(Int, Int)] = (0 until data.partitions.length).map { i =>
          (i, -1)}.toArray
        data.context.broadcast(emptyData)
      }
    val points = Matrices.fromRDD(data, bcMetaData.value, batchSize, buildSparseThreshold)

    /**
     * For the first iteration, the regVal will be initialized as sum of weight squares
     * if it's L2 updater; for L1 updater, the same logic is followed.
     */
    val updaterWithIndex = updater.zipWithIndex

    var regVal = updaterWithIndex.map { case (u, ind) =>
      u.compute(weights(ind), DenseMatrix.zeros(numFeatures, numModels),
        DenseMatrix.zeros(1, numModels), 1, regMatrix)._2
    }
    val orderedIters = numIterations.sorted
    var iterIndexCounter = 0
    for (i <- 1 to maxNumIter) {
      val bcWeights = data.context.broadcast(weights)
      // Sample a subset (fraction miniBatchFraction) of the total data
      // compute and sum up the subgradients on this subset (this is one map-reduce)
      val (gradientSum, lossSum) = points.sample(false, miniBatchFraction, 42 + i)
        .treeAggregate(updaterCounter.map(j => Matrices.zeros(numFeatures, numModels)),
          updaterCounter.map(j => Matrices.zeros(1, numModels)))(
          seqOp = (c, v) => (c, v) match { case ((grad, loss), (label, features)) =>
            val l = updaterCounter.map { j =>
              gradient.compute(features, label, bcWeights.value(j),
                grad(j).asInstanceOf[DenseMatrix])
            }
            (grad, loss.zip(l).map(r => r._1.elementWiseOperateInPlace(_ + _, r._2)))
          },
          combOp = (c1, c2) => (c1, c2) match { case ((grad1, loss1), (grad2, loss2)) =>
            (grad1.zip(grad2).map(r => r._1.elementWiseOperateInPlace(_ + _, r._2)),
              loss1.zip(loss2).map(r => r._1.elementWiseOperateInPlace(_ + _, r._2)))
          })
      stochasticLossHistory.append(Vectors.dense(Matrices.horzCat(updaterCounter.map { i =>
        lossSum(i).elementWiseOperateScalarInPlace(_ / _, miniBatchSize).
        elementWiseOperateOnRowsInPlace(_ + _, regVal(i))
      }).toArray))
      val update = updaterWithIndex.map { case (u, ind) => u.compute(
        weights(ind), gradientSum(ind).elementWiseOperateScalarInPlace(_ / _, miniBatchSize).
          asInstanceOf[DenseMatrix], stepSizeMatrix, i, regMatrix)
      }
      weights = update.map(_._1).toIndexedSeq
      regVal = update.map(_._2)

      if (i == orderedIters(iterIndexCounter)){
        finalWeights = Matrices.horzCat(Seq(finalWeights) ++ weights)
        iterIndexCounter += 1
      }
    }

    logInfo("GradientDescent.runMiniBatchSGD finished. Last 10 stochastic losses %s".format(
      stochasticLossHistory.takeRight(10).mkString(", ")))

    (finalWeights, stochasticLossHistory.toArray)

  }
}

