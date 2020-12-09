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
package org.apache.spark.ml.optim.aggregator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{Instance, InstanceBlock}
import org.apache.spark.ml.linalg._

/**
 * HuberAggregator computes the gradient and loss for a huber loss function,
 * as used in robust regression for samples in sparse or dense vector in an online fashion.
 *
 * The huber loss function based on:
 * <a href="http://statweb.stanford.edu/~owen/reports/hhu.pdf">Art B. Owen (2006),
 * A robust hybrid of lasso and ridge regression</a>.
 *
 * Two HuberAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * The huber loss function is given by
 *
 * <blockquote>
 *   $$
 *   \begin{align}
 *   \min_{w, \sigma}\frac{1}{2n}{\sum_{i=1}^n\left(\sigma +
 *   H_m\left(\frac{X_{i}w - y_{i}}{\sigma}\right)\sigma\right) + \frac{1}{2}\lambda {||w||_2}^2}
 *   \end{align}
 *   $$
 * </blockquote>
 *
 * where
 *
 * <blockquote>
 *   $$
 *   \begin{align}
 *   H_m(z) = \begin{cases}
 *            z^2, & \text {if } |z| &lt; \epsilon, \\
 *            2\epsilon|z| - \epsilon^2, & \text{otherwise}
 *            \end{cases}
 *   \end{align}
 *   $$
 * </blockquote>
 *
 * It is advised to set the parameter $\epsilon$ to 1.35 to achieve 95% statistical efficiency
 * for normally distributed data. Please refer to chapter 2 of
 * <a href="http://statweb.stanford.edu/~owen/reports/hhu.pdf">
 * A robust hybrid of lasso and ridge regression</a> for more detail.
 *
 * @param fitIntercept Whether to fit an intercept term.
 * @param epsilon The shape parameter to control the amount of robustness.
 * @param bcFeaturesStd The broadcast standard deviation values of the features.
 * @param bcParameters including three parts: the regression coefficients corresponding
 *                     to the features, the intercept (if fitIntercept is ture)
 *                     and the scale parameter (sigma).
 */
private[ml] class HuberAggregator(
    fitIntercept: Boolean,
    epsilon: Double,
    bcFeaturesStd: Broadcast[Array[Double]])(bcParameters: Broadcast[Vector])
  extends DifferentiableLossAggregator[Instance, HuberAggregator] {

  protected override val dim: Int = bcParameters.value.size
  private val numFeatures = if (fitIntercept) dim - 2 else dim - 1
  private val sigma = bcParameters.value(dim - 1)
  private val intercept = if (fitIntercept) bcParameters.value(dim - 2) else 0.0

  // make transient so we do not serialize between aggregation stages
  @transient private lazy val coefficients = bcParameters.value.toArray.take(numFeatures)

  /**
   * Add a new training instance to this HuberAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This HuberAggregator object.
   */
  def add(instance: Instance): HuberAggregator = {
    instance match { case Instance(label, weight, features) =>
      require(numFeatures == features.size, s"Dimensions mismatch when adding new sample." +
        s" Expecting $numFeatures but got ${features.size}.")
      require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

      if (weight == 0.0) return this
      val localFeaturesStd = bcFeaturesStd.value
      val localCoefficients = coefficients
      val localGradientSumArray = gradientSumArray

      val margin = {
        var sum = 0.0
        features.foreachNonZero { (index, value) =>
          if (localFeaturesStd(index) != 0.0) {
            sum += localCoefficients(index) * (value / localFeaturesStd(index))
          }
        }
        if (fitIntercept) sum += intercept
        sum
      }
      val linearLoss = label - margin

      if (math.abs(linearLoss) <= sigma * epsilon) {
        lossSum += 0.5 * weight * (sigma + math.pow(linearLoss, 2.0) / sigma)
        val linearLossDivSigma = linearLoss / sigma

        features.foreachNonZero { (index, value) =>
          if (localFeaturesStd(index) != 0.0) {
            localGradientSumArray(index) +=
              -1.0 * weight * linearLossDivSigma * (value / localFeaturesStd(index))
          }
        }
        if (fitIntercept) {
          localGradientSumArray(dim - 2) += -1.0 * weight * linearLossDivSigma
        }
        localGradientSumArray(dim - 1) += 0.5 * weight * (1.0 - math.pow(linearLossDivSigma, 2.0))
      } else {
        val sign = if (linearLoss >= 0) -1.0 else 1.0
        lossSum += 0.5 * weight *
          (sigma + 2.0 * epsilon * math.abs(linearLoss) - sigma * epsilon * epsilon)

        features.foreachNonZero { (index, value) =>
          if (localFeaturesStd(index) != 0.0) {
            localGradientSumArray(index) +=
              weight * sign * epsilon * (value / localFeaturesStd(index))
          }
        }
        if (fitIntercept) {
          localGradientSumArray(dim - 2) += weight * sign * epsilon
        }
        localGradientSumArray(dim - 1) += 0.5 * weight * (1.0 - epsilon * epsilon)
      }

      weightSum += weight
      this
    }
  }
}


/**
 * BlockHuberAggregator computes the gradient and loss for Huber loss function
 * as used in linear regression for blocks in sparse or dense matrix in an online fashion.
 *
 * Two BlockHuberAggregators can be merged together to have a summary of loss and gradient
 * of the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to be standardized before computation.
 *
 * @param fitIntercept Whether to fit an intercept term.
 */
private[ml] class BlockHuberAggregator(
    fitIntercept: Boolean,
    epsilon: Double)(bcParameters: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock, BlockHuberAggregator] {

  protected override val dim: Int = bcParameters.value.size
  private val numFeatures = if (fitIntercept) dim - 2 else dim - 1
  private val intercept = if (fitIntercept) bcParameters.value(dim - 2) else 0.0
  // make transient so we do not serialize between aggregation stages
  @transient private lazy val linear = Vectors.dense(bcParameters.value.toArray.take(numFeatures))

  /**
   * Add a new training instance block to this BlockHuberAggregator, and update the loss and
   * gradient of the objective function.
   *
   * @param block The instance block of data point to be added.
   * @return This BlockHuberAggregator object.
   */
  def add(block: InstanceBlock): BlockHuberAggregator = {
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.weightIter.forall(_ >= 0),
      s"instance weights ${block.weightIter.mkString("[", ",", "]")} has to be >= 0.0")

    if (block.weightIter.forall(_ == 0)) return this

    val size = block.size
    val sigma = bcParameters.value(dim - 1)

    // vec here represents margins or dotProducts
    val vec = if (fitIntercept) {
      Vectors.dense(Array.fill(size)(intercept)).toDense
    } else {
      Vectors.zeros(size).toDense
    }
    BLAS.gemv(1.0, block.matrix, linear, 1.0, vec)

    // in-place convert margins to multipliers
    // then, vec represents multipliers
    var sigmaGradSum = 0.0
    var localLossSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      if (weight > 0) {
        val label = block.getLabel(i)
        val margin = vec(i)
        val linearLoss = label - margin

        if (math.abs(linearLoss) <= sigma * epsilon) {
          localLossSum += 0.5 * weight * (sigma + math.pow(linearLoss, 2.0) / sigma)
          val linearLossDivSigma = linearLoss / sigma
          val multiplier = -1.0 * weight * linearLossDivSigma
          vec.values(i) = multiplier
          sigmaGradSum += 0.5 * weight * (1.0 - math.pow(linearLossDivSigma, 2.0))
        } else {
          localLossSum += 0.5 * weight *
            (sigma + 2.0 * epsilon * math.abs(linearLoss) - sigma * epsilon * epsilon)
          val sign = if (linearLoss >= 0) -1.0 else 1.0
          val multiplier = weight * sign * epsilon
          vec.values(i) = multiplier
          sigmaGradSum += 0.5 * weight * (1.0 - epsilon * epsilon)
        }
      } else { vec.values(i) = 0.0 }
      i += 1
    }
    lossSum += localLossSum
    weightSum += block.weightIter.sum

    block.matrix match {
      case dm: DenseMatrix =>
        BLAS.nativeBLAS.dgemv("N", dm.numCols, dm.numRows, 1.0, dm.values, dm.numCols,
          vec.values, 1, 1.0, gradientSumArray, 1)

      case sm: SparseMatrix =>
        val linearGradSumVec = Vectors.zeros(numFeatures).toDense
        BLAS.gemv(1.0, sm.transpose, vec, 0.0, linearGradSumVec)
        BLAS.getBLAS(numFeatures).daxpy(numFeatures, 1.0, linearGradSumVec.values, 1,
          gradientSumArray, 1)
    }

    gradientSumArray(dim - 1) += sigmaGradSum
    if (fitIntercept) gradientSumArray(dim - 2) += vec.values.sum

    this
  }
}
