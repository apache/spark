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
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._

/**
 * AFTAggregator computes the gradient and loss for a AFT loss function,
 * as used in AFT survival regression for samples in sparse or dense vector in an online fashion.
 *
 * The loss function and likelihood function under the AFT model based on:
 * Lawless, J. F., Statistical Models and Methods for Lifetime Data,
 * New York: John Wiley & Sons, Inc. 2003.
 *
 * Two AFTAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * Given the values of the covariates $x^{'}$, for random lifetime $t_{i}$ of subjects i = 1,..,n,
 * with possible right-censoring, the likelihood function under the AFT model is given as
 *
 * <blockquote>
 *    $$
 *    L(\beta,\sigma)=\prod_{i=1}^n[\frac{1}{\sigma}f_{0}
 *      (\frac{\log{t_{i}}-x^{'}\beta}{\sigma})]^{\delta_{i}}S_{0}
 *    (\frac{\log{t_{i}}-x^{'}\beta}{\sigma})^{1-\delta_{i}}
 *    $$
 * </blockquote>
 *
 * Where $\delta_{i}$ is the indicator of the event has occurred i.e. uncensored or not.
 * Using $\epsilon_{i}=\frac{\log{t_{i}}-x^{'}\beta}{\sigma}$, the log-likelihood function
 * assumes the form
 *
 * <blockquote>
 *    $$
 *    \iota(\beta,\sigma)=\sum_{i=1}^{n}[-\delta_{i}\log\sigma+
 *    \delta_{i}\log{f_{0}}(\epsilon_{i})+(1-\delta_{i})\log{S_{0}(\epsilon_{i})}]
 *    $$
 * </blockquote>
 * Where $S_{0}(\epsilon_{i})$ is the baseline survivor function,
 * and $f_{0}(\epsilon_{i})$ is corresponding density function.
 *
 * The most commonly used log-linear survival regression method is based on the Weibull
 * distribution of the survival time. The Weibull distribution for lifetime corresponding
 * to extreme value distribution for log of the lifetime,
 * and the $S_{0}(\epsilon)$ function is
 *
 * <blockquote>
 *    $$
 *    S_{0}(\epsilon_{i})=\exp(-e^{\epsilon_{i}})
 *    $$
 * </blockquote>
 *
 * and the $f_{0}(\epsilon_{i})$ function is
 *
 * <blockquote>
 *    $$
 *    f_{0}(\epsilon_{i})=e^{\epsilon_{i}}\exp(-e^{\epsilon_{i}})
 *    $$
 * </blockquote>
 *
 * The log-likelihood function for Weibull distribution of lifetime is
 *
 * <blockquote>
 *    $$
 *    \iota(\beta,\sigma)=
 *    -\sum_{i=1}^n[\delta_{i}\log\sigma-\delta_{i}\epsilon_{i}+e^{\epsilon_{i}}]
 *    $$
 * </blockquote>
 *
 * Due to minimizing the negative log-likelihood equivalent to maximum a posteriori probability,
 * the loss function we use to optimize is $-\iota(\beta,\sigma)$.
 * The gradient functions for $\beta$ and $\log\sigma$ respectively are
 *
 * <blockquote>
 *    $$
 *    \frac{\partial (-\iota)}{\partial \beta}=
 *    \sum_{1=1}^{n}[\delta_{i}-e^{\epsilon_{i}}]\frac{x_{i}}{\sigma} \\
 *
 *    \frac{\partial (-\iota)}{\partial (\log\sigma)}=
 *    \sum_{i=1}^{n}[\delta_{i}+(\delta_{i}-e^{\epsilon_{i}})\epsilon_{i}]
 *    $$
 * </blockquote>
 *
 * @param bcCoefficients The broadcasted value includes three part: 1, regression coefficients
 *                       corresponding to the features; 2, the intercept; 3, the log of scale
 *                       parameter.
 * @param fitIntercept   Whether to fit an intercept term.
 * @param bcFeaturesStd The broadcast standard deviation values of the features.
 */

private[ml] class AFTAggregator(
    bcFeaturesStd: Broadcast[Array[Double]],
    fitIntercept: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[Instance, AFTAggregator] {

  protected override val dim: Int = bcCoefficients.value.size

  /**
   * Add a new training data to this AFTAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param data The Instance representation for one data point to be added into this aggregator.
   * @return This AFTAggregator object.
   */
  def add(data: Instance): this.type = {
    val coefficients = bcCoefficients.value.toArray
    val intercept = coefficients(dim - 2)
    // sigma is the scale parameter of the AFT model
    val sigma = math.exp(coefficients(dim - 1))

    val xi = data.features
    val ti = data.label
    val delta = data.weight

    require(ti > 0.0, "The lifetime or label should be  greater than 0.")

    val localFeaturesStd = bcFeaturesStd.value

    val margin = {
      var sum = 0.0
      xi.foreachNonZero { (index, value) =>
        if (localFeaturesStd(index) != 0.0) {
          sum += coefficients(index) * (value / localFeaturesStd(index))
        }
      }
      sum + intercept
    }
    val epsilon = (math.log(ti) - margin) / sigma

    lossSum += delta * math.log(sigma) - delta * epsilon + math.exp(epsilon)

    val multiplier = (delta - math.exp(epsilon)) / sigma

    xi.foreachNonZero { (index, value) =>
      if (localFeaturesStd(index) != 0.0) {
        gradientSumArray(index) += multiplier * (value / localFeaturesStd(index))
      }
    }
    gradientSumArray(dim - 2) += { if (fitIntercept) multiplier else 0.0 }
    gradientSumArray(dim - 1) += delta + multiplier * sigma * epsilon
    weightSum += 1.0

    this
  }
}


/**
 * BlockAFTAggregator computes the gradient and loss as used in AFT survival regression
 * for blocks in sparse or dense matrix in an online fashion.
 *
 * Two BlockAFTAggregators can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to be standardized before computation.
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term.
 */
private[ml] class BlockAFTAggregator(
    fitIntercept: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock,
    BlockAFTAggregator] {

  protected override val dim: Int = bcCoefficients.value.size
  private val numFeatures = dim - 2

  @transient private lazy val coefficientsArray = bcCoefficients.value match {
    case DenseVector(values) => values
    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector" +
      s" but got type ${bcCoefficients.value.getClass}.")
  }

  @transient private lazy val linear = Vectors.dense(coefficientsArray.take(numFeatures))

  /**
   * Add a new training instance block to this BlockAFTAggregator, and update the loss and
   * gradient of the objective function.
   *
   * @return This BlockAFTAggregator object.
   */
  def add(block: InstanceBlock): this.type = {
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.labels.forall(_ > 0.0), "The lifetime or label should be  greater than 0.")

    val size = block.size
    val intercept = coefficientsArray(dim - 2)
    // sigma is the scale parameter of the AFT model
    val sigma = math.exp(coefficientsArray(dim - 1))

    // vec here represents margins
    val vec = if (fitIntercept) {
      Vectors.dense(Array.fill(size)(intercept)).toDense
    } else {
      Vectors.zeros(size).toDense
    }
    BLAS.gemv(1.0, block.matrix, linear, 1.0, vec)

    // in-place convert margins to gradient scales
    // then, vec represents gradient scales
    var localLossSum = 0.0
    var i = 0
    var sigmaGradSum = 0.0
    while (i < size) {
      val ti = block.getLabel(i)
      // here use Instance.weight to store censor for convenience
      val delta = block.getWeight(i)
      val margin = vec(i)
      val epsilon = (math.log(ti) - margin) / sigma
      val expEpsilon = math.exp(epsilon)
      localLossSum += delta * math.log(sigma) - delta * epsilon + expEpsilon
      val multiplier = (delta - expEpsilon) / sigma
      vec.values(i) = multiplier
      sigmaGradSum += delta + multiplier * sigma * epsilon
      i += 1
    }
    lossSum += localLossSum
    weightSum += size

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

    if (fitIntercept) gradientSumArray(dim - 2) += vec.values.sum
    gradientSumArray(dim - 1) += sigmaGradSum

    this
  }
}
