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
 * LeastSquaresAggregator computes the gradient and loss for a Least-squared loss function,
 * as used in linear regression for samples in sparse or dense vector in an online fashion.
 *
 * Two LeastSquaresAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * For improving the convergence rate during the optimization process, and also preventing against
 * features with very large variances exerting an overly large influence during model training,
 * package like R's GLMNET performs the scaling to unit variance and removing the mean to reduce
 * the condition number, and then trains the model in scaled space but returns the coefficients in
 * the original scale. See page 9 in http://cran.r-project.org/web/packages/glmnet/glmnet.pdf
 *
 * However, we don't want to apply the `StandardScaler` on the training dataset, and then cache
 * the standardized dataset since it will create a lot of overhead. As a result, we perform the
 * scaling implicitly when we compute the objective function. The following is the mathematical
 * derivation.
 *
 * Note that we don't deal with intercept by adding bias here, because the intercept
 * can be computed using closed form after the coefficients are converged.
 * See this discussion for detail.
 * http://stats.stackexchange.com/questions/13617/how-is-the-intercept-computed-in-glmnet
 *
 * When training with intercept enabled,
 * The objective function in the scaled space is given by
 *
 * <blockquote>
 *    $$
 *    L = 1/2n ||\sum_i w_i(x_i - \bar{x_i}) / \hat{x_i} - (y - \bar{y}) / \hat{y}||^2,
 *    $$
 * </blockquote>
 *
 * where $\bar{x_i}$ is the mean of $x_i$, $\hat{x_i}$ is the standard deviation of $x_i$,
 * $\bar{y}$ is the mean of label, and $\hat{y}$ is the standard deviation of label.
 *
 * If we fitting the intercept disabled (that is forced through 0.0),
 * we can use the same equation except we set $\bar{y}$ and $\bar{x_i}$ to 0 instead
 * of the respective means.
 *
 * This can be rewritten as
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *     L &= 1/2n ||\sum_i (w_i/\hat{x_i})x_i - \sum_i (w_i/\hat{x_i})\bar{x_i} - y / \hat{y}
 *          + \bar{y} / \hat{y}||^2 \\
 *       &= 1/2n ||\sum_i w_i^\prime x_i - y / \hat{y} + offset||^2 = 1/2n diff^2
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * where $w_i^\prime$ is the effective coefficients defined by $w_i/\hat{x_i}$, offset is
 *
 * <blockquote>
 *    $$
 *    - \sum_i (w_i/\hat{x_i})\bar{x_i} + \bar{y} / \hat{y}.
 *    $$
 * </blockquote>
 *
 * and diff is
 *
 * <blockquote>
 *    $$
 *    \sum_i w_i^\prime x_i - y / \hat{y} + offset
 *    $$
 * </blockquote>
 *
 * Note that the effective coefficients and offset don't depend on training dataset,
 * so they can be precomputed.
 *
 * Now, the first derivative of the objective function in scaled space is
 *
 * <blockquote>
 *    $$
 *    \frac{\partial L}{\partial w_i} = diff/N (x_i - \bar{x_i}) / \hat{x_i}
 *    $$
 * </blockquote>
 *
 * However, $(x_i - \bar{x_i})$ will densify the computation, so it's not
 * an ideal formula when the training dataset is sparse format.
 *
 * This can be addressed by adding the dense $\bar{x_i} / \hat{x_i}$ terms
 * in the end by keeping the sum of diff. The first derivative of total
 * objective function from all the samples is
 *
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *       \frac{\partial L}{\partial w_i} &=
 *           1/N \sum_j diff_j (x_{ij} - \bar{x_i}) / \hat{x_i} \\
 *         &= 1/N ((\sum_j diff_j x_{ij} / \hat{x_i}) - diffSum \bar{x_i} / \hat{x_i}) \\
 *         &= 1/N ((\sum_j diff_j x_{ij} / \hat{x_i}) + correction_i)
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * where $correction_i = - diffSum \bar{x_i} / \hat{x_i}$
 *
 * A simple math can show that diffSum is actually zero, so we don't even
 * need to add the correction terms in the end. From the definition of diff,
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *       diffSum &= \sum_j (\sum_i w_i(x_{ij} - \bar{x_i})
 *                    / \hat{x_i} - (y_j - \bar{y}) / \hat{y}) \\
 *         &= N * (\sum_i w_i(\bar{x_i} - \bar{x_i}) / \hat{x_i} - (\bar{y} - \bar{y}) / \hat{y}) \\
 *         &= 0
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * As a result, the first derivative of the total objective function only depends on
 * the training dataset, which can be easily computed in distributed fashion, and is
 * sparse format friendly.
 *
 * <blockquote>
 *    $$
 *    \frac{\partial L}{\partial w_i} = 1/N ((\sum_j diff_j x_{ij} / \hat{x_i})
 *    $$
 * </blockquote>
 *
 * @note The constructor is curried, since the cost function will repeatedly create new versions
 *       of this class for different coefficient vectors.
 *
 * @param labelStd The standard deviation value of the label.
 * @param labelMean The mean value of the label.
 * @param fitIntercept Whether to fit an intercept term.
 * @param bcFeaturesStd The broadcast standard deviation values of the features.
 * @param bcFeaturesMean The broadcast mean values of the features.
 * @param bcCoefficients The broadcast coefficients corresponding to the features.
 */
private[ml] class LeastSquaresAggregator(
    labelStd: Double,
    labelMean: Double,
    fitIntercept: Boolean,
    bcFeaturesStd: Broadcast[Array[Double]],
    bcFeaturesMean: Broadcast[Array[Double]])(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[Instance, LeastSquaresAggregator] {
  require(labelStd > 0.0, s"${this.getClass.getName} requires the label standard " +
    s"deviation to be positive.")

  private val numFeatures = bcFeaturesStd.value.length
  protected override val dim: Int = numFeatures
  // make transient so we do not serialize between aggregation stages
  @transient private lazy val featuresStd = bcFeaturesStd.value
  @transient private lazy val effectiveCoefAndOffset = {
    val coefficientsArray = bcCoefficients.value.toArray.clone()
    val featuresMean = bcFeaturesMean.value
    var sum = 0.0
    var i = 0
    val len = coefficientsArray.length
    while (i < len) {
      if (featuresStd(i) != 0.0) {
        coefficientsArray(i) /=  featuresStd(i)
        sum += coefficientsArray(i) * featuresMean(i)
      } else {
        coefficientsArray(i) = 0.0
      }
      i += 1
    }
    val offset = if (fitIntercept) labelMean / labelStd - sum else 0.0
    (Vectors.dense(coefficientsArray), offset)
  }
  // do not use tuple assignment above because it will circumvent the @transient tag
  @transient private lazy val effectiveCoefficientsVector = effectiveCoefAndOffset._1
  @transient private lazy val offset = effectiveCoefAndOffset._2

  /**
   * Add a new training instance to this LeastSquaresAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This LeastSquaresAggregator object.
   */
  def add(instance: Instance): LeastSquaresAggregator = {
    instance match { case Instance(label, weight, features) =>
      require(numFeatures == features.size, s"Dimensions mismatch when adding new sample." +
        s" Expecting $numFeatures but got ${features.size}.")
      require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

      if (weight == 0.0) return this

      val diff = BLAS.dot(features, effectiveCoefficientsVector) - label / labelStd + offset

      if (diff != 0) {
        val localGradientSumArray = gradientSumArray
        val localFeaturesStd = featuresStd
        features.foreachNonZero { (index, value) =>
          val fStd = localFeaturesStd(index)
          if (fStd != 0.0) {
            localGradientSumArray(index) += weight * diff * value / fStd
          }
        }
        lossSum += weight * diff * diff / 2.0
      }
      weightSum += weight
      this
    }
  }
}


/**
 * BlockLeastSquaresAggregator computes the gradient and loss for LeastSquares loss function
 * as used in linear regression for blocks in sparse or dense matrix in an online fashion.
 *
 * Two BlockLeastSquaresAggregators can be merged together to have a summary of loss and gradient
 * of the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to be standardized before computation.
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term.
 */
private[ml] class BlockLeastSquaresAggregator(
    labelStd: Double,
    labelMean: Double,
    fitIntercept: Boolean,
    bcFeaturesStd: Broadcast[Array[Double]],
    bcFeaturesMean: Broadcast[Array[Double]])(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock, BlockLeastSquaresAggregator] {
  require(labelStd > 0.0, s"${this.getClass.getName} requires the label standard " +
    s"deviation to be positive.")

  private val numFeatures = bcFeaturesStd.value.length
  protected override val dim: Int = numFeatures
  // make transient so we do not serialize between aggregation stages
  @transient private lazy val effectiveCoefAndOffset = {
    val coefficientsArray = bcCoefficients.value.toArray.clone()
    val featuresMean = bcFeaturesMean.value
    val featuresStd = bcFeaturesStd.value
    var sum = 0.0
    var i = 0
    val len = coefficientsArray.length
    while (i < len) {
      if (featuresStd(i) != 0.0) {
        sum += coefficientsArray(i) / featuresStd(i) * featuresMean(i)
      } else {
        coefficientsArray(i) = 0.0
      }
      i += 1
    }
    val offset = if (fitIntercept) labelMean / labelStd - sum else 0.0
    (Vectors.dense(coefficientsArray), offset)
  }

  /**
   * Add a new training instance block to this BlockLeastSquaresAggregator, and update the loss
   * and gradient of the objective function.
   *
   * @param block The instance block of data point to be added.
   * @return This BlockLeastSquaresAggregator object.
   */
  def add(block: InstanceBlock): BlockLeastSquaresAggregator = {
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.weightIter.forall(_ >= 0),
      s"instance weights ${block.weightIter.mkString("[", ",", "]")} has to be >= 0.0")

    if (block.weightIter.forall(_ == 0)) return this

    val size = block.size
    val (effectiveCoefficientsVec, offset) = effectiveCoefAndOffset

    // vec here represents diffs
    val vec = new DenseVector(Array.tabulate(size)(i => offset - block.getLabel(i) / labelStd))
    BLAS.gemv(1.0, block.matrix, effectiveCoefficientsVec, 1.0, vec)

    // in-place convert diffs to multipliers
    // then, vec represents multipliers
    var localLossSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      val diff = vec(i)
      localLossSum += weight * diff * diff / 2
      val multiplier = weight * diff
      vec.values(i) = multiplier
      i += 1
    }
    lossSum += localLossSum
    weightSum += block.weightIter.sum

    val gradSumVec = new DenseVector(gradientSumArray)
    BLAS.gemv(1.0, block.matrix.transpose, vec, 1.0, gradSumVec)

    this
  }
}
