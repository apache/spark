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
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.util.MLUtils

/**
 * LogisticAggregator computes the gradient and loss for binary or multinomial logistic (softmax)
 * loss function, as used in classification for instances in sparse or dense vector in an online
 * fashion.
 *
 * Two LogisticAggregators can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * For improving the convergence rate during the optimization process and also to prevent against
 * features with very large variances exerting an overly large influence during model training,
 * packages like R's GLMNET perform the scaling to unit variance and remove the mean in order to
 * reduce the condition number. The model is then trained in this scaled space, but returns the
 * coefficients in the original scale. See page 9 in
 * http://cran.r-project.org/web/packages/glmnet/glmnet.pdf
 *
 * However, we don't want to apply the [[org.apache.spark.ml.feature.StandardScaler]] on the
 * training dataset, and then cache the standardized dataset since it will create a lot of overhead.
 * As a result, we perform the scaling implicitly when we compute the objective function (though
 * we do not subtract the mean).
 *
 * Note that there is a difference between multinomial (softmax) and binary loss. The binary case
 * uses one outcome class as a "pivot" and regresses the other class against the pivot. In the
 * multinomial case, the softmax loss function is used to model each class probability
 * independently. Using softmax loss produces `K` sets of coefficients, while using a pivot class
 * produces `K - 1` sets of coefficients (a single coefficient vector in the binary case). In the
 * binary case, we can say that the coefficients are shared between the positive and negative
 * classes. When regularization is applied, multinomial (softmax) loss will produce a result
 * different from binary loss since the positive and negative don't share the coefficients while the
 * binary regression shares the coefficients between positive and negative.
 *
 * The following is a mathematical derivation for the multinomial (softmax) loss.
 *
 * The probability of the multinomial outcome $y$ taking on any of the K possible outcomes is:
 *
 * <blockquote>
 *    $$
 *    P(y_i=0|\vec{x}_i, \beta) = \frac{e^{\vec{x}_i^T \vec{\beta}_0}}{\sum_{k=0}^{K-1}
 *       e^{\vec{x}_i^T \vec{\beta}_k}} \\
 *    P(y_i=1|\vec{x}_i, \beta) = \frac{e^{\vec{x}_i^T \vec{\beta}_1}}{\sum_{k=0}^{K-1}
 *       e^{\vec{x}_i^T \vec{\beta}_k}}\\
 *    P(y_i=K-1|\vec{x}_i, \beta) = \frac{e^{\vec{x}_i^T \vec{\beta}_{K-1}}\,}{\sum_{k=0}^{K-1}
 *       e^{\vec{x}_i^T \vec{\beta}_k}}
 *    $$
 * </blockquote>
 *
 * The model coefficients $\beta = (\beta_0, \beta_1, \beta_2, ..., \beta_{K-1})$ become a matrix
 * which has dimension of $K \times (N+1)$ if the intercepts are added. If the intercepts are not
 * added, the dimension will be $K \times N$.
 *
 * Note that the coefficients in the model above lack identifiability. That is, any constant scalar
 * can be added to all of the coefficients and the probabilities remain the same.
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *    \frac{e^{\vec{x}_i^T \left(\vec{\beta}_0 + \vec{c}\right)}}{\sum_{k=0}^{K-1}
 *       e^{\vec{x}_i^T \left(\vec{\beta}_k + \vec{c}\right)}}
 *    = \frac{e^{\vec{x}_i^T \vec{\beta}_0}e^{\vec{x}_i^T \vec{c}}\,}{e^{\vec{x}_i^T \vec{c}}
 *       \sum_{k=0}^{K-1} e^{\vec{x}_i^T \vec{\beta}_k}}
 *    = \frac{e^{\vec{x}_i^T \vec{\beta}_0}}{\sum_{k=0}^{K-1} e^{\vec{x}_i^T \vec{\beta}_k}}
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * However, when regularization is added to the loss function, the coefficients are indeed
 * identifiable because there is only one set of coefficients which minimizes the regularization
 * term. When no regularization is applied, we choose the coefficients with the minimum L2
 * penalty for consistency and reproducibility. For further discussion see:
 *
 * Friedman, et al. "Regularization Paths for Generalized Linear Models via Coordinate Descent"
 *
 * The loss of objective function for a single instance of data (we do not include the
 * regularization term here for simplicity) can be written as
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *    \ell\left(\beta, x_i\right) &= -log{P\left(y_i \middle| \vec{x}_i, \beta\right)} \\
 *    &= log\left(\sum_{k=0}^{K-1}e^{\vec{x}_i^T \vec{\beta}_k}\right) - \vec{x}_i^T \vec{\beta}_y\\
 *    &= log\left(\sum_{k=0}^{K-1} e^{margins_k}\right) - margins_y
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * where ${margins}_k = \vec{x}_i^T \vec{\beta}_k$.
 *
 * For optimization, we have to calculate the first derivative of the loss function, and a simple
 * calculation shows that
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *    \frac{\partial \ell(\beta, \vec{x}_i, w_i)}{\partial \beta_{j, k}}
 *    &= x_{i,j} \cdot w_i \cdot \left(\frac{e^{\vec{x}_i \cdot \vec{\beta}_k}}{\sum_{k'=0}^{K-1}
 *      e^{\vec{x}_i \cdot \vec{\beta}_{k'}}\,} - I_{y=k}\right) \\
 *    &= x_{i, j} \cdot w_i \cdot multiplier_k
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * where $w_i$ is the sample weight, $I_{y=k}$ is an indicator function
 *
 *  <blockquote>
 *    $$
 *    I_{y=k} = \begin{cases}
 *          1 & y = k \\
 *          0 & else
 *       \end{cases}
 *    $$
 * </blockquote>
 *
 * and
 *
 * <blockquote>
 *    $$
 *    multiplier_k = \left(\frac{e^{\vec{x}_i \cdot \vec{\beta}_k}}{\sum_{k=0}^{K-1}
 *       e^{\vec{x}_i \cdot \vec{\beta}_k}} - I_{y=k}\right)
 *    $$
 * </blockquote>
 *
 * If any of margins is larger than 709.78, the numerical computation of multiplier and loss
 * function will suffer from arithmetic overflow. This issue occurs when there are outliers in
 * data which are far away from the hyperplane, and this will cause the failing of training once
 * infinity is introduced. Note that this is only a concern when max(margins) &gt; 0.
 *
 * Fortunately, when max(margins) = maxMargin &gt; 0, the loss function and the multiplier can
 * easily be rewritten into the following equivalent numerically stable formula.
 *
 * <blockquote>
 *    $$
 *    \ell\left(\beta, x\right) = log\left(\sum_{k=0}^{K-1} e^{margins_k - maxMargin}\right) -
 *       margins_{y} + maxMargin
 *    $$
 * </blockquote>
 *
 * Note that each term, $(margins_k - maxMargin)$ in the exponential is no greater than zero; as a
 * result, overflow will not happen with this formula.
 *
 * For $multiplier$, a similar trick can be applied as the following,
 *
 * <blockquote>
 *    $$
 *    multiplier_k = \left(\frac{e^{\vec{x}_i \cdot \vec{\beta}_k - maxMargin}}{\sum_{k'=0}^{K-1}
 *       e^{\vec{x}_i \cdot \vec{\beta}_{k'} - maxMargin}} - I_{y=k}\right)
 *    $$
 * </blockquote>
 *
 *
 * @param bcCoefficients The broadcast coefficients corresponding to the features.
 * @param bcFeaturesStd The broadcast standard deviation values of the features.
 * @param numClasses the number of possible outcomes for k classes classification problem in
 *                   Multinomial Logistic Regression.
 * @param fitIntercept Whether to fit an intercept term.
 * @param multinomial Whether to use multinomial (softmax) or binary loss
 * @note In order to avoid unnecessary computation during calculation of the gradient updates
 * we lay out the coefficients in column major order during training. This allows us to
 * perform feature standardization once, while still retaining sequential memory access
 * for speed. We convert back to row major order when we create the model,
 * since this form is optimal for the matrix operations used for prediction.
 */
private[ml] class LogisticAggregator(
    bcFeaturesStd: Broadcast[Array[Double]],
    numClasses: Int,
    fitIntercept: Boolean,
    multinomial: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[Instance, LogisticAggregator] with Logging {

  private val numFeatures = bcFeaturesStd.value.length
  private val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1 else numFeatures
  private val coefficientSize = bcCoefficients.value.size
  protected override val dim: Int = coefficientSize
  if (multinomial) {
    require(numClasses ==  coefficientSize / numFeaturesPlusIntercept, s"The number of " +
      s"coefficients should be ${numClasses * numFeaturesPlusIntercept} but was $coefficientSize")
  } else {
    require(coefficientSize == numFeaturesPlusIntercept, s"Expected $numFeaturesPlusIntercept " +
      s"coefficients but got $coefficientSize")
    require(numClasses == 1 || numClasses == 2, s"Binary logistic aggregator requires numClasses " +
      s"in {1, 2} but found $numClasses.")
  }

  @transient private lazy val coefficientsArray: Array[Double] = bcCoefficients.value match {
    case DenseVector(values) => values
    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector but " +
      s"got type ${bcCoefficients.value.getClass}.)")
  }

  if (multinomial && numClasses <= 2) {
    logInfo(s"Multinomial logistic regression for binary classification yields separate " +
      s"coefficients for positive and negative classes. When no regularization is applied, the" +
      s"result will be effectively the same as binary logistic regression. When regularization" +
      s"is applied, multinomial loss will produce a result different from binary loss.")
  }

  /** Update gradient and loss using binary loss function. */
  private def binaryUpdateInPlace(features: Vector, weight: Double, label: Double): Unit = {

    val localFeaturesStd = bcFeaturesStd.value
    val localCoefficients = coefficientsArray
    val localGradientArray = gradientSumArray
    val margin = - {
      var sum = 0.0
      features.foreachActive { (index, value) =>
        if (localFeaturesStd(index) != 0.0 && value != 0.0) {
          sum += localCoefficients(index) * value / localFeaturesStd(index)
        }
      }
      if (fitIntercept) sum += localCoefficients(numFeaturesPlusIntercept - 1)
      sum
    }

    val multiplier = weight * (1.0 / (1.0 + math.exp(margin)) - label)

    features.foreachActive { (index, value) =>
      if (localFeaturesStd(index) != 0.0 && value != 0.0) {
        localGradientArray(index) += multiplier * value / localFeaturesStd(index)
      }
    }

    if (fitIntercept) {
      localGradientArray(numFeaturesPlusIntercept - 1) += multiplier
    }

    if (label > 0) {
      // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
      lossSum += weight * MLUtils.log1pExp(margin)
    } else {
      lossSum += weight * (MLUtils.log1pExp(margin) - margin)
    }
  }

  /** Update gradient and loss using multinomial (softmax) loss function. */
  private def multinomialUpdateInPlace(features: Vector, weight: Double, label: Double): Unit = {
    // TODO: use level 2 BLAS operations
    /*
      Note: this can still be used when numClasses = 2 for binary
      logistic regression without pivoting.
     */
    val localFeaturesStd = bcFeaturesStd.value
    val localCoefficients = coefficientsArray
    val localGradientArray = gradientSumArray

    // marginOfLabel is margins(label) in the formula
    var marginOfLabel = 0.0
    var maxMargin = Double.NegativeInfinity

    val margins = new Array[Double](numClasses)
    features.foreachActive { (index, value) =>
      if (localFeaturesStd(index) != 0.0 && value != 0.0) {
        val stdValue = value / localFeaturesStd(index)
        var j = 0
        while (j < numClasses) {
          margins(j) += localCoefficients(index * numClasses + j) * stdValue
          j += 1
        }
      }
    }
    var i = 0
    while (i < numClasses) {
      if (fitIntercept) {
        margins(i) += localCoefficients(numClasses * numFeatures + i)
      }
      if (i == label.toInt) marginOfLabel = margins(i)
      if (margins(i) > maxMargin) {
        maxMargin = margins(i)
      }
      i += 1
    }

    /**
     * When maxMargin is greater than 0, the original formula could cause overflow.
     * We address this by subtracting maxMargin from all the margins, so it's guaranteed
     * that all of the new margins will be smaller than zero to prevent arithmetic overflow.
     */
    val multipliers = new Array[Double](numClasses)
    val sum = {
      var temp = 0.0
      var i = 0
      while (i < numClasses) {
        if (maxMargin > 0) margins(i) -= maxMargin
        val exp = math.exp(margins(i))
        temp += exp
        multipliers(i) = exp
        i += 1
      }
      temp
    }

    margins.indices.foreach { i =>
      multipliers(i) = multipliers(i) / sum - (if (label == i) 1.0 else 0.0)
    }
    features.foreachActive { (index, value) =>
      if (localFeaturesStd(index) != 0.0 && value != 0.0) {
        val stdValue = value / localFeaturesStd(index)
        var j = 0
        while (j < numClasses) {
          localGradientArray(index * numClasses + j) += weight * multipliers(j) * stdValue
          j += 1
        }
      }
    }
    if (fitIntercept) {
      var i = 0
      while (i < numClasses) {
        localGradientArray(numFeatures * numClasses + i) += weight * multipliers(i)
        i += 1
      }
    }

    val loss = if (maxMargin > 0) {
      math.log(sum) - marginOfLabel + maxMargin
    } else {
      math.log(sum) - marginOfLabel
    }
    lossSum += weight * loss
  }

  /**
   * Add a new training instance to this LogisticAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This LogisticAggregator object.
   */
  def add(instance: Instance): this.type = {
    instance match { case Instance(label, weight, features) =>
      require(numFeatures == features.size, s"Dimensions mismatch when adding new instance." +
        s" Expecting $numFeatures but got ${features.size}.")
      require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

      if (weight == 0.0) return this

      if (multinomial) {
        multinomialUpdateInPlace(features, weight, label)
      } else {
        binaryUpdateInPlace(features, weight, label)
      }
      weightSum += weight
      this
    }
  }
}
