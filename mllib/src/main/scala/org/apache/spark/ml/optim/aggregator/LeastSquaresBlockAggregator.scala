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
import org.apache.spark.ml.feature.InstanceBlock
import org.apache.spark.ml.linalg._


/**
 * LeastSquaresBlockAggregator computes the gradient and loss used in Linear Regression
 * for blocks in sparse or dense matrix in an online fashion.
 *
 * Two LeastSquaresBlockAggregator can be merged together to have a summary of loss and
 * gradient of the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to already have be scaled (multiplied by bcInverseStd,
 * but NOT centered) before computation.
 *
 * NOTE: the virtual centering is NOT applied, because the intercept here is computed using
 * closed form after the coefficients are converged.
 * See this discussion for detail.
 * http://stats.stackexchange.com/questions/13617/how-is-the-intercept-computed-in-glmnet
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term. Note that virtual centering is NOT
 *                     applied, because the intercept here is computed using closed form after
 *                     the coefficients are converged.
 */
private[ml] class LeastSquaresBlockAggregator(
    bcInverseStd: Broadcast[Array[Double]],
    bcScaledMean: Broadcast[Array[Double]],
    fitIntercept: Boolean,
    labelStd: Double,
    labelMean: Double)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock, LeastSquaresBlockAggregator]
  with Logging {

  require(labelStd > 0.0, s"${this.getClass.getName} requires the label standard " +
    s"deviation to be positive.")

  private val numFeatures = bcInverseStd.value.length

  protected override val dim: Int = numFeatures

  @transient private lazy val effectiveCoefVec = bcCoefficients.value match {
    case DenseVector(values) =>
      val inverseStd = bcInverseStd.value
      val effectiveCoefArray = Array.tabulate(numFeatures)(
        i => if (inverseStd(i) != 0) values(i) else 0.0)
      new DenseVector(effectiveCoefArray)

    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector but " +
      s"got type ${bcCoefficients.value.getClass}.)")
  }

  private val offset = if (fitIntercept) {
    labelMean / labelStd -
      BLAS.javaBLAS.ddot(numFeatures, bcCoefficients.value.toArray, 1, bcScaledMean.value, 1)
  } else {
    Double.NaN
  }

  /**
   * Add a new training instance block to this LeastSquaresBlockAggregator, and update the loss
   * and gradient of the objective function.
   *
   * @param block The instance block of data point to be added.
   * @return This LeastSquaresBlockAggregator object.
   */
  def add(block: InstanceBlock): this.type = {
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.weightIter.forall(_ >= 0),
      s"instance weights ${block.weightIter.mkString("[", ",", "]")} has to be >= 0.0")

    if (block.weightIter.forall(_ == 0)) return this

    val size = block.size

    // vec/arr here represents diffs
    val vec = new DenseVector(Array.ofDim[Double](size))
    val arr = vec.values
    if (fitIntercept) java.util.Arrays.fill(arr, offset)
    BLAS.javaBLAS.daxpy(size, -1.0 / labelStd, block.labels, 1, arr, 1)
    BLAS.gemv(1.0, block.matrix, effectiveCoefVec, 1.0, vec)

    // in-place convert diffs to multipliers
    // then, vec/arr represents multipliers
    var localLossSum = 0.0
    var localWeightSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      localWeightSum += weight
      val diff = arr(i)
      localLossSum += weight * diff * diff / 2
      val multiplier = weight * diff
      arr(i) = multiplier
      i += 1
    }
    lossSum += localLossSum
    weightSum += localWeightSum

    val gradSumVec = new DenseVector(gradientSumArray)
    BLAS.gemv(1.0, block.matrix.transpose, vec, 1.0, gradSumVec)

    this
  }
}
