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
 * AFTBlockAggregator computes the gradient and loss as used in AFT survival regression
 * for blocks in sparse or dense matrix in an online fashion.
 *
 * Two AFTBlockAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to already have be scaled (multiplied by bcInverseStd,
 * but NOT centered) before computation.
 *
 * @param bcCoefficients The coefficients corresponding to the features, it includes three parts:
 *                       1, regression coefficients corresponding to the features;
 *                       2, the intercept;
 *                       3, the log of scale parameter.
 * @param fitIntercept Whether to fit an intercept term. When true, will perform data centering
 *                     in a virtual way. Then we MUST adjust the intercept of both initial
 *                     coefficients and final solution in the caller.
 */
private[ml] class AFTBlockAggregator (
    bcScaledMean: Broadcast[Array[Double]],
    fitIntercept: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock,
    AFTBlockAggregator] {

  protected override val dim: Int = bcCoefficients.value.size
  private val numFeatures = dim - 2

  @transient private lazy val coefficientsArray = bcCoefficients.value match {
    case DenseVector(values) => values
    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector" +
      s" but got type ${bcCoefficients.value.getClass}.")
  }

  // pre-computed margin of an empty vector.
  // with this variable as an offset, for a sparse vector, we only need to
  // deal with non-zero values in prediction.
  private val marginOffset = if (fitIntercept) {
    coefficientsArray(dim - 2) -
      BLAS.getBLAS(numFeatures).ddot(numFeatures, coefficientsArray, 1, bcScaledMean.value, 1)
  } else {
    Double.NaN
  }

  @transient private var buffer: Array[Double] = _

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
    require(block.labels.forall(_ > 0.0), "The lifetime or label should be greater than 0.")

    val size = block.size
    // sigma is the scale parameter of the AFT model
    val sigma = math.exp(coefficientsArray(dim - 1))

    if (buffer == null || buffer.length < size) {
      buffer = Array.ofDim[Double](size)
    }

    // arr here represents margins
    val arr = buffer
    if (fitIntercept) {
      java.util.Arrays.fill(arr, 0, size, marginOffset)
      BLAS.gemv(1.0, block.matrix, coefficientsArray, 1.0, arr)
    } else {
      BLAS.gemv(1.0, block.matrix, coefficientsArray, 0.0, arr)
    }

    // in-place convert margins to gradient scales
    // then, arr represents gradient scales
    var localLossSum = 0.0
    var i = 0
    var sigmaGradSum = 0.0
    var multiplierSum = 0.0
    while (i < size) {
      val ti = block.getLabel(i)
      // here use Instance.weight to store censor for convenience
      val delta = block.getWeight(i)
      val margin = arr(i)
      val epsilon = (math.log(ti) - margin) / sigma
      val expEpsilon = math.exp(epsilon)
      localLossSum += delta * math.log(sigma) - delta * epsilon + expEpsilon
      val multiplier = (delta - expEpsilon) / sigma
      arr(i) = multiplier
      multiplierSum += multiplier
      sigmaGradSum += delta + multiplier * sigma * epsilon
      i += 1
    }
    lossSum += localLossSum
    weightSum += size

    // update the linear part of gradientSumArray
    BLAS.gemv(1.0, block.matrix.transpose, arr, 1.0, gradientSumArray)

    if (fitIntercept) {
      // above update of the linear part of gradientSumArray does NOT take the centering
      // into account, here we need to adjust this part.
      BLAS.javaBLAS.daxpy(numFeatures, -multiplierSum, bcScaledMean.value, 1,
        gradientSumArray, 1)

      gradientSumArray(dim - 2) += multiplierSum
    }

    gradientSumArray(dim - 1) += sigmaGradSum

    this
  }
}
