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
 * HingeBlockAggregator computes the gradient and loss for Huber loss function
 * as used in linear regression for blocks in sparse or dense matrix in an online fashion.
 *
 * Two BlockHuberAggregators can be merged together to have a summary of loss and gradient
 * of the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to already have be scaled (multiplied by bcInverseStd,
 * but NOT centered) before computation.
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term. When true, will perform data centering
 *                     in a virtual way. Then we MUST adjust the intercept of both initial
 *                     coefficients and final solution in the caller.
 */
private[ml] class HingeBlockAggregator(
    bcInverseStd: Broadcast[Array[Double]],
    bcScaledMean: Broadcast[Array[Double]],
    fitIntercept: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock, HingeBlockAggregator]
    with Logging {

  if (fitIntercept) {
    require(bcScaledMean != null && bcScaledMean.value.length == bcInverseStd.value.length,
      "scaled means is required when center the vectors")
  }

  private val numFeatures = bcInverseStd.value.length
  protected override val dim: Int = bcCoefficients.value.size

  @transient private lazy val coefficientsArray = bcCoefficients.value match {
    case DenseVector(values) => values
    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector but " +
      s"got type ${bcCoefficients.value.getClass}.)")
  }

  // pre-computed margin of an empty vector.
  // with this variable as an offset, for a sparse vector, we only need to
  // deal with non-zero values in prediction.
  private val marginOffset = if (fitIntercept) {
    coefficientsArray.last -
      BLAS.javaBLAS.ddot(numFeatures, coefficientsArray, 1, bcScaledMean.value, 1)
  } else {
    Double.NaN
  }

  @transient private var buffer: Array[Double] = _

  /**
   * Add a new training instance block to this HingeBlockAggregator, and update the loss
   * and gradient of the objective function.
   *
   * @param block The instance block of data point to be added.
   * @return This HingeBlockAggregator object.
   */
  def add(block: InstanceBlock): this.type = {
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.weightIter.forall(_ >= 0),
      s"instance weights ${block.weightIter.mkString("[", ",", "]")} has to be >= 0.0")

    if (block.weightIter.forall(_ == 0)) return this
    val size = block.size

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

    // in-place convert margins to multiplier
    // then, arr represents multiplier
    var localLossSum = 0.0
    var localWeightSum = 0.0
    var multiplierSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      localWeightSum += weight
      if (weight > 0) {
        // Our loss function with {0, 1} labels is max(0, 1 - (2y - 1) (f_w(x)))
        // Therefore the gradient is -(2y - 1)*x
        val label = block.getLabel(i)
        val labelScaled = label + label - 1.0
        val loss = (1.0 - labelScaled * arr(i)) * weight
        if (loss > 0) {
          localLossSum += loss
          val multiplier = -labelScaled * weight
          arr(i) = multiplier
          multiplierSum += multiplier
        } else { arr(i) = 0.0 }
      } else { arr(i) = 0.0 }
      i += 1
    }
    lossSum += localLossSum
    weightSum += localWeightSum

    // predictions are all correct, no gradient signal
    if (arr.forall(_ == 0)) return this

    // update the linear part of gradientSumArray
    BLAS.gemv(1.0, block.matrix.transpose, arr, 1.0, gradientSumArray)

    if (fitIntercept) {
      // above update of the linear part of gradientSumArray does NOT take the centering
      // into account, here we need to adjust this part.
      BLAS.javaBLAS.daxpy(numFeatures, -multiplierSum, bcScaledMean.value, 1,
        gradientSumArray, 1)

      // update the intercept part of gradientSumArray
      gradientSumArray(numFeatures) += multiplierSum
    }

    this
  }
}
