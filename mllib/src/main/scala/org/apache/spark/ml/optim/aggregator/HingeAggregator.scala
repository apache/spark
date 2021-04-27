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
 * HingeAggregator computes the gradient and loss for Hinge loss function as used in
 * binary classification for instances in sparse or dense vector in an online fashion.
 *
 * Two HingeAggregators can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * This class standardizes feature values during computation using bcFeaturesStd.
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term.
 * @param bcFeaturesStd The standard deviation values of the features.
 */
private[ml] class HingeAggregator(
    bcFeaturesStd: Broadcast[Array[Double]],
    fitIntercept: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[Instance, HingeAggregator] {

  private val numFeatures = bcFeaturesStd.value.length
  private val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1 else numFeatures
  @transient private lazy val coefficientsArray = bcCoefficients.value match {
    case DenseVector(values) => values
    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector" +
      s" but got type ${bcCoefficients.value.getClass}.")
  }
  protected override val dim: Int = numFeaturesPlusIntercept

  /**
   * Add a new training instance to this HingeAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This HingeAggregator object.
   */
  def add(instance: Instance): this.type = {
    instance match { case Instance(label, weight, features) =>
      require(numFeatures == features.size, s"Dimensions mismatch when adding new instance." +
        s" Expecting $numFeatures but got ${features.size}.")
      require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

      if (weight == 0.0) return this
      val localFeaturesStd = bcFeaturesStd.value
      val localCoefficients = coefficientsArray
      val localGradientSumArray = gradientSumArray

      val dotProduct = {
        var sum = 0.0
        features.foreachNonZero { (index, value) =>
          if (localFeaturesStd(index) != 0.0) {
            sum += localCoefficients(index) * value / localFeaturesStd(index)
          }
        }
        if (fitIntercept) sum += localCoefficients(numFeaturesPlusIntercept - 1)
        sum
      }
      // Our loss function with {0, 1} labels is max(0, 1 - (2y - 1) (f_w(x)))
      // Therefore the gradient is -(2y - 1)*x
      val labelScaled = 2 * label - 1.0
      val loss = if (1.0 > labelScaled * dotProduct) {
        (1.0 - labelScaled * dotProduct) * weight
      } else {
        0.0
      }

      if (1.0 > labelScaled * dotProduct) {
        val gradientScale = -labelScaled * weight
        features.foreachNonZero { (index, value) =>
          if (localFeaturesStd(index) != 0.0) {
            localGradientSumArray(index) += value * gradientScale / localFeaturesStd(index)
          }
        }
        if (fitIntercept) {
          localGradientSumArray(localGradientSumArray.length - 1) += gradientScale
        }
      }

      lossSum += loss
      weightSum += weight
      this
    }
  }
}


/**
 * BlockHingeAggregator computes the gradient and loss for Hinge loss function as used in
 * binary classification for blocks in sparse or dense matrix in an online fashion.
 *
 * Two BlockHingeAggregators can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to be standardized before computation.
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term.
 */
private[ml] class BlockHingeAggregator(
    fitIntercept: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock, BlockHingeAggregator] {

  protected override val dim: Int = bcCoefficients.value.size
  private val numFeatures = if (fitIntercept) dim - 1 else dim

  @transient private lazy val coefficientsArray = bcCoefficients.value match {
    case DenseVector(values) => values
    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector" +
      s" but got type ${bcCoefficients.value.getClass}.")
  }

  @transient private lazy val linear = {
    val linear = if (fitIntercept) coefficientsArray.take(numFeatures) else coefficientsArray
    Vectors.dense(linear)
  }

  /**
   * Add a new training instance block to this BlockHingeAggregator, and update the loss and
   * gradient of the objective function.
   *
   * @param block The InstanceBlock to be added.
   * @return This BlockHingeAggregator object.
   */
  def add(block: InstanceBlock): this.type = {
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.weightIter.forall(_ >= 0),
      s"instance weights ${block.weightIter.mkString("[", ",", "]")} has to be >= 0.0")

    if (block.weightIter.forall(_ == 0)) return this
    val size = block.size

    // vec here represents dotProducts
    val vec = if (fitIntercept) {
      Vectors.dense(Array.fill(size)(coefficientsArray.last)).toDense
    } else {
      Vectors.zeros(size).toDense
    }
    BLAS.gemv(1.0, block.matrix, linear, 1.0, vec)

    // in-place convert dotProducts to gradient scales
    // then, vec represents gradient scales
    var localLossSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      if (weight > 0) {
        // Our loss function with {0, 1} labels is max(0, 1 - (2y - 1) (f_w(x)))
        // Therefore the gradient is -(2y - 1)*x
        val label = block.getLabel(i)
        val labelScaled = label + label - 1.0
        val loss = (1.0 - labelScaled * vec(i)) * weight
        if (loss > 0) {
          localLossSum += loss
          val gradScale = -labelScaled * weight
          vec.values(i) = gradScale
        } else { vec.values(i) = 0.0 }
      } else { vec.values(i) = 0.0 }
      i += 1
    }
    lossSum += localLossSum
    weightSum += block.weightIter.sum

    // predictions are all correct, no gradient signal
    if (vec.values.forall(_ == 0)) return this

    block.matrix match {
      case dm: DenseMatrix =>
        BLAS.nativeBLAS.dgemv("N", dm.numCols, dm.numRows, 1.0, dm.values, dm.numCols,
          vec.values, 1, 1.0, gradientSumArray, 1)

      case sm: SparseMatrix if fitIntercept =>
        val linearGradSumVec = Vectors.zeros(numFeatures).toDense
        BLAS.gemv(1.0, sm.transpose, vec, 0.0, linearGradSumVec)
        BLAS.getBLAS(numFeatures).daxpy(numFeatures, 1.0, linearGradSumVec.values, 1,
          gradientSumArray, 1)

      case sm: SparseMatrix if !fitIntercept =>
        val gradSumVec = new DenseVector(gradientSumArray)
        BLAS.gemv(1.0, sm.transpose, vec, 1.0, gradSumVec)

      case m =>
        throw new IllegalArgumentException(s"Unknown matrix type ${m.getClass}.")
    }

    if (fitIntercept) gradientSumArray(numFeatures) += vec.values.sum

    this
  }
}
