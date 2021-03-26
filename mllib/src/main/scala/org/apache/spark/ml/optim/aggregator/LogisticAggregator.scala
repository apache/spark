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
import org.apache.spark.ml.impl.Utils
import org.apache.spark.ml.linalg._

/**
 * BlockLogisticAggregator computes the gradient and loss used in Logistic classification
 * for blocks in sparse or dense matrix in an online fashion.
 *
 * Two BlockLogisticAggregators can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to be standardized before computation.
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term.
 */
private[ml] class BlockLogisticAggregator(
    numFeatures: Int,
    numClasses: Int,
    fitIntercept: Boolean,
    multinomial: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock, BlockLogisticAggregator] with Logging {

  if (multinomial && numClasses <= 2) {
    logInfo(s"Multinomial logistic regression for binary classification yields separate " +
      s"coefficients for positive and negative classes. When no regularization is applied, the" +
      s"result will be effectively the same as binary logistic regression. When regularization" +
      s"is applied, multinomial loss will produce a result different from binary loss.")
  }

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

  @transient private lazy val coefficientsArray = bcCoefficients.value match {
    case DenseVector(values) => values
    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector but " +
      s"got type ${bcCoefficients.value.getClass}.)")
  }

  @transient private lazy val binaryLinear = (multinomial, fitIntercept) match {
    case (false, true) => Vectors.dense(coefficientsArray.take(numFeatures))
    case (false, false) => Vectors.dense(coefficientsArray)
    case _ => null
  }

  @transient private lazy val multinomialLinear = (multinomial, fitIntercept) match {
    case (true, true) =>
      Matrices.dense(numClasses, numFeatures,
        coefficientsArray.take(numClasses * numFeatures)).toDense
    case (true, false) =>
      Matrices.dense(numClasses, numFeatures, coefficientsArray).toDense
    case _ => null
  }

  /**
   * Add a new training instance block to this BlockLogisticAggregator, and update the loss and
   * gradient of the objective function.
   *
   * @param block The instance block of data point to be added.
   * @return This BlockLogisticAggregator object.
   */
  def add(block: InstanceBlock): this.type = {
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.weightIter.forall(_ >= 0),
      s"instance weights ${block.weightIter.mkString("[", ",", "]")} has to be >= 0.0")

    if (block.weightIter.forall(_ == 0)) return this

    if (multinomial) {
      multinomialUpdateInPlace(block)
    } else {
      binaryUpdateInPlace(block)
    }

    this
  }

  /** Update gradient and loss using binary loss function. */
  private def binaryUpdateInPlace(block: InstanceBlock): Unit = {
    val size = block.size

    // vec here represents margins or negative dotProducts
    val vec = if (fitIntercept) {
      Vectors.dense(Array.fill(size)(coefficientsArray.last)).toDense
    } else {
      Vectors.zeros(size).toDense
    }
    BLAS.gemv(-1.0, block.matrix, binaryLinear, -1.0, vec)

    // in-place convert margins to multiplier
    // then, vec represents multiplier
    var localLossSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      if (weight > 0) {
        val label = block.getLabel(i)
        val margin = vec(i)
        if (label > 0) {
          // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
          localLossSum += weight * Utils.log1pExp(margin)
        } else {
          localLossSum += weight * (Utils.log1pExp(margin) - margin)
        }
        val multiplier = weight * (1.0 / (1.0 + math.exp(margin)) - label)
        vec.values(i) = multiplier
      } else { vec.values(i) = 0.0 }
      i += 1
    }
    lossSum += localLossSum
    weightSum += block.weightIter.sum

    // predictions are all correct, no gradient signal
    if (vec.values.forall(_ == 0)) return

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
  }

  /** Update gradient and loss using multinomial (softmax) loss function. */
  private def multinomialUpdateInPlace(block: InstanceBlock): Unit = {
    val size = block.size

    // mat here represents margins, shape: S X C
    val mat = DenseMatrix.zeros(size, numClasses)
    if (fitIntercept) {
      val localCoefficientsArray = coefficientsArray
      val offset = numClasses * numFeatures
      var j = 0
      while (j < numClasses) {
        val intercept = localCoefficientsArray(offset + j)
        var i = 0
        while (i < size) { mat.update(i, j, intercept); i += 1 }
        j += 1
      }
    }
    BLAS.gemm(1.0, block.matrix, multinomialLinear.transpose, 1.0, mat)

    // in-place convert margins to multipliers
    // then, mat represents multipliers
    var localLossSum = 0.0
    var i = 0
    val tmp = Array.ofDim[Double](numClasses)
    val interceptGradSumArr = if (fitIntercept) Array.ofDim[Double](numClasses) else null
    while (i < size) {
      val weight = block.getWeight(i)
      if (weight > 0) {
        val label = block.getLabel(i)

        var maxMargin = Double.NegativeInfinity
        var j = 0
        while (j < numClasses) {
          tmp(j) = mat(i, j)
          maxMargin = math.max(maxMargin, tmp(j))
          j += 1
        }

        // marginOfLabel is margins(label) in the formula
        val marginOfLabel = tmp(label.toInt)

        var sum = 0.0
        j = 0
        while (j < numClasses) {
          if (maxMargin > 0) tmp(j) -= maxMargin
          val exp = math.exp(tmp(j))
          sum += exp
          tmp(j) = exp
          j += 1
        }

        j = 0
        while (j < numClasses) {
          val multiplier = weight * (tmp(j) / sum - (if (label == j) 1.0 else 0.0))
          mat.update(i, j, multiplier)
          if (fitIntercept) interceptGradSumArr(j) += multiplier
          j += 1
        }

        if (maxMargin > 0) {
          localLossSum += weight * (math.log(sum) - marginOfLabel + maxMargin)
        } else {
          localLossSum += weight * (math.log(sum) - marginOfLabel)
        }
      } else {
        var j = 0; while (j < numClasses) { mat.update(i, j, 0.0); j += 1 }
      }
      i += 1
    }
    lossSum += localLossSum
    weightSum += block.weightIter.sum

    // mat (multipliers):             S X C, dense                                N
    // mat.transpose (multipliers):   C X S, dense                                T
    // block.matrix:                  S X F, unknown type                         T
    // gradSumMat(gradientSumArray):  C X FPI (numFeaturesPlusIntercept), dense   N
    block.matrix match {
      case dm: DenseMatrix =>
        BLAS.nativeBLAS.dgemm("T", "T", numClasses, numFeatures, size, 1.0,
          mat.values, size, dm.values, numFeatures, 1.0, gradientSumArray, numClasses)

      case sm: SparseMatrix =>
        // linearGradSumMat = matrix.T X mat
        val linearGradSumMat = DenseMatrix.zeros(numFeatures, numClasses)
        BLAS.gemm(1.0, sm.transpose, mat, 0.0, linearGradSumMat)
        linearGradSumMat.foreachActive { (i, j, v) => gradientSumArray(i * numClasses + j) += v }
    }

    if (fitIntercept) {
      BLAS.getBLAS(numClasses).daxpy(numClasses, 1.0, interceptGradSumArr, 0, 1,
        gradientSumArray, numClasses * numFeatures, 1)
    }
  }
}
