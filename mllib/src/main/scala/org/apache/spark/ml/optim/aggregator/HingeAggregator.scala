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
 */
private[ml] class HingeAggregator(
    numFeatures: Int,
    fitIntercept: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock, HingeAggregator] {

  private val numFeaturesPlusIntercept: Int = if (fitIntercept) numFeatures + 1 else numFeatures
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
      val localCoefficients = coefficientsArray
      val localGradientSumArray = gradientSumArray

      val dotProduct = {
        var sum = 0.0
        features.foreachNonZero { (index, value) =>
          sum += localCoefficients(index) * value
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
          localGradientSumArray(index) += value * gradientScale
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

  /**
   * Add a new training instance to this HingeAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instanceBlock The InstanceBlock to be added.
   * @return This HingeAggregator object.
   */
  def add(instanceBlock: InstanceBlock): this.type = {
//    instanceBlock.instanceIterator.foreach(this.add)
    require(numFeatures == instanceBlock.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${instanceBlock.numFeatures}.")
    require(instanceBlock.weightIter.forall(_ >= 0),
      s"instance weights ${instanceBlock.weightIter.mkString("[", ",", "]")} has to be >= 0.0")

    if (instanceBlock.weightIter.forall(_ == 0)) return this
    val localCoefficients = coefficientsArray
    val localGradientSumArray = gradientSumArray
    val intercept = if (fitIntercept) localCoefficients(numFeatures) else 0.0

    var i = 0
    while (i < instanceBlock.size) {
      val weight = instanceBlock.getWeight(i)
      if (weight > 0) {
        var dot = intercept
        instanceBlock.getNonZeroIter(i).foreach { case (index, value) =>
          dot += localCoefficients(index) * value
        }

        // Our loss function with {0, 1} labels is max(0, 1 - (2y - 1) (f_w(x)))
        // Therefore the gradient is -(2y - 1)*x
        val label = instanceBlock.getLabel(i)
        val labelScaled = 2 * label - 1.0
        val loss = math.max((1.0 - labelScaled * dot) * weight, 0.0)

        if (loss != 0) {
          val gradientScale = -labelScaled * weight
          instanceBlock.getNonZeroIter(i).foreach { case (index, value) =>
            localGradientSumArray(index) += value * gradientScale
          }
          if (fitIntercept) {
            localGradientSumArray(numFeatures) += gradientScale
          }
          lossSum += loss
        }
        weightSum += weight
      }
      i += 1
    }

    this
  }
}
