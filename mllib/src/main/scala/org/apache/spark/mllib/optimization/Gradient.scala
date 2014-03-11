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

package org.apache.spark.mllib.optimization

import org.jblas.DoubleMatrix

/**
 * Class used to compute the gradient for a loss function, given a single data point.
 */
abstract class Gradient extends Serializable {
  /**
   * Compute the gradient and loss given the features of a single data point.
   *
   * @param data - Feature values for one data point. Column matrix of size dx1
   *               where d is the number of features.
   * @param label - Label for this data item.
   * @param weights - Column matrix containing weights for every feature.
   *
   * @return A tuple of 2 elements. The first element is a column matrix containing the computed
   *         gradient and the second element is the loss computed at this data point.
   *
   */
  def compute(data: DoubleMatrix, label: Double, weights: DoubleMatrix): 
      (DoubleMatrix, Double)
}

/**
 * Compute gradient and loss for a logistic loss function, as used in binary classification.
 * See also the documentation for the precise formulation.
 */
class LogisticGradient extends Gradient {
  override def compute(data: DoubleMatrix, label: Double, weights: DoubleMatrix): 
      (DoubleMatrix, Double) = {
    val margin: Double = -1.0 * data.dot(weights)
    val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label

    val gradient = data.mul(gradientMultiplier)
    val loss =
      if (label > 0) {
        math.log(1 + math.exp(margin))
      } else {
        math.log(1 + math.exp(margin)) - margin
      }

    (gradient, loss)
  }
}

/**
 * Compute gradient and loss for a Least-squared loss function, as used in linear regression.
 * This is correct for the averaged least squares loss function (mean squared error)
 *              L = 1/n ||A weights-y||^2
 * See also the documentation for the precise formulation.
 */
class LeastSquaresGradient extends Gradient {
  override def compute(data: DoubleMatrix, label: Double, weights: DoubleMatrix): 
      (DoubleMatrix, Double) = {
    val diff: Double = data.dot(weights) - label

    val loss = diff * diff
    val gradient =  data.mul(2.0 * diff)

    (gradient, loss)
  }
}

/**
 * Compute gradient and loss for a Hinge loss function, as used in SVM binary classification.
 * See also the documentation for the precise formulation.
 * NOTE: This assumes that the labels are {0,1}
 */
class HingeGradient extends Gradient {
  override def compute(data: DoubleMatrix, label: Double, weights: DoubleMatrix):
      (DoubleMatrix, Double) = {

    val dotProduct = data.dot(weights)

    // Our loss function with {0, 1} labels is max(0, 1 - (2y – 1) (f_w(x)))
    // Therefore the gradient is -(2y - 1)*x
    val labelScaled = 2 * label - 1.0

    if (1.0 > labelScaled * dotProduct) {
      (data.mul(-labelScaled), 1.0 - labelScaled * dotProduct)
    } else {
      (DoubleMatrix.zeros(1, weights.length), 0.0)
    }
  }
}
