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

package spark.mllib.optimization

import org.jblas.DoubleMatrix

abstract class Gradient extends Serializable {
  /**
   * Compute the gradient for a given row of data.
   *
   * @param data - One row of data. Row matrix of size 1xn where n is the number of features.
   * @param label - Label for this data item.
   * @param weights - Column matrix containing weights for every feature.
   */
  def compute(data: DoubleMatrix, label: Double, weights: DoubleMatrix): 
      (DoubleMatrix, Double)
}

class LogisticGradient extends Gradient {
  override def compute(data: DoubleMatrix, label: Double, weights: DoubleMatrix): 
      (DoubleMatrix, Double) = {
    val margin: Double = -1.0 * data.dot(weights)
    val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label

    val gradient = data.mul(gradientMultiplier)
    val loss =
      if (margin > 0) {
        math.log(1 + math.exp(0 - margin))
      } else {
        math.log(1 + math.exp(margin)) - margin
      }

    (gradient, loss)
  }
}


class SquaredGradient extends Gradient {
  override def compute(data: DoubleMatrix, label: Double, weights: DoubleMatrix): 
      (DoubleMatrix, Double) = {
    val diff: Double = data.dot(weights) - label

    val loss = 0.5 * diff * diff
    val gradient = data.mul(diff)

    (gradient, loss)
  }
}


class HingeGradient extends Gradient {
  override def compute(data: DoubleMatrix, label: Double, weights: DoubleMatrix): 
      (DoubleMatrix, Double) = {

    val dotProduct = data.dot(weights)

    if (1.0 > label * dotProduct)
      (data.mul(-label), 1.0 - label * dotProduct)
    else
      (DoubleMatrix.zeros(1,weights.length), 0.0)
  }
}
