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

import scala.math._
import org.jblas.DoubleMatrix

abstract class Updater extends Serializable {
  /**
   * Compute an updated value for weights given the gradient, stepSize and iteration number.
   *
   * @param weightsOlds - Column matrix of size nx1 where n is the number of features.
   * @param gradient - Column matrix of size nx1 where n is the number of features.
   * @param stepSize - step size across iterations
   * @param iter - Iteration number
   * @param regParam - Regularization parameter
   *
   * @return A tuple of 2 elements. The first element is a column matrix containing updated weights,
   *         and the second element is the regularization value.
   */
  def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix, stepSize: Double, iter: Int, regParam: Double):
      (DoubleMatrix, Double)
}

class SimpleUpdater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix,
      stepSize: Double, iter: Int, regParam: Double): (DoubleMatrix, Double) = {
    val normGradient = gradient.mul(stepSize / math.sqrt(iter))
    (weightsOld.sub(normGradient), 0)
  }
}

/**
* L1 regularization -- corresponding proximal operator is the soft-thresholding function
* That is, each weight component is shrunk towards 0 by shrinkageVal
* If w >  shrinkageVal, set weight component to w-shrinkageVal.
* If w < -shrinkageVal, set weight component to w+shrinkageVal.
* If -shrinkageVal < w < shrinkageVal, set weight component to 0.
* Equivalently, set weight component to signum(w) * max(0.0, abs(w) - shrinkageVal)
**/
class L1Updater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix,
      stepSize: Double, iter: Int, regParam: Double): (DoubleMatrix, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val normGradient = gradient.mul(thisIterStepSize)
    // Take gradient step
    val newWeights = weightsOld.sub(normGradient)
    // Soft thresholding
    val shrinkageVal = regParam * thisIterStepSize
    (0 until newWeights.length).foreach(i => {
      val wi = newWeights.get(i)
      newWeights.put(i, signum(wi) * max(0.0, abs(wi) - shrinkageVal))
      })
    (newWeights, newWeights.norm1 * regParam)
  }
}

class SquaredL2Updater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix,
      stepSize: Double, iter: Int, regParam: Double): (DoubleMatrix, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val normGradient = gradient.mul(thisIterStepSize)
    val newWeights = weightsOld.sub(normGradient).div(2.0 * thisIterStepSize * regParam + 1.0)
    (newWeights, pow(newWeights.norm2,2.0) * regParam)
  }
}

