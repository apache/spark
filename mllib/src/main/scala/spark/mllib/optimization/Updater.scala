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

abstract class Updater extends Serializable {
  /**
   * Compute an updated value for weights given the gradient, stepSize and iteration number.
   *
   * @param weightsOlds - Column matrix of size nx1 where n is the number of features.
   * @param gradient - Column matrix of size nx1 where n is the number of features.
   * @param stepSize - step size across iterations
   * @param iter - Iteration number
   *
   * @return A tuple of 2 elements. The first element is a column matrix containing updated weights,
   *         and the second element is the regularization value.
   */
  def compute(weightsOlds: DoubleMatrix, gradient: DoubleMatrix, stepSize: Double, iter: Int):
      (DoubleMatrix, Double)
}

class SimpleUpdater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix,
      stepSize: Double, iter: Int): (DoubleMatrix, Double) = {
    val normGradient = gradient.mul(stepSize / math.sqrt(iter))
    (weightsOld.sub(normGradient), 0)
  }
}
