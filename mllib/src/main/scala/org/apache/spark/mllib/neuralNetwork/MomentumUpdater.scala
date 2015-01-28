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

package org.apache.spark.mllib.neuralNetwork

import org.apache.spark.mllib.linalg.{Vector => SV, DenseVector => SDV, Vectors, BLAS}
import org.apache.spark.mllib.optimization.Updater

class MomentumUpdater(val momentum: Double) extends Updater {

  assert(momentum > 0 && momentum < 1)

  @transient private var momentumSum: SDV = null

  protected def l2(
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): Double = {
    0D
  }

  override def compute(
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): (SV, Double) = {
    if (momentumSum == null) {
      momentumSum = new SDV(new Array[Double](weightsOld.size))
    }
    val reg = l2(weightsOld, gradient, stepSize, iter, regParam)
    if (momentum > 0) {
      BLAS.axpy(momentum, momentumSum, gradient)
      this.synchronized {
        BLAS.copy(gradient, momentumSum)
      }
    }
    BLAS.axpy(-stepSize, gradient, weightsOld)
    (weightsOld, reg)
  }

}
