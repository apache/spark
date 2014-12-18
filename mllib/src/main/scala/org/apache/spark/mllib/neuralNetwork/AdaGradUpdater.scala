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

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vector => SV, DenseVector => SDV, Vectors, BLAS}
import org.apache.spark.mllib.optimization.Updater

@Experimental
class AdaGradUpdater(
  val rho: Double,
  val epsilon: Double,
  val gamma: Double,
  val momentum: Double) extends Updater {
  require(rho >= 0 && rho < 1)
  require(momentum >= 0 && momentum < 1)
  @transient private var etaSum: SDV = null
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
    if (momentum > 0 && momentumSum == null) {
      momentumSum = new SDV(new Array[Double](weightsOld.size))
    }
    if (etaSum == null) {
      etaSum = new SDV(new Array[Double](weightsOld.size))
    }
    val reg = l2(weightsOld, gradient, stepSize, iter, regParam)
    if (momentum > 0) {
      BLAS.axpy(momentum, momentumSum, gradient)
      this.synchronized {
        BLAS.copy(gradient, momentumSum)
      }
    }

    val grad = gradient.toBreeze
    val g2 = grad :* grad
    this.synchronized {
      BLAS.axpy(if (rho > 0D && rho < 1D) rho else 1D, Vectors.fromBreeze(g2), etaSum)
    }

    for (i <- 0 until grad.length) {
      grad(i) *= gamma / (epsilon + math.sqrt(etaSum(i)))
    }
    BLAS.axpy(-stepSize, Vectors.fromBreeze(grad), weightsOld)
    (weightsOld, reg)
  }
}
