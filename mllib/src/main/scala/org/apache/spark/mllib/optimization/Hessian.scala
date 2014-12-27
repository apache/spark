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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.BLAS.{dot, scal}

/**
 * :: DeveloperApi ::
 * Class used to compute the Hessian-vector product for a twice-differentiable loss function,
 * given a single data point.
 */
@DeveloperApi
abstract class HessianVector extends Serializable {
  def compute(data: Vector, label: Double, weights: Vector, direction: Vector): Vector
}

@DeveloperApi
class LogisticHessian extends HessianVector {
  override def compute(data: Vector, label: Double, weights: Vector, direction: Vector) = {
    val sigma = 1.0 / (1.0 + math.exp(- dot(weights, data) * (label * 2.0 - 1.0)))
    val hessianMultiplier = sigma * (1.0 - sigma) * dot(direction, data)
    val hv = data.copy
    scal(hessianMultiplier, hv)
    hv
  }
}
