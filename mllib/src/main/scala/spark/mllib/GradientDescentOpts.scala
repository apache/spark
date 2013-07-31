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

/**
 * Class used to configure options used for GradientDescent based optimization
 * algorithms.
 */

class GradientDescentOpts private (
  var stepSize: Double,
  var numIters: Int,
  var regParam: Double,
  var miniBatchFraction: Double) {

  def this() = this(1.0, 100, 0.0, 1.0)

  /**
   * Set the step size per-iteration of SGD. Default 1.0.
   */
  def setStepSize(step: Double) = {
    this.stepSize = step
    this
  }

  /**
   * Set fraction of data to be used for each SGD iteration. Default 1.0.
   */
  def setMiniBatchFraction(fraction: Double) = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int) = {
    this.numIters = iters
    this
  }

  /**
   * Set the regularization parameter used for SGD. Default 0.0.
   */
  def setRegParam(regParam: Double) = {
    this.regParam = regParam
    this
  }
}

object GradientDescentOpts {

  def apply(stepSize: Double, numIters: Int, regParam: Double, miniBatchFraction: Double) = {
    new GradientDescentOpts(stepSize, numIters, regParam, miniBatchFraction)
  }

  def apply() = {
    new GradientDescentOpts()
  }

}
