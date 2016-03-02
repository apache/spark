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

package org.apache.spark.ml.ann

import java.util.Random

import breeze.linalg.{sum => Bsum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{log => Blog}

/**
 * Trait for loss function
 */
private[ann] trait LossFunction {
  /**
   * Loss function
   * @param output actual output
   * @param target target output
   * @param delta output delta to write to
   * @return
   */
  def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double
}

private[ann] class SigmoidLayerWithSquaredError extends Layer {
  override val weightSize = 0
  override def outputSize(inputSize: Int): Int = inputSize
  override val inPlace = true
  override def model(weights: BDV[Double]): LayerModel = new SigmoidLayerModelWithSquaredError()
  override def initModel(weights: BDV[Double], random: Random): LayerModel =
    new SigmoidLayerModelWithSquaredError()
}

private[ann] class SigmoidLayerModelWithSquaredError
  extends FunctionalLayerModel(new FunctionalLayer(new SigmoidFunction)) with LossFunction {
  override def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double = {
    UniversalFunction(output, target, delta, (o: Double, t: Double) => o - t)
    val error = Bsum(delta :* delta) / 2 / output.cols
    UniversalFunction(delta, output, delta, (x: Double, o: Double) => x * (o - o * o))
    error
  }
}

private[ann] class SoftmaxLayerWithCrossEntropyLoss extends Layer {
  override val weightSize = 0
  override def outputSize(inputSize: Int): Int = inputSize
  override val inPlace = true
  override def model(weights: BDV[Double]): LayerModel =
    new SoftmaxLayerModelWithCrossEntropyLoss()
  override def initModel(weights: BDV[Double], random: Random): LayerModel =
    new SoftmaxLayerModelWithCrossEntropyLoss()
}

private[ann] class SoftmaxLayerModelWithCrossEntropyLoss extends LayerModel with LossFunction {

  val weights = new BDV[Double](0)

  def inplaceEval(x: BDM[Double], y: BDM[Double]): Unit = {
    var j = 0
    // find max value to make sure later that exponent is computable
    while (j < x.cols) {
      var i = 0
      var max = Double.MinValue
      while (i < x.rows) {
        if (x(i, j) > max) {
          max = x(i, j)
        }
        i += 1
      }
      var sum = 0.0
      i = 0
      while (i < x.rows) {
        val res = Math.exp(x(i, j) - max)
        y(i, j) = res
        sum += res
        i += 1
      }
      i = 0
      while (i < x.rows) {
        y(i, j) /= sum
        i += 1
      }
      j += 1
    }
  }

  override def eval(data: BDM[Double], output: BDM[Double]): Unit = {
    inplaceEval(data, output)
  }
  override def prevDelta(nextDelta: BDM[Double], input: BDM[Double], delta: BDM[Double]): Unit = {}

  override def grad(delta: BDM[Double], input: BDM[Double], cumGrad: BDV[Double]): Unit = {}

  override def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double = {
    UniversalFunction(output, target, delta, (o: Double, t: Double) => o - t)
    -Bsum( target :* Blog(output)) / output.cols
  }
}
