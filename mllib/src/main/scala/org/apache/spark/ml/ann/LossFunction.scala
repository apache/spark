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
import breeze.numerics.{log => brzlog}

/**
 * Trait for loss function
 */
private[ann] trait LossFunction {
  /**
   * Returns the value of loss function.
   * Computes loss based on target and output.
   * Writes delta (error) to delta in place.
   * Delta is allocated based on the outputSize
   * of model implementation.
   *
   * @param output actual output
   * @param target target output
   * @param delta delta (updated in place)
   * @return loss
   */
  def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double
}

private[ann] class SigmoidLayerWithSquaredError extends Layer {
  override val weightSize = 0
  override val inPlace = true

  override def getOutputSize(inputSize: Int): Int = inputSize
  override def createModel(weights: BDV[Double]): LayerModel =
    new SigmoidLayerModelWithSquaredError()
  override def initModel(weights: BDV[Double], random: Random): LayerModel =
    new SigmoidLayerModelWithSquaredError()
}

private[ann] class SigmoidLayerModelWithSquaredError
  extends FunctionalLayerModel(new FunctionalLayer(new SigmoidFunction)) with LossFunction {
  override def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double = {
    ApplyInPlace(output, target, delta, (o: Double, t: Double) => o - t)
    val error = Bsum(delta :* delta) / 2 / output.cols
    ApplyInPlace(delta, output, delta, (x: Double, o: Double) => x * (o - o * o))
    error
  }
}

private[ann] class SoftmaxLayerWithCrossEntropyLoss extends Layer {
  override val weightSize = 0
  override val inPlace = true

  override def getOutputSize(inputSize: Int): Int = inputSize
  override def createModel(weights: BDV[Double]): LayerModel =
    new SoftmaxLayerModelWithCrossEntropyLoss()
  override def initModel(weights: BDV[Double], random: Random): LayerModel =
    new SoftmaxLayerModelWithCrossEntropyLoss()
}

private[ann] class SoftmaxLayerModelWithCrossEntropyLoss extends LayerModel with LossFunction {

  // loss layer models do not have weights
  val weights = new BDV[Double](0)

  override def eval(data: BDM[Double], output: BDM[Double]): Unit = {
    var j = 0
    // find max value to make sure later that exponent is computable
    while (j < data.cols) {
      var i = 0
      var max = Double.MinValue
      while (i < data.rows) {
        if (data(i, j) > max) {
          max = data(i, j)
        }
        i += 1
      }
      var sum = 0.0
      i = 0
      while (i < data.rows) {
        val res = math.exp(data(i, j) - max)
        output(i, j) = res
        sum += res
        i += 1
      }
      i = 0
      while (i < data.rows) {
        output(i, j) /= sum
        i += 1
      }
      j += 1
    }
  }
  override def computePrevDelta(
    nextDelta: BDM[Double],
    input: BDM[Double],
    delta: BDM[Double]): Unit = {
    /* loss layer model computes delta in loss function */
  }

  override def grad(delta: BDM[Double], input: BDM[Double], cumGrad: BDV[Double]): Unit = {
    /* loss layer model does not have weights */
  }

  override def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double = {
    ApplyInPlace(output, target, delta, (o: Double, t: Double) => o - t)
    -Bsum( target :* brzlog(output)) / output.cols
  }
}

private[ml] class EmptyLayerWithSquaredError extends Layer {
  override val weightSize = 0
  override def getOutputSize(inputSize: Int): Int = inputSize
  override val inPlace = true
  override def createModel(weights: BDV[Double]): LayerModel =
    new EmptyLayerModelWithSquaredError()
  override def initModel(weights: BDV[Double], random: Random): LayerModel =
    new EmptyLayerModelWithSquaredError()
}

private[ann] class EmptyLayerModelWithSquaredError extends LayerModel with LossFunction {

  val weights = new BDV[Double](0)

  override def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double = {
    ApplyInPlace(output, target, delta, (o: Double, t: Double) => o - t)
    Bsum(delta :* delta) / 2 / output.cols
  }

  override def eval(data: BDM[Double], output: BDM[Double]): Unit = {}
  override def computePrevDelta(
                                 nextDelta: BDM[Double],
                                 input: BDM[Double],
                                 delta: BDM[Double]): Unit = {}
  override def grad(delta: BDM[Double], input: BDM[Double], cumGrad: BDV[Double]): Unit = {}
}

private[ml] class SigmoidLayerWithCrossEntropyLoss extends Layer {
  override val weightSize = 0
  override def getOutputSize(inputSize: Int): Int = inputSize
  override val inPlace = true
  override def createModel(weights: BDV[Double]): LayerModel =
    new SigmoidLayerModelWithCrossEntropyLoss()
  override def initModel(weights: BDV[Double], random: Random): LayerModel =
    new SigmoidLayerModelWithCrossEntropyLoss()
}

private[ann] class SigmoidLayerModelWithCrossEntropyLoss
  extends FunctionalLayerModel(new FunctionalLayer(new SigmoidFunction)) with LossFunction {
  // TODO: make a common place where ones matrices reside
  private var oneMatrix: BDM[Double] = null
  private val epsilon = 1e-15
  private var epsilonMatrix: BDM[Double] = null

  override def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double = {
    if (oneMatrix == null || oneMatrix.cols != target.cols) {
      oneMatrix = BDM.ones[Double](target.rows, target.cols)
    }
    if (epsilonMatrix == null || epsilonMatrix.cols != target.cols) {
      epsilonMatrix = BDM.fill[Double](target.rows, target.cols)(epsilon)
    }
    ApplyInPlace(output, target, delta, (o: Double, t: Double) => o - t)
    // NB: operation :* don't have execution priority over summation
    // TODO: is adding epsilon a good way to fight log(o) ?
    -Bsum((target :* brzlog(output + epsilonMatrix)) +
      ((oneMatrix - target) :* brzlog(oneMatrix - output + epsilonMatrix))) / output.cols
  }
}

