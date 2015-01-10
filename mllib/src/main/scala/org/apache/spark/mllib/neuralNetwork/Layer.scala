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

import java.util.Random

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, Axis => BrzAxis, sum => brzSum}

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{DenseMatrix => SDM, Matrix => SM,
Vector => SV, Vectors, Matrices, BLAS}

import NNUtil._

private[mllib] trait Layer extends Serializable {

  def weight: SM

  def bias: SV

  def numIn = weight.numCols

  def numOut = weight.numRows

  def layerType: String

  protected lazy val rand: Random = new Random()

  def setSeed(seed: Long): Unit = {
    rand.setSeed(seed)
  }

  def forward(input: SM): SM = {
    val batchSize = input.numCols
    val output = SDM.zeros(numOut, batchSize)
    BLAS.gemm(1.0, weight, new SDM(input.numRows, input.numCols, input.toArray), 1.0, output)
    val brzOutput = new BDM(numOut, batchSize, output.values)
    val brzBias = bias.toBreeze
    for (i <- 0 until batchSize) {
      brzOutput(::, i) :+= brzBias
    }
    computeNeuron(output)
    output
  }

  def backward(input: SM, delta: SM): (SM, SV) = {
    val gradWeight = SDM.zeros(numOut, numIn)
    BLAS.gemm(false, true, 1.0, delta,
      new SDM(input.numRows, input.numCols, input.toArray), 1.0, gradWeight)

    val brzDelta = new BDM(delta.numRows, delta.numCols, delta.toArray)
    val gradBias = brzSum(brzDelta, BrzAxis._1)

    (gradWeight, Vectors.fromBreeze(gradBias))
  }

  def outputError(output: SM, label: SM): SM = {
    val delta = Matrices.fromBreeze(output.toBreeze - label.toBreeze)
    computeNeuronPrimitive(delta, output)
    delta
  }

  def previousError(input: SM, previousLayer: Layer, currentDelta: SM): SM = {
    val preDelta = weight.transposeMultiply(
      new SDM(currentDelta.numRows, currentDelta.numCols, currentDelta.toArray))
    previousLayer.computeNeuronPrimitive(preDelta, input)
    preDelta
  }

  def computeNeuron(temp: SM): Unit

  def computeNeuronPrimitive(temp: SM, output: SM): Unit

  protected[mllib] def sample(out: SM): SM = out
}

private[mllib] class SigmoidLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, 4D * math.sqrt(6D / (numIn + numOut))),
      initializeBias(numOut))
  }

  override def layerType: String = "Sigmoid"

  override def computeNeuron(temp: SM): Unit = {
    for (i <- 0 until temp.numRows) {
      for (j <- 0 until temp.numCols) {
        temp(i, j) = sigmoid(temp(i, j))
      }
    }
  }

  override def computeNeuronPrimitive(
    temp: SM,
    output: SM): Unit = {
    for (i <- 0 until temp.numRows) {
      for (j <- 0 until temp.numCols) {
        temp(i, j) = temp(i, j) * sigmoidPrimitive(output(i, j))
      }
    }
  }

  protected[mllib] override def sample(input: SM): SM = {
    input.map(v => if (rand.nextDouble() < v) 1D else 0D)
  }
}

private[mllib] class TanhLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, math.sqrt(6D / (numIn + numOut))),
      initializeBias(numOut))
  }

  override def layerType: String = "Tanh"

  override def computeNeuron(temp: SM): Unit = {
    for (i <- 0 until temp.numRows) {
      for (y <- 0 until temp.numCols) {
        temp(i, y) = tanh(temp(i, y))
      }
    }
  }

  def computeNeuronPrimitive(
    temp: SM,
    output: SM): Unit = {
    for (i <- 0 until temp.numRows) {
      for (y <- 0 until temp.numCols) {
        temp(i, y) = temp(i, y) * tanhPrimitive(output(i, y))
      }
    }
  }

  protected[mllib] override def sample(input: SM): SM = {
    input.map(v => if (rand.nextDouble() < v) 1D else 0D)
  }
}

private[mllib] class SoftMaxLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initializeWeight(numIn, numOut), initializeBias(numOut))
  }

  override def layerType: String = "SoftMax"

  override def computeNeuron(temp: SM): Unit = {
    val brzTemp = temp.toBreeze.asInstanceOf[BDM[Double]]
    for (col <- 0 until brzTemp.cols) {
      softMax(brzTemp(::, col))
    }
  }

  def softMax(temp: BDV[Double]): Unit = {
    var sum = 0D
    for (i <- 0 until temp.length) {
      temp(i) = Math.exp(temp(i))
      sum += temp(i)
    }
    temp :/= sum
  }

  override def computeNeuronPrimitive(
    temp: SM,
    output: SM): Unit = {
    // See: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.49.6403

    //  for (i <- 0 until temp.numRows) {
    //    for (j <- 0 until temp.numCols) {
    //      temp(i, j) = temp(i, j) * softMaxPrimitive(output(i, j))
    //    }
    //  }
  }

  override protected[mllib] def sample(out: SM): SM = {
    val brzOut = out.toBreeze.asInstanceOf[BDM[Double]]
    for (j <- 0 until brzOut.cols) {
      val v = brzOut(::, j)
      var sum = 0D
      var index = 0
      var find = false
      val s = rand.nextDouble()
      while (!find && index < v.length) {
        sum += v(index)
        if (sum >= s) {
          find = true
        } else {
          index += 1
        }
      }
      v :*= 0D
      index = if (find) index else index - 1
      v(index) = 1
    }
    out
  }
}

private[mllib] class NoisyReLULayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {
  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, 0D, 0.01),
      initializeBias(numOut))
  }

  override def layerType: String = "NoisyReLU"

  private def nReLu(tmp: SM): Unit = {
    for (i <- 0 until tmp.numRows) {
      for (j <- 0 until tmp.numCols) {
        val v = tmp(i, j)
        val sd = sigmoid(v)
        val x = v + sd * rand.nextGaussian()
        tmp(i, j) = math.max(0, x)
      }
    }
  }

  override def computeNeuron(temp: SM): Unit = {
    nReLu(temp)
  }

  override def computeNeuronPrimitive(
    temp: SM,
    output: SM): Unit = {
    for (i <- 0 until temp.numRows) {
      for (j <- 0 until temp.numCols)
        if (output(i, j) <= 0) {
          temp(i, j) = 0
        }
    }
  }
}

private[mllib] class ReLuLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, 0.0, 0.01),
      initializeBias(numOut))
  }

  override def layerType: String = "ReLu"

  private def relu(tmp: SM): Unit = {
    for (i <- 0 until tmp.numRows) {
      for (j <- 0 until tmp.numCols) {
        tmp(i, j) = math.max(0, tmp(i, j))
      }
    }
  }

  override def computeNeuron(temp: SM): Unit = {
    relu(temp)
  }

  override def computeNeuronPrimitive(temp: SM, output: SM): Unit = {
    for (i <- 0 until temp.numRows) {
      for (j <- 0 until temp.numCols)
        if (output(i, j) <= 0) {
          temp(i, j) = 0
        }
    }
  }

  override protected[mllib] def sample(input: SM): SM = {
    input.map { v =>
      val sd = sigmoid(v, 32)
      val x = v + sd * rand.nextGaussian()
      math.max(0, x)
    }
  }
}

private[mllib] class SoftPlusLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {
  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, 0D, 0.01),
      initializeBias(numOut))
  }

  override def layerType: String = "SoftPlus"

  override def computeNeuron(temp: SM): Unit = {
    for (i <- 0 until temp.numRows) {
      for (j <- 0 until temp.numCols) {
        temp(i, j) = softplus(temp(i, j))
      }
    }
  }

  override def computeNeuronPrimitive(temp: SM, output: SM): Unit = {
    for (i <- 0 until temp.numRows) {
      for (j <- 0 until temp.numCols) {
        temp(i, j) *= softplusPrimitive(output(i, j))
      }
    }
  }

  override protected[mllib] def sample(input: SM): SM = {
    input.map { v =>
      val sd = sigmoid(v)
      val x = v + sd * rand.nextGaussian()
      // val rng = new NormalDistribution(rand, 0, sd + 1e-23, 1e-9)
      // val x = v + rng.sample()
      math.max(0, x)
    }
  }
}

private[mllib] class Identity(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, 0D, 0.01),
      initializeBias(numOut))
  }

  override def layerType: String = "Identity"

  override def computeNeuron(tmp: SM): Unit = {}

  override def computeNeuronPrimitive(temp: SM, output: SM): Unit = {}

  override protected[mllib] def sample(input: SM): SM = {
    input.map(v => v + rand.nextGaussian())
  }
}

private[mllib] object Layer {

  def initializeLayer(weight: SM, bias: SV, layerType: String): Layer = {
    layerType match {
      case "SoftPlus" =>
        new SoftPlusLayer(weight, bias)
      case "ReLu" =>
        new ReLuLayer(weight, bias)
      case "NoisyReLU" =>
        new NoisyReLULayer(weight, bias)
      case "SoftMax" =>
        new SoftMaxLayer(weight, bias)
      case "Tanh" =>
        new TanhLayer(weight, bias)
      case "Sigmoid" =>
        new SigmoidLayer(weight, bias)
      case "Identity" =>
        new Identity(weight, bias)
      case _ =>
        throw new IllegalArgumentException("layerType is not correct")
    }
  }
}
