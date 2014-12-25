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

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM,
max => brzMax, Axis => BrzAxis, sum => brzSum}

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{DenseMatrix => SDM, SparseMatrix => SSM, Matrix => SM,
SparseVector => SSV, DenseVector => SDV, Vector => SV, Vectors, Matrices, BLAS}
import org.apache.spark.util.Utils

import Layer._

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
    require(input.numRows == numIn)
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

  def computeDeltaTop(output: SM, label: SM): SM = {
    val delta = Matrices.fromBreeze(output.toBreeze - label.toBreeze)
    computeNeuronPrimitive(delta, output)
    delta
  }

  def computeDeltaMiddle(output: SM, nextLayer: Layer, nextDelta: SM): SM = {
    val batchSize = output.numCols
    val delta = SDM.zeros(numOut, batchSize)
    BLAS.gemm(true, false, 1.0, nextLayer.weight,
      new SDM(nextDelta.numRows, nextDelta.numCols, nextDelta.toArray), 1.0, delta)
    computeNeuronPrimitive(delta, output)
    delta
  }

  def computeNeuron(temp: SM): Unit

  def computeNeuronPrimitive(temp: SM, output: SM): Unit

  protected[neuralNetwork] def sample(out: SM): SM = out
}

private[mllib] class SigmoidLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, 4D * math.sqrt(6D / (numIn + numOut))),
      initializeBias(numOut))
  }

  override def layerType: String = "sigmoid"

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

  protected[neuralNetwork] override def sample(input: SM): SM = {
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

  override def layerType: String = "tanh"

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

  protected[neuralNetwork] override def sample(input: SM): SM = {
    input.map(v => if (rand.nextDouble() < v) 1D else 0D)
  }
}

private[mllib] class SoftMaxLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initializeWeight(numIn, numOut), initializeBias(numOut))
  }

  override def layerType: String = "softMax"

  override def computeNeuron(temp: SM): Unit = {
    val brzTemp = temp.toBreeze.asInstanceOf[BDM[Double]]
    for (col <- 0 until brzTemp.cols) {
      softMax(brzTemp(::, col))
    }
  }

  def softMax(temp: BDV[Double]): Unit = {
    val max = brzMax(temp)
    var sum = 0D
    for (i <- 0 until temp.length) {
      temp(i) = Math.exp(temp(i) - max)
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

  override protected[neuralNetwork] def sample(out: SM): SM = {
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

private[mllib] class NReLuLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {
  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, 0D, 0.01),
      initializeBias(numOut))
  }

  override def layerType: String = "nrelu"

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

  override def layerType: String = "relu"

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

  override protected[neuralNetwork] def sample(input: SM): SM = {
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

  override def layerType: String = "softplus"

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

  override protected[neuralNetwork] def sample(input: SM): SM = {
    input.map { v =>
      val sd = sigmoid(v)
      val x = v + sd * rand.nextGaussian()
      // val rng = new NormalDistribution(rand, 0, sd + 1e-23, 1e-9)
      // val x = v + rng.sample()
      math.max(0, x)
    }
  }
}

private[mllib] class GaussianLayer(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initGaussianDistWeight(numIn, numOut), initializeBias(numOut))
  }

  override def layerType: String = "gaussian"

  override def computeNeuron(tmp: SM): Unit = {
    for (i <- 0 until tmp.numRows) {
      for (j <- 0 until tmp.numCols) {
        val x = tmp(i, j)
        tmp(i, j) = 1 / Math.sqrt(2 * Math.PI) * Math.exp(-x * x / 2)
      }
    }
  }

  override def computeNeuronPrimitive(temp: SM, output: SM): Unit = {
    for (i <- 0 until output.numRows) {
      for (j <- 0 until output.numCols) {
        val x = output(i, j)
        temp(i, j) *= -x * Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI)
      }
    }
  }

  override protected[neuralNetwork] def sample(input: SM): SM = {
    input.map(v => v + rand.nextGaussian())
  }
}

private[mllib] class Identity(
  val weight: SM,
  val bias: SV) extends Layer with Logging {

  def this(numIn: Int, numOut: Int) {
    this(initUniformDistWeight(numIn, numOut, 0D, 0.01),
      initializeBias(numOut))
  }

  override def layerType: String = "identity"

  override def computeNeuron(tmp: SM): Unit = {}

  override def computeNeuronPrimitive(temp: SM, output: SM): Unit = {}

  override protected[neuralNetwork] def sample(input: SM): SM = {
    input.map(v => v + rand.nextGaussian())
  }
}

private[mllib] object Layer {

  def initializeLayer(
    weight: SM,
    bias: SV,
    layerType: String): Layer = {
    layerType match {
      case "gaussian" =>
        new GaussianLayer(weight, bias)
      case "softplus" =>
        new SoftPlusLayer(weight, bias)
      case "relu" =>
        new ReLuLayer(weight, bias)
      case "nrelu" =>
        new NReLuLayer(weight, bias)
      case "softMax" =>
        new SoftMaxLayer(weight, bias)
      case "tanh" =>
        new TanhLayer(weight, bias)
      case "sigmoid" =>
        new SigmoidLayer(weight, bias)
      case "identity" =>
        new Identity(weight, bias)
      case _ =>
        throw new IllegalArgumentException("layerType is not correct")
    }
  }

  def initializeBias(numOut: Int): SV = {
    new SDV(new Array[Double](numOut))
  }

  def initializeWeight(numIn: Int, numOut: Int): SM = {
    SDM.zeros(numOut, numIn)
  }

  def initializeWeight(numIn: Int, numOut: Int, rand: () => Double): SM = {
    val weight = initializeWeight(numIn, numOut)
    initializeWeight(weight, rand)
  }

  def initializeWeight(w: SM, rand: () => Double): SM = {
    for (i <- 0 until w.numRows) {
      for (j <- 0 until w.numCols) {
        w(i, j) = rand()
      }
    }
    w
  }

  def initUniformDistWeight(numIn: Int, numOut: Int): SM = {
    initUniformDistWeight(initializeWeight(numIn, numOut), 0.0)
  }

  def initUniformDistWeight(numIn: Int, numOut: Int, scale: Double): SM = {
    initUniformDistWeight(initializeWeight(numIn, numOut), scale)
  }

  def initUniformDistWeight(w: SM, scale: Double): SM = {
    val numIn = w.numCols
    val numOut = w.numRows
    val s = if (scale <= 0) math.sqrt(6D / (numIn + numOut)) else scale
    initUniformDistWeight(w, -s, s)
  }

  def initUniformDistWeight(numIn: Int, numOut: Int, low: Double, high: Double): SM = {
    initUniformDistWeight(initializeWeight(numIn, numOut), low, high)
  }

  def initUniformDistWeight(w: SM, low: Double, high: Double): SM = {
    initializeWeight(w, () => Utils.random.nextDouble() * (high - low) + low)
  }

  def initGaussianDistWeight(numIn: Int, numOut: Int): SM = {
    initGaussianDistWeight(initializeWeight(numIn, numOut), 0.0)
  }

  def initGaussianDistWeight(numIn: Int, numOut: Int, scale: Double): SM = {
    initGaussianDistWeight(initializeWeight(numIn, numOut), scale)
  }

  def initGaussianDistWeight(weight: SM, scale: Double): SM = {
    val sd = if (scale <= 0) 0.01 else scale
    initializeWeight(weight, () => Utils.random.nextGaussian() * sd)
  }

  @inline def softplus(x: Double, expThreshold: Double = 64): Double = {
    if (x > expThreshold) {
      x
    }
    else if (x < -expThreshold) {
      0
    } else {
      math.log1p(math.exp(x))
    }
  }

  @inline def softplusPrimitive(y: Double, expThreshold: Double = 64): Double = {
    if (y > expThreshold) {
      1
    } else {
      val z = math.exp(y)
      (z - 1) / z
    }

  }

  @inline def tanh(x: Double): Double = {
    val a = math.pow(math.exp(x), 2)
    (a - 1) / (a + 1)
  }

  @inline def tanhPrimitive(y: Double): Double = {
    1 - math.pow(y, 2)
  }

  @inline def sigmoid(x: Double): Double = {
    1d / (1d + math.exp(-x))
  }

  @inline def sigmoid(x: Double, expThreshold: Double): Double = {
    if (x > expThreshold) {
      1D
    } else if (x < -expThreshold) {
      0D
    } else {
      sigmoid(x)
    }
  }

  @inline def sigmoidPrimitive(y: Double): Double = {
    y * (1 - y)
  }

  @inline def softMaxPrimitive(y: Double): Double = {
    y * (1 - y)
  }

  def scalarExp(x: Double, expThreshold: Double = 64D) = {
    if (x < -expThreshold) {
      math.exp(-expThreshold)
    } else if (x > expThreshold) {
      math.exp(-expThreshold)
    }
    else {
      math.exp(x)
    }
  }

  def meanSquaredError(out: SM, label: SM): Double = {
    require(label.numRows == out.numRows)
    require(label.numCols == out.numCols)
    var diff = 0D
    for (i <- 0 until out.numRows) {
      for (j <- 0 until out.numCols) {
        diff += math.pow(label(i, j) - out(i, j), 2)
      }
    }
    diff / out.numRows
  }

  def crossEntropy(out: SM, label: SM): Double = {
    require(label.numRows == out.numRows)
    require(label.numCols == out.numCols)
    var cost = 0D
    for (i <- 0 until out.numRows) {
      for (j <- 0 until out.numCols) {
        val a = label(i, j)
        var b = out(i, j)
        if (b == 0) {
          b += 1e-15
        } else if (b == 1D) {
          b -= 1e-15
        }
        cost -= a * math.log(b) + (1 - a) * math.log1p(1 - b)
      }
    }
    cost / out.numRows
  }
}
