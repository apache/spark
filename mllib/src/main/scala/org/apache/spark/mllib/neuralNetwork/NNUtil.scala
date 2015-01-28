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

object NNUtil {
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
