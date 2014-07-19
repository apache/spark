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

import scala.collection.mutable.ListBuffer
import scala.math._

import breeze.linalg.{DenseVector => BDV, Vector => BV}

import org.apache.spark.mllib.linalg.{Vectors, Vector}

abstract class Regularizer extends Serializable {
  var isSmooth: Boolean = true

  def add(that: Regularizer): CompositeRegularizer = {
    (new CompositeRegularizer).add(this).add(that)
  }

  def compute(weights: Vector, cumGradient: Vector): Double
}

class SimpleRegularizer extends Regularizer {
    isSmooth = true

  override def compute(weights: Vector, cumGradient: Vector): Double = 0
}

class CompositeRegularizer extends Regularizer {
   isSmooth = true

  protected val regularizers = ListBuffer[Regularizer]()

  override def add(that: Regularizer): this.type = {
    if (this.isSmooth && !that.isSmooth) isSmooth = false
    regularizers.append(that)
    this
  }

  override def compute(weights: Vector, cumGradient: Vector): Double = {
    if (regularizers.isEmpty) {
      0.0
    } else {
      regularizers.foldLeft(0.0)((loss: Double, x: Regularizer) =>
        loss + x.compute(weights, cumGradient)
      )
    }
  }
}

class L1Regularizer(private val regParam: BV[Double]) extends Regularizer {
   isSmooth = false

  def this(regParam: Double) = this(new BDV[Double](Array[Double](regParam)))

  def this(regParam: Vector) = this(regParam.toBreeze)

  def compute(weights: Vector, cumGradient: Vector): Double = {
    val brzWeights = weights.toBreeze
    val brzCumGradient = cumGradient.toBreeze

    if (regParam.length > 1) require(brzWeights.length == regParam.length)

    if (regParam.length == 1 && regParam(0) == 0.0) {
      0.0
    }
    else {
      var loss: Double = 0.0
      brzWeights.activeIterator.foreach {
        case (_, 0.0) => // Skip explicit zero elements.
        case (i, value) => {
          val lambda = if (regParam.length > 1) regParam(i) else regParam(0)
          loss += lambda * Math.abs(value)
          brzCumGradient(i) += lambda * signum(value)
        }
      }
      loss
    }
  }
}

class L2Regularizer(private val regParam: BV[Double]) extends Regularizer {
   isSmooth = true

  def this(regParam: Double) = this(new BDV[Double](Array[Double](regParam)))

  def this(regParam: Vector) = this(regParam.toBreeze)

  def compute(weights: Vector, cumGradient: Vector): Double = {
    val brzWeights = weights.toBreeze
    val brzCumGradient = cumGradient.toBreeze

    if (regParam.length > 1) require(brzWeights.length == regParam.length)

    if (regParam.length == 1 && regParam(0) == 0) {
      0.0
    }
    else {
      var loss: Double = 0.0
      brzWeights.activeIterator.foreach {
        case (_, 0.0) => // Skip explicit zero elements.
        case (i, value) => {
          val lambda = if (regParam.length > 1) regParam(i) else regParam(0)
          loss += lambda * value * value / 2.0
          brzCumGradient(i) += lambda * value
        }
      }
      loss
    }
  }
}

class ElasticNetRegularizer(private val regParam: BV[Double], private val alpha: Double)
  extends CompositeRegularizer {

  def this(regParam: Double, alpha: Double) = this(new BDV[Double](Array[Double](regParam)), alpha)

  def this(regParam: Vector, alpha: Double) = this(regParam.toBreeze, alpha)

  if (alpha != 0.0) {
    this.add(new L2Regularizer(Vectors.fromBreeze(this.regParam * alpha)))
  }
  if (alpha != 1.0) {
    this.add(new L1Regularizer(Vectors.fromBreeze(this.regParam * (1.0 - alpha))))
  }
}