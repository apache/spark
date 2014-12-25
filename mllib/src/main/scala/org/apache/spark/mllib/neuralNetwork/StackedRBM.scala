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

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum => brzSum}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{DenseMatrix => SDM, SparseMatrix => SSM, Matrix => SM,
SparseVector => SSV, DenseVector => SDV, Vector => SV, Vectors, Matrices, BLAS}
import org.apache.spark.rdd.RDD

class StackedRBM(val innerRBMs: Array[RBM])
  extends Logging with Serializable {
  def this(topology: Array[Int]) {
    this(StackedRBM.initializeRBMs(topology))
  }

  def numLayer = innerRBMs.length

  def numInput = innerRBMs.head.numIn

  def numOut = innerRBMs.last.numOut

  def forward(visible: SM, toLayer: Int): SM = {
    var x = visible
    for (layer <- 0 until toLayer) {
      x = innerRBMs(layer).forward(x)
    }
    x
  }

  def forward(visible: SM): SM = {
    forward(visible, numLayer)
  }

  def topology: Array[Int] = {
    val topology = new Array[Int](numLayer + 1)
    topology(0) = numInput
    for (i <- 1 to numLayer) {
      topology(i) = innerRBMs(i - 1).numOut
    }
    topology
  }

  def toMLP(): MLP = {
    val layers = new Array[Layer](numLayer)
    for (layer <- 0 until numLayer) {
      layers(layer) = innerRBMs(layer).hiddenLayer
    }
    new MLP(layers, innerRBMs.map(_.dropoutRate))
  }
}

object StackedRBM extends Logging {
  def train(
    data: RDD[SV],
    batchSize: Int,
    numIteration: Int,
    topology: Array[Int],
    fraction: Double,
    learningRate: Double,
    weightCost: Double): StackedRBM = {
    train(data, batchSize, numIteration, new StackedRBM(topology),
      fraction, learningRate, weightCost)
  }

  def train(
    data: RDD[SV],
    batchSize: Int,
    numIteration: Int,
    stackedRBM: StackedRBM,
    fraction: Double,
    learningRate: Double,
    weightCost: Double,
    maxLayer: Int = -1): StackedRBM = {
    val trainLayer = if (maxLayer > -1D) {
      maxLayer
    } else {
      stackedRBM.numLayer
    }

    for (layer <- 0 until trainLayer) {
      logInfo(s"Train ($layer/$trainLayer)")
      val broadcast = data.context.broadcast(stackedRBM)
      val dataBatch = forward(data, broadcast, layer)
      val rbm = stackedRBM.innerRBMs(layer)
      RBM.train(dataBatch, batchSize, numIteration, rbm,
        fraction, learningRate, weightCost)
      // broadcast.destroy(blocking = false)
    }
    stackedRBM
  }

  private def forward(
    data: RDD[SV],
    broadcast: Broadcast[StackedRBM],
    toLayer: Int): RDD[SV] = {
    if (toLayer > 0) {
      data.mapPartitions { itr =>
        val stackedRBM = broadcast.value
        itr.map { data =>
          val input = new SDM(data.size, 1, data.toArray)
          val x = stackedRBM.forward(input, toLayer).toBreeze.toDenseMatrix
          Vectors.fromBreeze(x(::, 0))
        }
      }
    } else {
      data
    }
  }

  def initializeRBMs(topology: Array[Int]): Array[RBM] = {
    val numLayer = topology.length - 1
    val innerRBMs: Array[RBM] = new Array[RBM](numLayer)
    for (layer <- 0 until numLayer) {
      val dropout = if (layer == 0) {
        0.2
      } else if (layer < numLayer - 1) {
        0.5
      } else {
        0.0
      }
      innerRBMs(layer) = new RBM(topology(layer), topology(layer + 1), dropout)
      println(s"innerRBMs($layer) = ${innerRBMs(layer).numIn} * ${innerRBMs(layer).numOut}")
    }
    innerRBMs
  }
}
