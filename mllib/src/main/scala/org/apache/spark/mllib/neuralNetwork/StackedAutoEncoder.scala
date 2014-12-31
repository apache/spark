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

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{DenseMatrix => SDM, SparseMatrix => SSM, Matrix => SM,
SparseVector => SSV, DenseVector => SDV, Vector => SV, Vectors, Matrices, BLAS}
import org.apache.spark.rdd.RDD

class StackedAutoEncoder(val stackedRBM: StackedRBM)
  extends Logging with Serializable {

  def this(topology: Array[Int]) {
    this(new StackedRBM(topology))
  }

  require(numInput == numOut)
  require(numLayer > 1)

  lazy val mlp: MLP = stackedRBM.toMLP

  def numInput = stackedRBM.numInput

  def numOut = stackedRBM.numOut

  def numLayer = stackedRBM.numLayer

  def forward(visible: SM): SM = {
    stackedRBM.forward(visible, stackedRBM.numLayer / 2 + 1)
  }
}

object StackedAutoEncoder extends Logging {
  def pretrain(
    data: RDD[SV],
    batchSize: Int,
    numIteration: Int,
    sae: StackedAutoEncoder,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): StackedAutoEncoder = {
    val stackedRBM = sae.stackedRBM
    val numLayer = stackedRBM.innerRBMs.length
    StackedRBM.train(data, batchSize, numIteration, stackedRBM,
      fraction, learningRate, weightCost, numLayer - 1)
    sae
  }

  def finetune(data: RDD[SV],
    batchSize: Int,
    numIteration: Int,
    sae: StackedAutoEncoder,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): StackedAutoEncoder = {
    MLP.train(data.map(t => (t, t)), batchSize, numIteration, sae.mlp,
      fraction, learningRate, weightCost)
    // MLP.runLBFGS(data.map(t => (t, t)), sae.mlp, batchSize, numIteration, 1e-4, 0.0001)
    sae
  }
}
