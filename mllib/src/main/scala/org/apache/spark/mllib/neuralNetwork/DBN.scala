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

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Vector => SV}
import org.apache.spark.rdd.RDD

class DBN(val stackedRBM: StackedRBM, val nn: MLP)
  extends Logging with Serializable {
}

object DBN extends Logging {
  def train(
    data: RDD[(SV, SV)],
    batchSize: Int,
    numIteration: Int,
    topology: Array[Int],
    fraction: Double,
    weightCost: Double,
    learningRate: Double): DBN = {
    val dbn = initializeDBN(topology)
    pretrain(data, batchSize, numIteration, dbn,
      fraction, learningRate, weightCost)
    finetune(data, batchSize, numIteration, dbn,
      fraction, weightCost, learningRate)
    dbn
  }

  def pretrain(
    data: RDD[(SV, SV)],
    batchSize: Int,
    numIteration: Int,
    dbn: DBN,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): DBN = {
    val stackedRBM = dbn.stackedRBM
    val numLayer = stackedRBM.innerRBMs.length
    StackedRBM.train(data.map(_._1), batchSize, numIteration, stackedRBM,
      fraction, learningRate, weightCost, numLayer - 1)
    dbn
  }

  def finetune(data: RDD[(SV, SV)],
    batchSize: Int,
    numIteration: Int,
    dbn: DBN,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): DBN = {
    MLP.train(data, batchSize, numIteration, dbn.nn,
      fraction, learningRate, weightCost)
    dbn
  }

  def initializeDBN(topology: Array[Int]): DBN = {
    val numLayer = topology.length - 1
    val stackedRBM = new StackedRBM(topology)
    val lastRBM = stackedRBM.innerRBMs.last
    Layer.initUniformDistWeight(lastRBM.weight, 0)
    val innerLayers = new Array[Layer](numLayer)
    stackedRBM.innerRBMs.zipWithIndex.foreach { case (rbm, index) =>
      innerLayers(index) = rbm.hiddenLayer
    }
    val mlp = new MLP(innerLayers)
    new DBN(stackedRBM, mlp)
  }
}
