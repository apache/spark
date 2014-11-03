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

package org.apache.spark.mllib.ann

import breeze.linalg.{DenseVector, Vector => BV, axpy => brzAxpy}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

/*
 * Implements a Artificial Neural Network (ANN)
 *
 * The data consists of an input vector and an output vector, combined into a single vector
 * as follows:
 *
 * [ ---input--- ---output--- ]
 *
 * NOTE: output values should be in the range [0,1]
 *
 * For a network of H hidden layers:
 *
 * hiddenLayersTopology(h) indicates the number of nodes in hidden layer h, excluding the bias
 * node. h counts from 0 (first hidden layer, taking inputs from input layer) to H - 1 (last
 * hidden layer, sending outputs to the output layer).
 *
 * hiddenLayersTopology is converted internally to topology, which adds the number of nodes
 * in the input and output layers.
 *
 * noInput = topology(0), the number of input nodes
 * noOutput = topology(L-1), the number of output nodes
 *
 * input = data( 0 to noInput-1 )
 * output = data( noInput to noInput + noOutput - 1 )
 *
 * W_ijl is the weight from node i in layer l-1 to node j in layer l
 * W_ijl goes to position ofsWeight(l) + j*(topology(l-1)+1) + i in the weights vector
 *
 * B_jl is the bias input of node j in layer l
 * B_jl goes to position ofsWeight(l) + j*(topology(l-1)+1) + topology(l-1) in the weights vector
 *
 * error function: E( O, Y ) = sum( O_j - Y_j )
 * (with O = (O_0, ..., O_(noOutput-1)) the output of the ANN,
 * and (Y_0, ..., Y_(noOutput-1)) the input)
 *
 * node_jl is node j in layer l
 * node_jl goes to position ofsNode(l) + j
 *
 * The weights gradient is defined as dE/dW_ijl and dE/dB_jl
 * It has same mapping as W_ijl and B_jl
 *
 * For back propagation:
 * delta_jl = dE/dS_jl, where S_jl the output of node_jl, but before applying the sigmoid
 * delta_jl has the same mapping as node_jl
 *
 * Where E = ((estOutput-output),(estOutput-output)),
 * the inner product of the difference between estimation and target output with itself.
 *
 */

/**
 * Artificial neural network (ANN) model
 *
 * @param weights the weights between the neurons in the ANN.
 * @param topology array containing the number of nodes per layer in the network, including
 * the nodes in the input and output layer, but excluding the bias nodes.
 */
class ArtificialNeuralNetworkModel private[mllib](val weights: Vector, val topology: Array[Int])
  extends Serializable with ANNHelper {

  /**
   * Predicts values for a single data point using the trained model.
   *
   * @param testData represents a single data point.
   * @return prediction using the trained model.
   */
  def predict(testData: Vector): Vector = {
    Vectors.dense(computeValues(testData.toArray, weights.toArray))
  }

  /**
   * Predict values for an RDD of data points using the trained model.
   *
   * @param testDataRDD RDD representing the input vectors.
   * @return RDD with predictions using the trained model as (input, output) pairs.
   */
  def predict(testDataRDD: RDD[Vector]): RDD[(Vector,Vector)] = {
    testDataRDD.map(T => (T, predict(T)) )
  }

  private def computeValues(arrData: Array[Double], arrWeights: Array[Double]): Array[Double] = {
    val arrNodes = forwardRun(arrData, arrWeights)
    arrNodes.slice(arrNodes.size - topology(L), arrNodes.size)
  }
}

/**
 * Performs the training of an Artificial Neural Network (ANN)
 *
 * @param topology A vector containing the number of nodes per layer in the network, including
 * the nodes in the input and output layer, but excluding the bias nodes.
 * @param maxNumIterations The maximum number of iterations for the training phase.
 * @param convergenceTol Convergence tolerance for LBFGS. Smaller value for closer convergence.
 */
class ArtificialNeuralNetwork private[mllib](
    topology: Array[Int],
    maxNumIterations: Int,
    convergenceTol: Double)
  extends Serializable {

  private val gradient = new ANNLeastSquaresGradient(topology)
  private val updater = new ANNUpdater()
  private val optimizer = new LBFGS(gradient, updater).
    setConvergenceTol(convergenceTol).
    setMaxNumIterations(maxNumIterations)

  /**
   * Trains the ANN model.
   * Uses default convergence tolerance 1e-4 for LBFGS.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param initialWeights the initial weights of the ANN
   * @return ANN model.
   */
  private def run(trainingRDD: RDD[(Vector, Vector)], initialWeights: Vector):
      ArtificialNeuralNetworkModel = {
    val data = trainingRDD.map(v =>
      (0.0,
        Vectors.fromBreeze(DenseVector.vertcat(
          v._1.toBreeze.toDenseVector,
          v._2.toBreeze.toDenseVector))
        ))
    val weights = optimizer.optimize(data, initialWeights)
    new ArtificialNeuralNetworkModel(weights, topology)
  }
}

/**
 * Top level methods for training the artificial neural network (ANN)
 */
object ArtificialNeuralNetwork {

  private val defaultTolerance: Double = 1e-4

  /**
   * Trains an ANN.
   * Uses default convergence tolerance 1e-4 for LBFGS.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param hiddenLayersTopology number of nodes per hidden layer, excluding the bias nodes.
   * @param maxNumIterations specifies maximum number of training iterations.
   * @return ANN model.
   */
  def train(
      trainingRDD: RDD[(Vector, Vector)],
      hiddenLayersTopology: Array[Int],
      maxNumIterations: Int): ArtificialNeuralNetworkModel = {
    train(trainingRDD, hiddenLayersTopology, maxNumIterations, defaultTolerance)
  }

  /**
   * Continues training of an ANN.
   * Uses default convergence tolerance 1e-4 for LBFGS.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param model model of an already partly trained ANN.
   * @param maxNumIterations maximum number of training iterations.
   * @return ANN model.
   */
  def train(
      trainingRDD: RDD[(Vector,Vector)],
      model: ArtificialNeuralNetworkModel,
      maxNumIterations: Int): ArtificialNeuralNetworkModel = {
    train(trainingRDD, model, maxNumIterations, defaultTolerance)
  }

  /**
   * Trains an ANN with given initial weights.
   * Uses default convergence tolerance 1e-4 for LBFGS.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param initialWeights initial weights vector.
   * @param maxNumIterations maximum number of training iterations.
   * @return ANN model.
   */
  def train(
      trainingRDD: RDD[(Vector,Vector)],
      hiddenLayersTopology: Array[Int],
      initialWeights: Vector,
      maxNumIterations: Int): ArtificialNeuralNetworkModel = {
    train(trainingRDD, hiddenLayersTopology, initialWeights, maxNumIterations, defaultTolerance)
  }

  /**
   * Trains an ANN using customized convergence tolerance.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param model model of an already partly trained ANN.
   * @param maxNumIterations maximum number of training iterations.
   * @param convergenceTol convergence tolerance for LBFGS. Smaller value for closer convergence.
   * @return ANN model.
   */
  def train(
      trainingRDD: RDD[(Vector,Vector)],
      model: ArtificialNeuralNetworkModel,
      maxNumIterations: Int,
      convergenceTol: Double): ArtificialNeuralNetworkModel = {
    new ArtificialNeuralNetwork(model.topology, maxNumIterations, convergenceTol).
      run(trainingRDD, model.weights)
  }

  /**
   * Continues training of an ANN using customized convergence tolerance.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param hiddenLayersTopology number of nodes per hidden layer, excluding the bias nodes.
   * @param maxNumIterations maximum number of training iterations.
   * @param convergenceTol convergence tolerance for LBFGS. Smaller value for closer convergence.
   * @return ANN model.
   */
  def train(
      trainingRDD: RDD[(Vector, Vector)],
      hiddenLayersTopology: Array[Int],
      maxNumIterations: Int,
      convergenceTol: Double): ArtificialNeuralNetworkModel = {
    val topology = convertTopology(trainingRDD, hiddenLayersTopology)
    new ArtificialNeuralNetwork(topology, maxNumIterations, convergenceTol).
      run(trainingRDD, randomWeights(topology, false))
  }

  /**
   * Trains an ANN with given initial weights.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param initialWeights initial weights vector.
   * @param maxNumIterations maximum number of training iterations.
   * @param convergenceTol convergence tolerance for LBFGS. Smaller value for closer convergence.
   * @return ANN model.
   */
  def train(
      trainingRDD: RDD[(Vector,Vector)],
      hiddenLayersTopology: Array[Int],
      initialWeights: Vector,
      maxNumIterations: Int,
      convergenceTol: Double): ArtificialNeuralNetworkModel = {
    val topology = convertTopology(trainingRDD, hiddenLayersTopology)
    new ArtificialNeuralNetwork(topology, maxNumIterations, convergenceTol).
      run(trainingRDD, initialWeights)
  }

  /**
   * Provides a random weights vector.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param hiddenLayersTopology number of nodes per hidden layer, excluding the bias nodes.
   * @return random weights vector.
   */
  def randomWeights(
      trainingRDD: RDD[(Vector,Vector)],
      hiddenLayersTopology: Array[Int]): Vector = {
    val topology = convertTopology(trainingRDD, hiddenLayersTopology)
    return randomWeights(topology, false)
  }

  /**
   * Provides a random weights vector, using given random seed.
   *
   * @param trainingRDD RDD containing (input, output) pairs for later training.
   * @param hiddenLayersTopology number of nodes per hidden layer, excluding the bias nodes.
   * @param seed random generator seed.
   * @return random weights vector.
   */
  def randomWeights(
      trainingRDD: RDD[(Vector,Vector)],
      hiddenLayersTopology: Array[Int],
      seed: Int): Vector = {
    val topology = convertTopology(trainingRDD, hiddenLayersTopology)
    return randomWeights(topology, true, seed)
  }

  /**
   * Provides a random weights vector, using given random seed.
   *
   * @param inputLayerSize size of input layer.
   * @param outputLayerSize size of output layer.
   * @param hiddenLayersTopology number of nodes per hidden layer, excluding the bias nodes.
   * @param seed random generator seed.
   * @return random weights vector.
   */
  def randomWeights(
      inputLayerSize: Int,
      outputLayerSize: Int,
      hiddenLayersTopology: Array[Int],
      seed: Int): Vector = {
    val topology = inputLayerSize +: hiddenLayersTopology :+ outputLayerSize
    return randomWeights(topology, true, seed)
  }

  private def convertTopology(
      input: RDD[(Vector,Vector)],
      hiddenLayersTopology: Array[Int] ): Array[Int] = {
    val firstElt = input.first
    firstElt._1.size +: hiddenLayersTopology :+ firstElt._2.size
  }

  private def randomWeights(topology: Array[Int], useSeed: Boolean, seed: Int = 0): Vector = {
    val rand: XORShiftRandom =
      if( useSeed == false ) new XORShiftRandom() else new XORShiftRandom(seed)
    var i: Int = 0
    var l: Int = 0
    val noWeights = {
      var tmp = 0
      var i = 1
      while (i < topology.size) {
        tmp = tmp + topology(i) * (topology(i - 1) + 1)
        i += 1
      }
      tmp
    }
    val initialWeightsArr = new Array[Double](noWeights)
    var pos = 0
    l = 1
    while (l < topology.length) {
      i = 0
      while (i < (topology(l) * (topology(l - 1) + 1))) {
        initialWeightsArr(pos) = (rand.nextDouble * 4.8 - 2.4) / (topology(l - 1) + 1)
        pos += 1
        i += 1
      }
      l += 1
    }
    Vectors.dense(initialWeightsArr)
  }
}

/**
 * Helper methods for ANN
 */
private[ann] trait ANNHelper {
  protected val topology: Array[Int]
  protected def g(x: Double) = 1.0 / (1.0 + math.exp(-x))
  protected val L = topology.length - 1
  protected val noWeights = {
    var tmp = 0
    var l = 1
    while (l <= L) {
      tmp = tmp + topology(l) * (topology(l - 1) + 1)
      l += 1
    }
    tmp
  }
  protected val ofsWeight: Array[Int] = {
    val tmp = new Array[Int](L + 1)
    var curPos = 0
    tmp(0) = 0
    var l = 1
    while (l <= L) {
      tmp(l) = curPos
      curPos = curPos + (topology(l - 1) + 1) * topology(l)
      l += 1
    }
    tmp
  }
  protected val noNodes: Int = {
    var tmp: Integer = 0
    var l = 0
    while (l < topology.size) {
      tmp = tmp + topology(l)
      l += 1
    }
    tmp
  }
  protected val ofsNode: Array[Int] = {
    val tmp = new Array[Int](L + 1)
    tmp(0) = 0
    var l = 1
    while (l <= L) {
      tmp(l) = tmp(l - 1) + topology(l - 1)
      l += 1
    }
    tmp
  }

  protected def forwardRun(arrData: Array[Double], arrWeights: Array[Double]): Array[Double] = {
    val arrNodes = new Array[Double](noNodes)
    var i: Int = 0
    var j: Int = 0
    var l: Int = 0
    i = 0
    while (i < topology(0)) {
      arrNodes(i) = arrData(i)
      i += 1
    }
    l = 1
    while (l <= L) {
      j = 0
      while (j < topology(l)) {
        var cum: Double = 0.0
        i = 0
        while (i < topology(l - 1)) {
          cum = cum +
            arrWeights(ofsWeight(l) + (topology(l - 1) + 1) * j + i) *
              arrNodes(ofsNode(l - 1) + i)
          i += 1
        }
        cum = cum + arrWeights(ofsWeight(l) + (topology(l - 1) + 1) * j + topology(l - 1))
        arrNodes(ofsNode(l) + j) = g(cum)
        j += 1
      }
      l += 1
    }
    arrNodes
  }
}

private class ANNLeastSquaresGradient(val topology: Array[Int]) extends Gradient with ANNHelper {

  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val arrData = data.toArray
    val arrWeights = weights.toArray
    var i: Int = 0
    var j: Int = 0
    var l: Int = 0
    // forward run
    val arrNodes = forwardRun(arrData, arrWeights)
    val arrDiff = new Array[Double](topology(L))
    j = 0
    while (j < topology(L)) {
      arrDiff(j) = arrNodes(ofsNode(L) + j) - arrData(topology(0) + j)
      j += 1
    }
    var err: Double = 0
    j = 0
    while (j < topology(L)) {
      err = err + arrDiff(j) * arrDiff(j)
      j += 1
    }
    err = err * .5
    // back propagation
    val arrDelta = new Array[Double](noNodes)
    j = 0
    while (j < topology(L)) {
      arrDelta(ofsNode(L) + j) =
        arrDiff(j) *
          arrNodes(ofsNode(L) + j) * (1 - arrNodes(ofsNode(L) + j))
      j += 1
    }
    l = L - 1
    while (l > 0) {
      j = 0
      while (j < topology(l)) {
        var cum: Double = 0.0
        i = 0
        while (i < topology(l + 1)) {
          cum = cum +
            arrWeights(ofsWeight(l + 1) + (topology(l) + 1) * i + j) *
              arrDelta(ofsNode(l + 1) + i) *
              arrNodes(ofsNode(l) + j) * (1 - arrNodes(ofsNode(l) + j))
          i += 1
        }
        arrDelta(ofsNode(l) + j) = cum
        j += 1
      }
      l -= 1
    }
    // gradient
    val arrGrad = new Array[Double](noWeights)
    l = 1
    while (l <= L) {
      j = 0
      while (j < topology(l)) {
        i = 0
        while (i < topology(l - 1)) {
          arrGrad(ofsWeight(l) + (topology(l - 1) + 1) * j + i) =
            arrNodes(ofsNode(l - 1) + i) *
              arrDelta(ofsNode(l) + j)
          i += 1
        }
        arrGrad(ofsWeight(l) + (topology(l - 1) + 1) * j + topology(l - 1)) =
          arrDelta(ofsNode(l) + j)
        j += 1
      }
      l += 1
    }
    (Vectors.dense(arrGrad), err)
  }

  override def compute(
      data: Vector,
      label: Double,
      weights: Vector,
      cumGradient: Vector): Double = {
    val (grad, err) = compute(data, label, weights)
    cumGradient.toBreeze += grad.toBreeze
    err
  }
}

private class ANNUpdater extends Updater {

  override def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {
    val thisIterStepSize = stepSize
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
    (Vectors.fromBreeze(brzWeights), 0)
  }
}
