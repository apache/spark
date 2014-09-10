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
 * For a network of L layers:
 *
 * topology( l ) indicates the number of nodes in layer l, excluding the bias node.
 *
 * noInput = topology(0), the number of input nodes
 * noOutput = topology(L-1), the number of output nodes
 *
 * input = data( 0 to noInput-1 )
 * output = data( noInput to noInput+noOutput-1 )
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

class ArtificialNeuralNetworkModel private[mllib](val weights: Vector, val topology: Array[Int])
  extends Serializable {

  private val L = topology.length - 1

  private val ofsWeight: Array[Int] = {
    val tmp = new Array[Int](L + 1)
    var curPos = 0
    tmp(0) = 0
    var l = 1
    while(l <= L) {
      tmp(l) = curPos
      curPos = curPos + (topology(l - 1) + 1) * (topology(l))
      l = l + 1
    }
    tmp
  }

  private def g(x: Double) = 1.0 / (1.0 + math.exp(-x))

  def computeValues(arrData: Array[Double], arrWeights: Array[Double]): Array[Double] = {
    var arrPrev = new Array[Double](topology(0))
    var i: Int = 0
    var j: Int = 0
    var l: Int = 0
    i = 0
    while(i < topology(0)) {
      arrPrev(i) = arrData(i)
      i = i + 1
    }
    l = 1
    while(l <= L) {
      val arrCur = new Array[Double](topology(l))
      j = 0
      while(j < topology(l)) {
        var cum: Double = 0.0
        i = 0
        while( i < topology(l - 1) ) {
          cum = cum +
            arrPrev(i) * arrWeights(ofsWeight(l) + (topology(l - 1) + 1) * j + i)
          i = i + 1
        }
        cum = cum +
          arrWeights(ofsWeight(l) + (topology(l - 1) + 1) * j + topology(l - 1)) // bias
        arrCur(j) = g(cum)
        j = j + 1
      }
      arrPrev = arrCur
      l = l + 1
    }
    arrPrev
  }

  def predictPoint(data: Vector, weights: Vector): Double = {
    val outp = computeValues(data.toArray, weights.toArray)
    outp(0)
  }

  def predictPointV(data: Vector, weights: Vector): Vector = {
    Vectors.dense(computeValues(data.toArray, weights.toArray))
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return Vector prediction from the trained model
   *
   *         Returns the complete vector.
   */
  def predictV(testData: Vector): Vector = {
    predictPointV(testData, weights)
  }

}

class ArtificialNeuralNetwork private(
    private var topology: Array[Int],
    private var numIterations: Int,
    private var stepSize: Double,
    private var miniBatchFraction: Double)
  extends Serializable {

  private val gradient = new ANNLeastSquaresGradient(topology)
  private val updater = new ANNUpdater()
  private val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setMiniBatchFraction(miniBatchFraction)

  private def run(input: RDD[(Vector, Vector)], initialWeights: Vector):
  ArtificialNeuralNetworkModel = {
    val data = input.map(v =>
      (0.0,
        Vectors.fromBreeze(DenseVector.vertcat(
          v._1.toBreeze.toDenseVector,
          v._2.toBreeze.toDenseVector))
        ))
    val weights = optimizer.optimize(data, initialWeights)
    new ArtificialNeuralNetworkModel(weights, topology)
  }
}

object ArtificialNeuralNetwork {

  def train(
      input: RDD[(Vector, Vector)],
      topology: Array[Int],
      initialWeights: Vector,
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double): ArtificialNeuralNetworkModel = {
    new ArtificialNeuralNetwork(topology, numIterations, stepSize, miniBatchFraction)
      .run(input, initialWeights)
  }

  def train(
      input: RDD[(Vector, Vector)],
      topology: Array[Int],
      initialWeights: Vector,
      numIterations: Int,
      stepSize: Double): ArtificialNeuralNetworkModel = {
    new ArtificialNeuralNetwork(topology, numIterations, stepSize, 1.0).run(input, initialWeights)
  }

  def train(
      input: RDD[(Vector,Vector)],
      model: ArtificialNeuralNetworkModel,
      numIterations: Int,
      stepSize: Double): ArtificialNeuralNetworkModel = {
    train(input, model.topology, model.weights, numIterations, stepSize)
  }


  def train(
      input: RDD[(Vector, Vector)],
      topology: Array[Int],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double): ArtificialNeuralNetworkModel = {
    new ArtificialNeuralNetwork(topology, numIterations, stepSize, miniBatchFraction)
      .run(input, randomWeights(topology))
  }

  def train(
      input: RDD[(Vector, Vector)],
      topology: Array[Int],
      numIterations: Int,
      stepSize: Double): ArtificialNeuralNetworkModel = {
    train(input, topology, numIterations, stepSize, 1.0)
  }

  def randomWeights(topology: Array[Int]): Vector = {
    val rand = new XORShiftRandom()
    
    var i: Int = 0
    var l: Int = 0
    
    val noWeights = {
      var tmp = 0
      var i = 1
      while(i < topology.size) {
        tmp = tmp + topology(i) * (topology(i - 1) + 1)
        i = i + 1
      }
      tmp
    }

    val initialWeightsArr = new Array[Double](noWeights)
    var pos = 0;
    
    l = 1
    while( l < topology.length) {
      i = 0
      while(i < (topology(l) * (topology(l - 1) + 1))) {
        initialWeightsArr(pos) = (rand.nextDouble * 4.8 - 2.4) / (topology(l - 1) + 1)
        pos += 1;
        i += 1
      }
      l += 1
    }
    Vectors.dense(initialWeightsArr)
  }

}

private class ANNLeastSquaresGradient(topology: Array[Int]) extends Gradient {

  private def g(x: Double) = 1.0 / (1.0 + math.exp(-x))

  private val L = topology.length - 1

  private val noWeights = {
    var tmp = 0
    var l = 1
    while(l <= L) {
      tmp = tmp + topology(l) * (topology(l - 1) + 1)
      l += 1
    }
    tmp
  }

  val ofsWeight: Array[Int] = {
    val tmp = new Array[Int](L + 1)
    var curPos = 0;
    tmp(0) = 0;
    var l = 1
    while(l <= L) {
      tmp(l) = curPos
      curPos = curPos + (topology(l - 1) + 1) * (topology(l))
      l += 1
    }
    tmp
  }

  val noNodes: Int = {
    var tmp: Integer = 0
    var l = 0
    while(l < topology.size) {
      tmp = tmp + topology(l)
      l += 1
    }
    tmp
  }

  val ofsNode: Array[Int] = {
    val tmp = new Array[Int](L + 1)
    tmp(0) = 0
    var l = 1
    while(l <= L) {
      tmp(l) = tmp(l - 1) + topology(l - 1)
      l += 1
    }
    tmp
  }

  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val arrData = data.toArray
    val arrWeights = weights.toArray
    val arrNodes = new Array[Double](noNodes)
        
    var i: Int = 0
    var j: Int = 0
    var l: Int = 0
    
    // forward run
    i = 0;
    while(i < topology(0)) {
      arrNodes(i) = arrData(i)
      i += 1
    }
    l = 1
    while( l <= L ) {
      j = 0
      while(j < topology(l)) {
        var cum: Double = 0.0;
        i = 0
        while(i < topology(l - 1)) {
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
    val arrDiff = new Array[Double](topology(L))
    j = 0
    while( j < topology(L)) {
      arrDiff(j) = (arrNodes(ofsNode(L) + j) - arrData(topology(0) + j))
      j += 1
    }
    var err: Double = 0;
    j = 0
    while(j < topology(L)) {
      err = err + arrDiff(j) * arrDiff(j)
      j += 1
    }
    err = err * .5
    // back propagation
    val arrDelta = new Array[Double](noNodes)
    j = 0
    while(j < topology(L)) {
      arrDelta(ofsNode(L) + j) =
        arrDiff(j) *
          arrNodes(ofsNode(L) + j) * (1 - arrNodes(ofsNode(L) + j))
      j += 1
    }
    l = L - 1
    while(l > 0) {
      j = 0
      while(j < topology(l)) {
        var cum: Double = 0.0
        i = 0
        while( i < topology(l + 1)) {
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
    while(l <= L) {
      j = 0
      while(j < topology(l)) {
        i = 0
        while(i < topology(l - 1)) {
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
    return err
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
