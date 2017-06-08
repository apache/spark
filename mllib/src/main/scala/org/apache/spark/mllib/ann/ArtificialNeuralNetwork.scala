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

import breeze.linalg.{axpy => brzAxpy, Vector => BV, DenseVector => BDV,
DenseMatrix => BDM, sum => Bsum, argmax => Bargmax, norm => Bnorm, *}
import breeze.numerics.{sigmoid => Bsigmoid}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg

import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Vector, Vectors}
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
  extends Serializable with NeuralHelper {

  val (weightMatrices, bias) = unrollWeights(weights)

  /**
   * Predicts values for a single data point using the trained model.
   *
   * @param testData represents a single data point.
   * @return prediction using the trained model.
   */
  def predict(testData: Vector): Vector = {
    Vectors.dense(computeValues(testData, topology.length - 1))
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

  private def computeValues(testData: Vector, layer: Int): Array[Double] = {
    require(layer >=0 && layer < topology.length)
    /* TODO: BDM */
    val outputs = forwardRun(testData.toBreeze.toDenseVector.toDenseMatrix.t, weightMatrices, bias)
    outputs(layer).toArray
  }

  /**
   * Returns output values of a given layer for a single data point using the trained model.
   *
   * @param testData RDD represents a single data point.
   * @param layer index of a network layer
   * @return output of a given layer.
   */
  def output(testData: Vector, layer: Int): Vector = {
    Vectors.dense(computeValues(testData, layer))
  }

  /**
   * Returns weights for a given layer in vector form.
   *
   * @param index index of a layer: ranges from 1 until topology.length.
   *              (no weights for the 0 layer)
   * @return weights.
   */
  def weightsByLayer(index: Int): Vector = {
    require(index > 0 && index < topology.length)
    val layerWeight = BDV.vertcat(weightMatrices(index).toDenseVector, bias(index).toDenseVector)
    Vectors.dense(layerWeight.toArray)
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
                                              convergenceTol: Double,
                                              batchSize: Int = 1)
  extends Serializable {

  private val gradient = new ANNLeastSquaresGradient(topology, batchSize)
  private val updater = new ANNUpdater()
  private val optimizer = new LBFGS(gradient, updater).
    setConvergenceTol(convergenceTol).
    setNumIterations(maxNumIterations)

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
    val t = System.currentTimeMillis()
    val data = if (batchSize == 1) {
      trainingRDD.map(v =>
        (0.0,
          Vectors.fromBreeze(BDV.vertcat(
            v._1.toBreeze.toDenseVector,
            v._2.toBreeze.toDenseVector))
          ))
    } else { trainingRDD.mapPartitions { it =>
      it.grouped(batchSize).map { seq =>
        val size = seq.size
        val bigVector = new Array[Double](topology(0) * size + topology.last * size)
        var i = 0
        seq.foreach { case (in, out) =>
          System.arraycopy(in.toArray, 0, bigVector, i * topology(0), topology(0))
          System.arraycopy(out.toArray, 0, bigVector,
            topology(0) * size + i * topology.last, topology.last)
          i += 1
        }
        (0.0, Vectors.dense(bigVector))
      }
    }
    }
    val weights = optimizer.optimize(data, initialWeights)
    new ArtificialNeuralNetworkModel(weights, topology)
  }
}

/**
 * Top level methods for training the artificial neural network (ANN)
 */
object ArtificialNeuralNetwork {

  private val defaultTolerance: Double = 1e-4


  def train(trainingRDD: RDD[(Vector, Vector)],
            batchSize: Int,
            hiddenLayersTopology: Array[Int],
            initialWeights: Vector,
            maxNumIterations: Int,
            convergenceTol: Double) : ArtificialNeuralNetworkModel = {
    val topology = convertTopology(trainingRDD, hiddenLayersTopology)
    new ArtificialNeuralNetwork(topology, maxNumIterations, convergenceTol, batchSize).
      run(trainingRDD, initialWeights)
  }

  def train(trainingRDD: RDD[(Vector, Vector)],
            batchSize: Int,
            hiddenLayersTopology: Array[Int],
            maxNumIterations: Int) : ArtificialNeuralNetworkModel = {
    val topology = convertTopology(trainingRDD, hiddenLayersTopology)
    new ArtificialNeuralNetwork(topology, maxNumIterations, defaultTolerance, batchSize).
      run(trainingRDD, randomWeights(topology, false))
  }

  /**
   * Trains an ANN.
   * Uses default convergence tolerance 1e-4 for LBFGS.
   *
   * @param trainingRDD RDD containing (input, output) pairs for training.
   * @param hiddenLayersTopology number of nodes per hidden layer, excluding the bias nodes.
   * @param maxNumIterations specifies maximum number of training iterations.
   * @return ANN model.
   */
  def train(trainingRDD: RDD[(Vector, Vector)],
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
  def train(trainingRDD: RDD[(Vector,Vector)],
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
  def train(trainingRDD: RDD[(Vector,Vector)],
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
  def train(trainingRDD: RDD[(Vector,Vector)],
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
  def train(trainingRDD: RDD[(Vector, Vector)],
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
  def train(trainingRDD: RDD[(Vector,Vector)],
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
  def randomWeights(trainingRDD: RDD[(Vector,Vector)],
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
  def randomWeights(trainingRDD: RDD[(Vector,Vector)],
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
  def randomWeights(inputLayerSize: Int,
                    outputLayerSize: Int,
                    hiddenLayersTopology: Array[Int],
                    seed: Int): Vector = {
    val topology = inputLayerSize +: hiddenLayersTopology :+ outputLayerSize
    return randomWeights(topology, true, seed)
  }

  private def convertTopology(input: RDD[(Vector,Vector)],
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
 * ::Experimental::
 * Trait for roll/unroll weights and forward/back propagation in neural network
 */
@Experimental
private[ann] trait NeuralHelper {
  protected val topology: Array[Int]
  protected val weightCount =
    (for(i <- 1 until topology.size) yield (topology(i) * topology(i - 1))).sum +
      topology.sum - topology(0)

  protected def unrollWeights(weights: linalg.Vector): (Array[BDM[Double]], Array[BDV[Double]]) = {
    require(weights.size == weightCount)
    val weightsCopy = weights.toArray
    val weightMatrices = new Array[BDM[Double]](topology.size)
    val bias = new Array[BDV[Double]](topology.size)
    var offset = 0
    for(i <- 1 until topology.size){
      weightMatrices(i) = new BDM[Double](topology(i), topology(i - 1), weightsCopy, offset)
      offset += topology(i) * topology(i - 1)
      /* TODO: BDM */
      bias(i) = new BDV[Double](weightsCopy, offset, 1, topology(i))
      offset += topology(i)
    }
    (weightMatrices, bias)
  }

  protected def rollWeights(weightMatricesUpdate: Array[BDM[Double]],
                            biasUpdate: Array[BDV[Double]],
                            cumGradient: Vector): Unit = {
    val wu = cumGradient.toArray
    var offset = 0
    for(i <- 1 until topology.length){
      var k = 0
      val numElements = topology(i) * topology(i - 1)
      while(k < numElements){
        wu(offset + k) += weightMatricesUpdate(i).data(k)
        k += 1
      }
      offset += numElements
      k = 0
      while(k < topology(i)){
        wu(offset + k) += biasUpdate(i).data(k)
        k += 1
      }
      offset += topology(i)
    }
  }

  protected def forwardRun(data: BDM[Double], weightMatrices: Array[BDM[Double]],
                           bias: Array[BDV[Double]]): Array[BDM[Double]] = {
    val outArray = new Array[BDM[Double]](topology.size)
    outArray(0) = data
    for(i <- 1 until topology.size) {
      outArray(i) = weightMatrices(i) * outArray(i - 1)// :+ bias(i))
      outArray(i)(::, *) :+= bias(i)
      Bsigmoid.inPlace(outArray(i))
    }
    outArray
  }

  protected def wGradient(weightMatrices: Array[BDM[Double]],
                          targetOutput: BDM[Double],
                          outputs: Array[BDM[Double]]):
  (Array[BDM[Double]], Array[BDV[Double]]) = {
    /* error back propagation */
    val deltas = new Array[BDM[Double]](topology.size)
    val avgDeltas = new Array[BDV[Double]](topology.size)
    for(i <- (topology.size - 1) until (0, -1)){
      /* TODO: GEMM? */
      val outPrime = BDM.ones[Double](outputs(i).rows, outputs(i).cols)
      outPrime :-= outputs(i)
      outPrime :*= outputs(i)
      if(i == topology.size - 1){
        deltas(i) = (outputs(i) :- targetOutput) :* outPrime
      }else{
        deltas(i) = (weightMatrices(i + 1).t * deltas(i + 1)) :* outPrime
      }
      avgDeltas(i) = Bsum(deltas(i)(*, ::))
      avgDeltas(i) :/= outputs(i).cols.toDouble
    }
    /* gradient */
    val gradientMatrices = new Array[BDM[Double]](topology.size)
    for(i <- (topology.size - 1) until (0, -1)) {
      /* TODO: GEMM? */
      gradientMatrices(i) = deltas(i) * outputs(i - 1).t
      /* NB! dividing by the number of instances in
       * the batch to be transparent for the optimizer */
      gradientMatrices(i) :/= outputs(i).cols.toDouble
    }
    (gradientMatrices, avgDeltas)
  }
}


private class ANNLeastSquaresGradient(val topology: Array[Int],
                                      val batchSize: Int = 1) extends Gradient with NeuralHelper {

  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val gradient = Vectors.zeros(weights.size)
    val loss = compute(data, label, weights, gradient)
    (gradient, loss)
  }

  override def compute(data: Vector, label: Double, weights: Vector,
                       cumGradient: Vector): Double = {
    val arrData = data.toArray
    val realBatchSize = arrData.length / (topology(0) + topology.last)
    val input = new BDM(topology(0), realBatchSize, arrData)
    val target = new BDM(topology.last, realBatchSize, arrData, topology(0) * realBatchSize)
    val (weightMatrices, bias) = unrollWeights(weights)
    /* forward run */
    val outputs = forwardRun(input, weightMatrices, bias)
    /* error back propagation */
    val (gradientMatrices, deltas) = wGradient(weightMatrices, target, outputs)
    rollWeights(gradientMatrices, deltas, cumGradient)
    /* error */
    val diff = target :- outputs(topology.size - 1)
    val outerError = Bsum(diff :* diff) / 2
    /* NB! dividing by the number of instances in
     * the batch to be transparent for the optimizer */
    outerError / realBatchSize
  }
}

private class ANNUpdater extends Updater {

  override def compute(weightsOld: Vector,
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
