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

import scala.collection.JavaConversions._

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, axpy => brzAxpy,
argmax => brzArgMax, norm => brzNorm}

import org.apache.spark.annotation.Experimental
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Vector => SV, DenseVector => SDV, Vectors, BLAS}
import org.apache.spark.mllib.optimization.{Gradient, Updater, LBFGS, GradientDescent}
import org.apache.spark.rdd.RDD

class MLP(
  val innerLayers: Array[Layer],
  val hiddenLayersDropoutRate: Double = 0.5,
  val inputLayerDropoutRate: Double = 0.2) extends Logging with Serializable {

  def this(topology: Array[Int]) {
    this(MLP.initializeLayers(topology))
  }

  assert(hiddenLayersDropoutRate >= 0 && hiddenLayersDropoutRate < 1)
  assert(inputLayerDropoutRate >= 0 && inputLayerDropoutRate < 1)

  val topology: Array[Int] = {
    val topology = new Array[Int](numLayer + 1)
    topology(0) = numInput
    for (i <- 1 to numLayer) {
      topology(i) = innerLayers(i - 1).numOut
    }
    topology
  }

  private lazy val rand: Random = new Random()

  def numLayer = innerLayers.length

  def numInput = innerLayers.head.numIn

  def numOut = innerLayers.last.numOut

  def predict(x: BDM[Double]): BDM[Double] = {
    var output = x
    for (layer <- 0 until numLayer) {
      output = innerLayers(layer).forward(output)
      val rate = layerDropoutRate(layer)
      if (rate > 0 && layer < numLayer - 1) {
        output :*= (1 - rate)
      }
    }
    output
  }

  private[mllib] def learn(x: BDM[Double], label: BDM[Double]): (Array[(BDM[Double],
    BDV[Double])], Double, Double) = {
    val batchSize = x.cols
    val in = new Array[BDM[Double]](numLayer)
    val out = new Array[BDM[Double]](numLayer)
    val delta = new Array[BDM[Double]](numLayer)
    val grads = new Array[(BDM[Double], BDV[Double])](numLayer)
    val dropOutMasks: Array[BDM[Double]] = if (layerDropoutRate(0) > 0) {
      dropOutMask(batchSize)
    }
    else {
      null
    }

    for (layer <- 0 until numLayer) {
      val input = if (layer == 0) {
        x
      } else {
        out(layer - 1)
      }
      in(layer) = input

      val output = innerLayers(layer).forward(input)
      if (layer < numLayer - 1 && dropOutMasks != null) {
        assert(output.rows == dropOutMasks(layer).rows)
        output :*= dropOutMasks(layer)
      }
      out(layer) = output
    }

    for (layer <- (0 until numLayer).reverse) {
      val input = in(layer)
      val output = out(layer)
      delta(layer) = if (layer == numLayer - 1) {
        innerLayers(layer).computeDeltaTop(output, label)
      } else {
        innerLayers(layer).computeDeltaMiddle(output, innerLayers(layer + 1), delta(layer + 1))
      }
      if (layer < numLayer - 1 && dropOutMasks != null) {
        delta(layer) :*= dropOutMasks(layer)
      }
      grads(layer) = innerLayers(layer).backward(input, delta(layer))
    }

    val ce = crossEntropy(out.last, label)
    (grads, ce, batchSize.toDouble)
  }

  private def crossEntropy(out: BDM[Double], label: BDM[Double]): Double = {
    assert(label.rows == out.rows)
    assert(label.cols == out.cols)
    var cost = 0D
    for (i <- 0 until out.rows) {
      for (j <- 0 until out.cols) {
        val a = label(i, j)
        var b = out(i, j)
        if (b == 0) {
          b += 1e-15
        } else if (b == 1D) {
          b -= 1e-15
        }
        cost += a * math.log(b) + (1 - a) * math.log1p(1 - b)
      }
    }
    (0D - cost) / out.rows
  }

  private def dropOutMask(cols: Int): Array[BDM[Double]] = {
    val masks = new Array[BDM[Double]](numLayer - 1)
    for (layer <- 0 until numLayer - 1) {
      val dropoutRate = layerDropoutRate(layer)
      val rows = innerLayers(layer).numOut
      val mask = new BDM[Double](rows, cols)
      for (i <- 0 until rows) {
        for (j <- 0 until cols) {
          mask(i, j) = if (rand.nextDouble() > dropoutRate) 1D else 0D
        }
      }
      masks(layer) = mask
    }
    masks
  }

  private def layerDropoutRate(layer: Int): Double = {
    if (layer == 0 && inputLayerDropoutRate > 0) {
      inputLayerDropoutRate
    }
    else {
      hiddenLayersDropoutRate
    }
  }

  private[mllib] def assign(newNN: MLP): MLP = {
    innerLayers.zip(newNN.innerLayers).foreach { case (oldLayer, newLayer) =>
      oldLayer.weight := newLayer.weight
      oldLayer.bias := newLayer.bias
    }
    this
  }

  def setSeed(seed: Long): Unit = {
    rand.setSeed(seed)
  }
}

object MLP extends Logging {
  def train(
    data: RDD[(SV, SV)],
    batchSize: Int,
    numIteration: Int,
    topology: Array[Int],
    fraction: Double,
    learningRate: Double,
    weightCost: Double): MLP = {
    train(data, batchSize, numIteration, new MLP(topology),
      fraction, learningRate, weightCost)
  }

  def train(
    data: RDD[(SV, SV)],
    batchSize: Int,
    numIteration: Int,
    nn: MLP,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): MLP = {
    runSGD(data, nn, batchSize, numIteration,
      fraction, learningRate, weightCost)
  }

  def runSGD(
    trainingRDD: RDD[(SV, SV)],
    topology: Array[Int],
    batchSize: Int,
    maxNumIterations: Int,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): MLP = {
    val nn = new MLP(topology)
    runSGD(trainingRDD, nn, batchSize, maxNumIterations,
      fraction, learningRate, weightCost)
  }

  def runSGD(
    data: RDD[(SV, SV)],
    nn: MLP,
    batchSize: Int,
    maxNumIterations: Int,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): MLP = {
    runSGD(data, nn, batchSize, maxNumIterations, fraction,
      learningRate, weightCost, 1 - 1e-2, 1e-8)
  }

  def runSGD(
    data: RDD[(SV, SV)],
    nn: MLP,
    batchSize: Int,
    maxNumIterations: Int,
    fraction: Double,
    learningRate: Double,
    weightCost: Double,
    rho: Double,
    epsilon: Double): MLP = {
    val gradient = new MLPGradient(nn)
    val updater = new MLPAdaDeltaUpdater(nn.topology, rho, epsilon)
    val optimizer = new GradientDescent(gradient, updater).
      setMiniBatchFraction(fraction).
      setNumIterations(maxNumIterations).
      setRegParam(weightCost).
      setStepSize(learningRate)

    val trainingRDD = toTrainingRDD(data, batchSize, nn.numInput, nn.numOut)
    trainingRDD.cache().setName("MLP-dataBatch")
    val weights = optimizer.optimize(trainingRDD, toVector(nn))
    trainingRDD.unpersist()
    fromVector(nn, weights)
    nn
  }

  @Experimental
  def runLBFGS(
    trainingRDD: RDD[(SV, SV)],
    topology: Array[Int],
    batchSize: Int,
    maxNumIterations: Int,
    convergenceTol: Double,
    weightCost: Double): MLP = {
    val nn = new MLP(topology)
    runLBFGS(trainingRDD, nn, batchSize, maxNumIterations,
      convergenceTol, weightCost)
  }

  @Experimental
  def runLBFGS(
    data: RDD[(SV, SV)],
    nn: MLP,
    batchSize: Int,
    maxNumIterations: Int,
    convergenceTol: Double,
    weightCost: Double): MLP = {
    val gradient = new MLPGradient(nn, batchSize)
    val updater = new MLPUpdater(nn.topology)
    val optimizer = new LBFGS(gradient, updater).
      setConvergenceTol(convergenceTol).
      setNumIterations(maxNumIterations).
      setRegParam(weightCost)

    val trainingRDD = toTrainingRDD(data, batchSize, nn.numInput, nn.numOut)
    val weights = optimizer.optimize(trainingRDD, toVector(nn))
    fromVector(nn, weights)
    nn
  }

  private[mllib] def fromVector(mlp: MLP, weights: SV): Unit = {
    val structure = vectorToStructure(mlp.topology, weights)
    val layers: Array[Layer] = mlp.innerLayers
    for (i <- 0 until structure.length) {
      val (weight, bias) = structure(i)
      val layer = layers(i)
      layer.setWeight(weight)
      layer.setBias(bias)

    }
  }

  private[mllib] def toVector(nn: MLP): SV = {
    structureToVector(nn.innerLayers.map(l => (l.weight, l.bias)))
  }

  private[mllib] def structureToVector(grads: Array[(BDM[Double], BDV[Double])]): SV = {
    val numLayer = grads.length
    val sumLen = grads.map(m => m._1.rows * m._1.cols + m._2.length).sum
    val data = new Array[Double](sumLen)
    var offset = 0
    for (l <- 0 until numLayer) {
      val (gradWeight, gradBias) = grads(l)
      val numIn = gradWeight.cols
      val numOut = gradWeight.rows
      System.arraycopy(gradWeight.toArray, 0, data, offset, numOut * numIn)
      offset += numIn * numOut
      System.arraycopy(gradBias.toArray, 0, data, offset, numOut)
      offset += numOut
    }
    new SDV(data)
  }

  private[mllib] def vectorToStructure(
    topology: Array[Int],
    weights: SV): Array[(BDM[Double], BDV[Double])] = {
    val data = weights.toArray
    val numLayer = topology.length - 1
    val grads = new Array[(BDM[Double], BDV[Double])](numLayer)
    var offset = 0
    for (layer <- 0 until numLayer) {
      val numIn = topology(layer)
      val numOut = topology(layer + 1)
      val weight = new BDM[Double](numOut, numIn, data, offset)
      offset += numIn * numOut
      val bias = new BDV[Double](data, offset, 1, numOut)
      offset += numOut
      grads(layer) = (weight, bias)
    }
    grads
  }

  def error(data: RDD[(SV, SV)], nn: MLP, batchSize: Int): Double = {
    val count = data.count()
    val dataBatches = batchMatrix(data, batchSize, nn.numInput, nn.numOut)
    val sumError = dataBatches.map { case (x, y) =>
      val h = nn.predict(x)
      (0 until h.cols).map(i => {
        if (brzArgMax(y(::, i)) == brzArgMax(h(::, i))) 0D else 1D
      }).sum
    }.sum
    sumError / count
  }

  private def toTrainingRDD(
    data: RDD[(SV, SV)],
    batchSize: Int,
    numInput: Int,
    numOut: Int): RDD[(Double, SV)] = {
    if (batchSize > 1) {
      batchVector(data, batchSize, numInput, numOut).map(t => (0D, t))
    }
    else {
      data.map { case (input, label) =>
        val sumLen = input.size + label.size
        val data = new Array[Double](sumLen)
        var offset = 0
        System.arraycopy(input.toArray, 0, data, offset, input.size)
        offset += input.size

        System.arraycopy(label.toArray, 0, data, offset, label.size)
        offset += label.size

        (0D, new SDV(data))
      }
    }
  }

  private[mllib] def batchMatrix(
    data: RDD[(SV, SV)],
    batchSize: Int,
    numInput: Int,
    numOut: Int): RDD[(BDM[Double], BDM[Double])] = {
    val dataBatch = data.mapPartitions { itr =>
      itr.grouped(batchSize).map { seq =>
        val x = BDM.zeros[Double](numInput, seq.size)
        val y = BDM.zeros[Double](numOut, seq.size)
        seq.zipWithIndex.foreach { case (v, i) =>
          x(::, i) :+= v._1.toBreeze
          y(::, i) :+= v._2.toBreeze
        }
        (x, y)
      }
    }
    dataBatch
  }

  private[mllib] def batchVector(
    data: RDD[(SV, SV)],
    batchSize: Int,
    numInput: Int,
    numOut: Int): RDD[SV] = {
    batchMatrix(data, batchSize, numInput, numOut).map { t =>
      val input = t._1
      val label = t._2
      val sumLen = (input.rows + label.rows) * input.cols
      val data = new Array[Double](sumLen)
      var offset = 0
      System.arraycopy(input.toArray, 0, data, offset, input.rows * input.cols)
      offset += input.rows * input.cols

      System.arraycopy(label.toArray, 0, data, offset, label.rows * input.cols)
      offset += label.rows * label.cols
      new SDV(data)
    }
  }

  private[mllib] def initializeLayers(topology: Array[Int]): Array[Layer] = {
    val numLayer = topology.length - 1
    val layers = new Array[Layer](numLayer)
    var nextLayer: Layer = null
    for (layer <- (0 until numLayer).reverse) {
      layers(layer) = if (layer == numLayer - 1) {
        new SoftmaxLayer(topology(layer), topology(layer + 1))
      }
      else {
        new ReLuLayer(topology(layer), topology(layer + 1))
      }
      nextLayer = layers(layer)
      println(s"layers($layer) = ${layers(layer).numIn} * ${layers(layer).numOut}")
    }
    layers
  }

  private[mllib] def l2(
    topology: Array[Int],
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): Double = {
    if (regParam > 0D) {
      var norm = 0D
      val nn = MLP.vectorToStructure(topology, weightsOld)
      val grads = MLP.vectorToStructure(topology, gradient)
      for (layer <- 0 until nn.length) {
        brzAxpy(regParam, nn(layer)._1, grads(layer)._1)
        for (i <- 0 until nn(layer)._1.rows) {
          for (j <- 0 until nn(layer)._1.cols) {
            norm += math.pow(nn(layer)._1(i, j), 2)
          }
        }
      }
      // TODO: why?
      0.5 * regParam * norm * norm
    } else {
      regParam
    }
  }
}

private[mllib] class MLPGradient(
  val nn: MLP,
  batchSize: Int = 1) extends Gradient {

  val numIn = nn.numInput
  val numLabel = nn.numOut

  override def compute(data: SV, label: Double, weights: SV): (SV, Double) = {

    var input: BDM[Double] = null
    var label: BDM[Double] = null
    val batchedData = data.toArray
    if (data.size != numIn + numLabel) {
      val numCol = data.size / (numIn + numLabel)
      input = new BDM[Double](numIn, numCol, batchedData)
      label = new BDM[Double](numLabel, numCol, batchedData, numIn * numCol)
    }
    else {
      input = new BDV(batchedData, 0, 1, numIn).toDenseMatrix.t
      label = new BDV(batchedData, numIn, 1, numLabel).toDenseMatrix.t
    }

    MLP.fromVector(nn, weights)
    var (grads, error, numCol) = nn.learn(input, label)
    if (numCol != 1D) {
      val scale = 1D / numCol
      grads.foreach { t =>
        t._1 :*= scale
        t._2 :*= scale
      }
      error *= scale
    }

    (MLP.structureToVector(grads), error)
  }

  override def compute(
    data: SV,
    label: Double,
    weights: SV,
    cumGradient: SV): Double = {
    val (grad, err) = compute(data, label, weights)
    cumGradient.toBreeze += grad.toBreeze
    err
  }

}

private[mllib] class MLPUpdater(val topology: Array[Int]) extends Updater {
  override def compute(
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): (SV, Double) = {
    val newReg = MLP.l2(topology, weightsOld, gradient, stepSize, iter, regParam)
    BLAS.axpy(-stepSize, gradient, weightsOld)
    (weightsOld, newReg)
  }
}

@Experimental
private[mllib] class MLPAdaGradUpdater(
  val topology: Array[Int],
  rho: Double = 0,
  epsilon: Double = 1e-2,
  gamma: Double = 1e-1,
  momentum: Double = 0) extends AdaGradUpdater(rho, epsilon, gamma, momentum) {
  override protected def l2(
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): Double = {
    MLP.l2(topology, weightsOld, gradient, stepSize, iter, regParam)
  }
}

private[mllib] class MLPAdaDeltaUpdater(
  val topology: Array[Int],
  rho: Double = 0.99,
  epsilon: Double = 1e-8,
  momentum: Double = 0.9) extends AdaDeltaUpdater(rho, epsilon, momentum) {
  override protected def l2(
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): Double = {
    MLP.l2(topology, weightsOld, gradient, stepSize, iter, regParam)
  }
}

private[mllib] class MLPMomentumUpdater(
  val topology: Array[Int],
  momentum: Double = 0.9) extends MomentumUpdater(momentum) {
  override protected def l2(
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): Double = {
    MLP.l2(topology, weightsOld, gradient, stepSize, iter, regParam)
  }
}
