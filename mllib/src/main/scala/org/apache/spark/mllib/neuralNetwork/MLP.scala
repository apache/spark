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

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM, Matrix => BM,
axpy => brzAxpy, argmax => brzArgMax, max => brzMax, sum => brzSum, norm => brzNorm}

import org.apache.spark.annotation.Experimental
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{DenseMatrix => SDM, SparseMatrix => SSM, Matrix => SM,
SparseVector => SSV, DenseVector => SDV, Vector => SV, Vectors, Matrices, BLAS}
import org.apache.spark.mllib.optimization.{Gradient, Updater, LBFGS, GradientDescent}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

class MLP(
  val innerLayers: Array[Layer],
  val dropout: Array[Double]) extends Logging with Serializable {
  def this(topology: Array[Int],
    inputDropout: Double,
    hiddenDropout: Double) {
    this(MLP.initLayers(topology),
      MLP.initDropout(topology.length - 1, Array(hiddenDropout, inputDropout)))
  }

  def this(topology: Array[Int]) {
    this(MLP.initLayers(topology), MLP.initDropout(topology.length - 1, Array(0.2, 0.5)))
  }

  require(innerLayers.length > 0)
  require(dropout.forall(t => t >= 0 && t < 1))
  require(dropout.last == 0D)
  require(innerLayers.length == dropout.length)

  protected[neuralNetwork] lazy val rand: Random = new Random()

  def topology: Array[Int] = {
    val topology = new Array[Int](numLayer + 1)
    topology(0) = numInput
    for (i <- 1 to numLayer) {
      topology(i) = innerLayers(i - 1).numOut
    }
    topology
  }

  def numLayer = innerLayers.length

  def numInput = innerLayers.head.numIn

  def numOut = innerLayers.last.numOut

  def predict(x: SM): SM = {
    var output = x
    for (layer <- 0 until numLayer) {
      output = innerLayers(layer).forward(output)
      val dropoutRate = dropout(layer)
      if (dropoutRate > 0D) {
        output.toBreeze :*= (1D - dropoutRate)
      }
    }
    output
  }

  protected[neuralNetwork] def learn(x: SM, label: SM): (Array[(SM, SV)], Double, Double) = {
    val batchSize = x.numCols
    val in = new Array[SM](numLayer)
    val out = new Array[SM](numLayer)
    val delta = new Array[SM](numLayer)
    val grads = new Array[(SM, SV)](numLayer)
    val dropOutMasks: Array[SM] = dropOutMask(batchSize)

    for (layer <- 0 until numLayer) {
      val input = if (layer == 0) {
        x
      } else {
        out(layer - 1)
      }
      in(layer) = input

      val output = innerLayers(layer).forward(input)
      if (dropOutMasks(layer) != null) {
        assert(output.numRows == dropOutMasks(layer).numRows)
        output.toBreeze :*= dropOutMasks(layer).toBreeze
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
      if (dropOutMasks(layer) != null) {
        delta(layer).toBreeze :*= dropOutMasks(layer).toBreeze
      }
      grads(layer) = innerLayers(layer).backward(input, delta(layer))
    }

    val cost = if (innerLayers.last.layerType == "softMax") {
      Layer.crossEntropy(out.last, label)
    } else {
      Layer.meanSquaredError(out.last, label)
    }
    (grads, cost, batchSize.toDouble)
  }

  protected[neuralNetwork] def dropOutMask(cols: Int): Array[SM] = {
    val masks = new Array[SM](numLayer)
    for (layer <- 0 until numLayer) {
      val dropoutRate = dropout(layer)
      masks(layer) = if (dropoutRate > 0) {
        val rows = innerLayers(layer).numOut
        val mask = SDM.zeros(rows, cols)
        for (i <- 0 until rows) {
          for (j <- 0 until cols) {
            mask(i, j) = if (rand.nextDouble() > dropoutRate) 1D else 0D
          }
        }
        mask
      } else {
        null
      }
    }
    masks
  }

  protected[neuralNetwork] def assign(newNN: MLP): MLP = {
    innerLayers.zip(newNN.innerLayers).foreach { case (oldLayer, newLayer) =>
      oldLayer.weight.toBreeze := newLayer.weight.toBreeze
      oldLayer.bias.toBreeze := newLayer.bias.toBreeze
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
    mlp: MLP,
    batchSize: Int,
    maxNumIterations: Int,
    fraction: Double,
    learningRate: Double,
    weightCost: Double,
    rho: Double,
    epsilon: Double): MLP = {
    val gradient = new MLPGradient(
      mlp.topology,
      mlp.innerLayers.map(_.layerType),
      mlp.dropout,
      batchSize)
    val updater = new MLPAdaDeltaUpdater(mlp.topology, rho, epsilon)
    val optimizer = new GradientDescent(gradient, updater).
      setMiniBatchFraction(fraction).
      setNumIterations(maxNumIterations).
      setRegParam(weightCost).
      setStepSize(learningRate)

    val trainingRDD = toTrainingRDD(data)
    trainingRDD.persist(StorageLevel.MEMORY_AND_DISK).setName("MLP-dataBatch")
    val weights = optimizer.optimize(trainingRDD, toVector(mlp))
    trainingRDD.unpersist()
    fromVector(mlp, weights)
    mlp
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
    val gradient = new MLPGradient(
      nn.topology,
      nn.innerLayers.map(_.layerType),
      nn.dropout,
      batchSize)
    val updater = new MLPUpdater(nn.topology)
    val optimizer = new LBFGS(gradient, updater).
      setConvergenceTol(convergenceTol).
      setNumIterations(maxNumIterations).
      setRegParam(weightCost)

    val trainingRDD = toTrainingRDD(data)
    trainingRDD.persist(StorageLevel.MEMORY_AND_DISK).setName("MLP-dataBatch")
    val weights = optimizer.optimize(trainingRDD, toVector(nn))
    trainingRDD.unpersist()
    fromVector(nn, weights)
    nn
  }

  private[mllib] def fromVector(mlp: MLP, weights: SV): Unit = {
    val structure = vectorToStructure(mlp.topology, weights)
    val layers: Array[Layer] = mlp.innerLayers
    for (i <- 0 until structure.length) {
      val (weight, bias) = structure(i)
      val layer = layers(i)
      layer.weight.toBreeze := weight.toBreeze
      layer.bias.toBreeze := bias.toBreeze

    }
  }

  private[mllib] def toVector(nn: MLP): SV = {
    structureToVector(nn.innerLayers.map(l => (l.weight, l.bias)))
  }

  private[mllib] def structureToVector(grads: Array[(SM, SV)]): SV = {
    val numLayer = grads.length
    val sumLen = grads.map(m => m._1.numRows * m._1.numCols + m._2.size).sum
    val data = new Array[Double](sumLen)
    var offset = 0
    for (l <- 0 until numLayer) {
      val (gradWeight, gradBias) = grads(l)
      val numIn = gradWeight.numCols
      val numOut = gradWeight.numRows
      System.arraycopy(gradWeight.toArray, 0, data, offset, numOut * numIn)
      offset += numIn * numOut
      System.arraycopy(gradBias.toArray, 0, data, offset, numOut)
      offset += numOut
    }
    new SDV(data)
  }

  private[mllib] def vectorToStructure(
    topology: Array[Int],
    weights: SV): Array[(SM, SV)] = {
    val data = weights.toArray
    val numLayer = topology.length - 1
    val grads = new Array[(SM, SV)](numLayer)
    var offset = 0
    for (layer <- 0 until numLayer) {
      val numIn = topology(layer)
      val numOut = topology(layer + 1)

      val weight = SDM.zeros(numOut, numIn)
      System.arraycopy(data, offset, weight.values, 0, numIn * numOut)
      offset += numIn * numOut

      val bias = Vectors.zeros(numOut).asInstanceOf[SDV]
      System.arraycopy(data, offset, bias.values, 0, numOut)
      offset += numOut
      grads(layer) = (weight, bias)
    }
    grads
  }

  private[mllib] def vectorToBreeze(
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
      val h = nn.predict(Matrices.fromBreeze(x)).toBreeze.toDenseMatrix
      (0 until h.cols).map(i => {
        if (brzArgMax(y(::, i)) == brzArgMax(h(::, i))) 0D else 1D
      }).sum
    }.sum
    sumError / count
  }

  private def batchMatrix(
    data: RDD[(SV, SV)],
    batchSize: Int,
    numInput: Int,
    numOut: Int): RDD[(BDM[Double], BDM[Double])] = {
    val dataBatch = data.mapPartitions { itr =>
      itr.grouped(batchSize).map { seq =>
        val x = BDM.zeros[Double](numInput, seq.size)
        val y = BDM.zeros[Double](numOut, seq.size)
        seq.zipWithIndex.foreach { case (v, i) =>
          x(::, i) := v._1.toBreeze
          y(::, i) := v._2.toBreeze
        }
        (x, y)
      }
    }
    dataBatch
  }

  private def toTrainingRDD(data: RDD[(SV, SV)]): RDD[(Double, SV)] = {
    data.map { case (input, label) =>
      if (input.isInstanceOf[SSV]) {
        val si = input.toBreeze.asInstanceOf[BSV[Double]]
        val sl = BSV.zeros[Double](label.size)
        sl := label.toBreeze
        (0D, Vectors.fromBreeze(BSV.vertcat(si, sl)))
      } else {
        val di = input.toBreeze.toDenseVector
        val dl = label.toBreeze.toDenseVector
        (0D, Vectors.fromBreeze(BDV.vertcat(di, dl)))
      }
    }
  }

  private def initLayers(topology: Array[Int]): Array[Layer] = {
    val numLayer = topology.length - 1
    val layers = new Array[Layer](numLayer)
    for (layer <- (0 until numLayer).reverse) {
      val numIn = topology(layer)
      val numOut = topology(layer + 1)
      layers(layer) = if (layer == numLayer - 1) {
        new SoftMaxLayer(numIn, numOut)
      }
      else {
        new ReLuLayer(numIn, numOut)
      }
      println(s"layers($layer) = ${numIn} * ${numOut}")
    }
    layers
  }

  private def initDropout(numLayer: Int, d: Array[Double]): Array[Double] = {
    require(d.length > 0)
    val dropout = new Array[Double](numLayer)
    for (layer <- 0 until numLayer) {
      dropout(layer) = if (layer == numLayer - 1) {
        0D
      } else if (layer < d.length) {
        d(layer)
      } else {
        d.last
      }
    }
    dropout
  }

  private[mllib] def initLayers(
    params: Array[(SM, SV)],
    layerTypes: Array[String]): Array[Layer] = {
    val numLayer = params.length
    val layers = new Array[Layer](numLayer)
    for (layer <- 0 until numLayer) {
      val (weight, bias) = params(layer)
      layers(layer) = Layer.initializeLayer(weight, bias, layerTypes(layer))
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
      val nn = MLP.vectorToBreeze(topology, weightsOld)
      val grads = MLP.vectorToBreeze(topology, gradient)
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
  val topology: Array[Int],
  val layerTypes: Array[String],
  val dropoutRate: Array[Double],
  val batchSize: Int) extends Gradient {

  override def compute(data: SV, label: Double, weights: SV): (SV, Double) = {
    val layers = MLP.initLayers(MLP.vectorToStructure(topology, weights), layerTypes)
    val mlp = new MLP(layers, dropoutRate)
    val numIn = mlp.numInput
    val numLabel = mlp.numOut
    assert(data.size == numIn + numLabel)
    val batchedData = data.toArray
    val input = new SDM(numIn, 1, batchedData.slice(0, numIn))
    val label = new SDM(numLabel, 1, batchedData.slice(numIn, numIn + numLabel))
    val (grads, error, _) = mlp.learn(input, label)
    (MLP.structureToVector(grads), error)
  }

  override def compute(
    data: SV,
    label: Double,
    weights: SV,
    cumGradient: SV): Double = {
    val (grad, err) = compute(data, label, weights)
    BLAS.axpy(1, grad, cumGradient)
    err
  }

  override def compute(
    iter: Iterator[(Double, SV)],
    weights: SV,
    cumGradient: SV): (Long, Double) = {
    val layers = MLP.initLayers(MLP.vectorToStructure(topology, weights), layerTypes)
    val mlp = new MLP(layers, dropoutRate)
    val numIn = mlp.numInput
    val numLabel = mlp.numOut
    var loss = 0D
    var count = 0L
    iter.map(_._2).grouped(batchSize).foreach { seq =>
      val numCol = seq.size
      val input = BDM.zeros[Double](numIn, numCol)
      val label = BDM.zeros[Double](numLabel, numCol)

      seq.zipWithIndex.foreach { case (data, index) =>
        assert(data.size == numIn + numLabel)
        val brzVector = data.toBreeze
        input(::, index) := brzVector(0 until numIn)
        label(::, index) := brzVector(numIn until numIn + numLabel)
      }
      val (grads, error, _) = mlp.learn(Matrices.fromBreeze(input), Matrices.fromBreeze(label))
      BLAS.axpy(1, MLP.structureToVector(grads), cumGradient)
      loss += error
      count += numCol
    }
    (count, loss)
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
  rho: Double = 0D,
  epsilon: Double = 1e-2,
  gamma: Double = 1e-1,
  momentum: Double = 0D) extends AdaGradUpdater(rho, epsilon, gamma, momentum) {
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
