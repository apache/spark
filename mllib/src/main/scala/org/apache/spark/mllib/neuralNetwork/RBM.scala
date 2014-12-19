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

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum => brzSum, axpy => brzAxpy}
import org.apache.commons.math3.random.JDKRandomGenerator

import org.apache.spark.annotation.Experimental
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Vector => SV, DenseVector => SDV}
import org.apache.spark.mllib.optimization.{Gradient, GradientDescent}
import org.apache.spark.util.Utils
import org.apache.spark.rdd.RDD

class RBM(
  val weight: BDM[Double],
  val visibleBias: BDV[Double],
  val hiddenBias: BDV[Double],
  val dropoutRate: Double) extends Logging with Serializable {

  def this(
    numIn: Int,
    numOut: Int,
    dropoutRate: Double = 0.5D) {
    this(Layer.initUniformDistWeight(numIn, numOut, 0D, 0.001),
      Layer.initializeBias(numIn),
      Layer.initializeBias(numOut),
      dropoutRate)
  }

  require(dropoutRate >= 0 && dropoutRate < 1)
  protected lazy val rand = new JDKRandomGenerator()

  setSeed(Utils.random.nextInt())

  def visibleLayer: Layer = {
    new ReLuLayer(weight.t, visibleBias)
  }

  def hiddenLayer: Layer = {
    new ReLuLayer(weight, hiddenBias)
  }

  def setSeed(seed: Long): Unit = {
    rand.setSeed(seed)
    visibleLayer.setSeed(rand.nextInt())
    hiddenLayer.setSeed(rand.nextInt())
  }

  def cdK: Int = 1

  def numOut: Int = weight.rows

  def numIn: Int = weight.cols

  def forward(visible: BDM[Double]): BDM[Double] = {
    val hidden = activateHidden(visible)
    if (dropoutRate > 0) {
      hidden :*= (1 - dropoutRate)
    }
    hidden
  }

  protected def activateHidden(visible: BDM[Double]): BDM[Double] = {
    assert(visible.rows == weight.cols)
    hiddenLayer.forward(visible)
  }

  protected def sampleHidden(hiddenMean: BDM[Double]): BDM[Double] = {
    hiddenLayer.sample(hiddenMean)
  }

  protected def sampleVisible(visibleMean: BDM[Double]): BDM[Double] = {
    visibleLayer.sample(visibleMean)
  }

  protected def activateVisible(hidden: BDM[Double]): BDM[Double] = {
    assert(hidden.rows == weight.rows)
    visibleLayer.forward(hidden)
  }

  protected def dropOutMask(cols: Int): BDM[Double] = {
    val mask = new BDM[Double](numOut, cols)
    for (i <- 0 until numOut) {
      for (j <- 0 until cols) {
        mask(i, j) = if (rand.nextDouble() > dropoutRate) 1D else 0D
      }
    }
    mask
  }

  protected def meanSquaredError(visible: BDM[Double], out: BDM[Double]): Double = {
    assert(visible.rows == out.rows)
    assert(visible.cols == out.cols)
    var diff = 0D
    for (i <- 0 until out.rows) {
      for (j <- 0 until out.cols) {
        diff += math.pow(visible(i, j) - out(i, j), 2)
      }
    }
    diff / out.rows
  }

  def learn(input: BDM[Double]): (BDM[Double], BDV[Double], BDV[Double], Double, Double) = {
    val batchSize = input.cols
    val mask: BDM[Double] = if (dropoutRate > 0) {
      this.dropOutMask(input.cols)
    } else {
      null
    }

    val h1Mean = activateHidden(input)
    val h1Sample = sampleHidden(h1Mean)

    var vKMean: BDM[Double] = null
    var vKSample: BDM[Double] = null
    var hKMean: BDM[Double] = null
    var hKSample: BDM[Double] = h1Sample
    if (dropoutRate > 0) {
      hKSample :*= mask
    }

    for (i <- 0 until cdK) {
      vKMean = activateVisible(hKSample)
      hKMean = activateHidden(vKMean)
      hKSample = sampleHidden(hKMean)
      if (dropoutRate > 0) {
        hKSample :*= mask
      }
    }

    val gradWeight: BDM[Double] = hKMean * vKMean.t
    gradWeight :-= h1Mean * input.t

    val diffVisible = vKMean - input
    val gradVisibleBias = BDV.zeros[Double](numIn)
    for (i <- 0 until batchSize) {
      gradVisibleBias :+= diffVisible(::, i)
    }

    val diffHidden = hKMean - h1Mean
    val gradHiddenBias = BDV.zeros[Double](numOut)
    for (i <- 0 until batchSize) {
      gradHiddenBias :+= diffHidden(::, i)
    }

    val mse = meanSquaredError(input, vKMean)
    (gradWeight, gradVisibleBias, gradHiddenBias, mse, batchSize.toDouble)
  }

}

object RBM extends Logging {
  def train(
    data: RDD[SV],
    batchSize: Int,
    numIteration: Int,
    numVisible: Int,
    numHidden: Int,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): RBM = {
    val rbm = new RBM(numVisible, numHidden)
    train(data, batchSize, numIteration, rbm,
      fraction, learningRate, weightCost)
  }

  def train(
    data: RDD[SV],
    batchSize: Int,
    numIteration: Int,
    rbm: RBM,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): RBM = {
    runSGD(data, rbm, batchSize, numIteration,
      fraction, learningRate, weightCost)
  }

  def runSGD(
    trainingRDD: RDD[SV],
    batchSize: Int,
    numVisible: Int,
    numHidden: Int,
    maxNumIterations: Int,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): RBM = {
    val rbm = new RBM(numVisible, numHidden)
    runSGD(trainingRDD, rbm, batchSize, maxNumIterations,
      fraction, learningRate, weightCost)
  }

  def runSGD(
    data: RDD[SV],
    rbm: RBM,
    batchSize: Int,
    maxNumIterations: Int,
    fraction: Double,
    learningRate: Double,
    weightCost: Double): RBM = {
    runSGD(data, rbm, batchSize, maxNumIterations,
      fraction, learningRate, weightCost, 1 - 1e-2, 1e-8)
  }

  def runSGD(
    data: RDD[SV],
    rbm: RBM,
    batchSize: Int,
    maxNumIterations: Int,
    fraction: Double,
    learningRate: Double,
    weightCost: Double,
    rho: Double,
    epsilon: Double): RBM = {
    val numVisible = rbm.numIn
    val numHidden = rbm.numOut
    val gradient = new RBMGradient(rbm.numIn, rbm.numOut, rbm.dropoutRate)
    val updater = new RBMAdaDeltaUpdater(numVisible, numHidden, rho, epsilon)
    val optimizer = new GradientDescent(gradient, updater).
      setMiniBatchFraction(fraction).
      setNumIterations(maxNumIterations).
      setRegParam(weightCost).
      setStepSize(learningRate)
    val trainingRDD = if (batchSize > 1) {
      batchVector(data, batchSize, numVisible).map(t => (0D, t))
    } else {
      data.map(t => (0D, t))
    }
    // TODO: the related jira SPARK-4526
    trainingRDD.cache().setName("RBM-dataBatch")
    val weights = optimizer.optimize(trainingRDD, toVector(rbm))
    trainingRDD.unpersist()
    fromVector(rbm, weights)
    rbm
  }

  private[mllib] def batchMatrix(
    data: RDD[SV],
    batchSize: Int,
    numVisible: Int): RDD[BDM[Double]] = {
    val dataBatch = data.mapPartitions { itr =>
      itr.grouped(batchSize).map { seq =>
        val batch = BDM.zeros[Double](numVisible, seq.size)
        seq.zipWithIndex.foreach { case (v, i) =>
          batch(::, i) := v.toBreeze
        }
        batch
      }
    }
    dataBatch
  }

  private[mllib] def batchVector(
    data: RDD[SV],
    batchSize: Int,
    numVisible: Int): RDD[SV] = {
    batchMatrix(data, batchSize, numVisible).map { t =>
      new SDV(t.toArray)
    }
  }

  private[mllib] def fromVector(rbm: RBM, weights: SV): Unit = {
    val (weight, visibleBias, hiddenBias) = vectorToStructure(rbm.numIn, rbm.numOut, weights)
    rbm.weight := weight
    rbm.visibleBias := visibleBias
    rbm.hiddenBias := hiddenBias
  }

  private[mllib] def toVector(rbm: RBM): SV = {
    structureToVector(rbm.weight, rbm.visibleBias, rbm.hiddenBias)
  }

  private[mllib] def structureToVector(
    weight: BDM[Double],
    visibleBias: BDV[Double],
    hiddenBias: BDV[Double]): SV = {
    val numVisible = visibleBias.length
    val numHidden = hiddenBias.length
    val sumLen = numHidden * numVisible + numVisible + numHidden
    val data = new Array[Double](sumLen)
    var offset = 0

    System.arraycopy(weight.toArray, 0, data, offset, numHidden * numVisible)
    offset += numHidden * numVisible

    System.arraycopy(visibleBias.toArray, 0, data, offset, numVisible)
    offset += numVisible

    System.arraycopy(hiddenBias.toArray, 0, data, offset, numHidden)
    offset += numHidden

    new SDV(data)
  }

  private[mllib] def vectorToStructure(
    numVisible: Int,
    numHidden: Int,
    weights: SV): (BDM[Double], BDV[Double], BDV[Double]) = {
    val data = weights.toArray
    var offset = 0

    val weight = new BDM[Double](numHidden, numVisible, data, offset)
    offset += numHidden * numVisible

    val visibleBias = new BDV[Double](data, offset, 1, numVisible)
    offset += numVisible

    val hiddenBias = new BDV[Double](data, offset, 1, numHidden)
    offset += numHidden

    (weight, visibleBias, hiddenBias)

  }

  private[mllib] def l2(
    numVisible: Int,
    numHidden: Int,
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): Double = {
    if (regParam > 0D) {
      val (weight, _, _) = RBM.vectorToStructure(numVisible, numHidden, weightsOld)
      val (gradWeight, _, _) =
        RBM.vectorToStructure(numVisible, numHidden, gradient)
      brzAxpy(regParam, weight, gradWeight)
      var norm = 0D
      for (i <- 0 until weight.rows) {
        for (j <- 0 until weight.cols) {
          norm += math.pow(weight(i, j), 2)
        }
      }
      0.5 * regParam * norm * norm
    }
    regParam
  }
}

private[mllib] class RBMGradient(
  val numIn: Int,
  val numOut: Int,
  val dropoutRate: Double) extends Gradient {
  override def compute(data: SV, label: Double, weights: SV): (SV, Double) = {
    val (weight, visibleBias, hiddenBias) = RBM.vectorToStructure(numIn, numOut, weights)
    val rbm = new RBM(weight, visibleBias, hiddenBias, dropoutRate)

    val input = if (data.size > numIn) {
      val numCol = data.size / numIn
      new BDM[Double](numIn, numCol, data.toArray)
    }
    else {
      new BDV(data.toArray, 0, 1, numIn).toDenseMatrix.t
    }
    RBM.fromVector(rbm, weights)

    var (gradWeight, gradVisibleBias, gradHiddenBias, error, numCol) = rbm.learn(input)
    if (numCol != 1D) {
      val scale = 1D / numCol
      gradWeight :*= scale
      gradVisibleBias :*= scale
      gradHiddenBias :*= scale
      error *= scale
    }

    (RBM.structureToVector(gradWeight, gradVisibleBias, gradHiddenBias), error)
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

@Experimental
private[mllib] class RBMAdaGradUpdater(
  val numIn: Int,
  val numOut: Int,
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
    RBM.l2(numIn, numOut, weightsOld, gradient, stepSize, iter, regParam)
  }
}

private[mllib] class RBMAdaDeltaUpdater(
  val numIn: Int,
  val numOut: Int,
  rho: Double = 0.99,
  epsilon: Double = 1e-8,
  momentum: Double = 0.9) extends AdaDeltaUpdater(rho, epsilon, momentum) {

  override protected def l2(
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): Double = {
    RBM.l2(numIn, numOut, weightsOld, gradient, stepSize, iter, regParam)
  }
}

private[mllib] class RBMMomentumUpdater(
  val numIn: Int,
  val numOut: Int,
  momentum: Double = 0.9) extends MomentumUpdater(momentum) {
  override protected def l2(
    weightsOld: SV,
    gradient: SV,
    stepSize: Double,
    iter: Int,
    regParam: Double): Double = {
    RBM.l2(numIn, numOut, weightsOld, gradient, stepSize, iter, regParam)
  }
}
