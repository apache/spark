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

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, Matrix => BM,
sum => brzSum, Axis => BrzAxis, axpy => brzAxpy, norm => brzNorm}
import org.apache.commons.math3.random.JDKRandomGenerator

import org.apache.spark.annotation.Experimental
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{DenseMatrix => SDM, SparseMatrix => SSM, Matrix => SM,
SparseVector => SSV, DenseVector => SDV, Vector => SV, Vectors, Matrices, BLAS}
import org.apache.spark.mllib.optimization.{Gradient, GradientDescent}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.rdd.RDD

class RBM(
  val weight: SM,
  val visibleBias: SV,
  val hiddenBias: SV,
  val dropoutRate: Double) extends Logging with Serializable {

  def this(
    numIn: Int,
    numOut: Int,
    dropout: Double = 0.5D) {
    this(NNUtil.initUniformDistWeight(numIn, numOut, 0D, 0.01),
      NNUtil.initializeBias(numIn),
      NNUtil.initializeBias(numOut),
      dropout)
  }

  require(dropoutRate >= 0 && dropoutRate < 1)
  @transient protected lazy val rand: Random = new JDKRandomGenerator()
  @transient protected[mllib] lazy val visibleLayer: Layer = {
    val brzWeight = weight.toBreeze.toDenseMatrix
    new ReLuLayer(new SDM(weight.numCols, weight.numRows, brzWeight.t.toArray), visibleBias)
  }

  @transient protected[mllib] lazy val hiddenLayer: Layer = {
    new ReLuLayer(weight, hiddenBias)
  }

  setSeed(Utils.random.nextInt())


  def setSeed(seed: Long): Unit = {
    rand.setSeed(seed)
    visibleLayer.setSeed(rand.nextInt())
    hiddenLayer.setSeed(rand.nextInt())
  }

  def cdK: Int = 1

  def numOut: Int = weight.numRows

  def numIn: Int = weight.numCols

  def forward(visible: SM): SM = {
    val hidden = activateHidden(visible)
    if (dropoutRate > 0) {
      hidden.toBreeze :*= (1 - dropoutRate)
    }
    hidden
  }

  protected def activateHidden(visible: SM): SM = {
    require(visible.numRows == weight.numCols)
    hiddenLayer.forward(visible)
  }

  protected def sampleHidden(hiddenMean: SM): SM = {
    hiddenLayer.sample(hiddenMean)
  }

  protected def sampleVisible(visibleMean: SM): SM = {
    visibleLayer.sample(visibleMean)
  }

  protected def activateVisible(hidden: SM): SM = {
    require(hidden.numRows == weight.numRows)
    visibleLayer.forward(hidden)
  }

  protected def dropOutMask(cols: Int): SM = {
    val mask = SDM.zeros(numOut, cols)
    for (i <- 0 until numOut) {
      for (j <- 0 until cols) {
        mask(i, j) = if (rand.nextDouble() > dropoutRate) 1D else 0D
      }
    }
    mask
  }

  def learn(input: SM): (SM, SV, SV, Double, Double) = {
    val batchSize = input.numCols
    val mask: SM = if (dropoutRate > 0) {
      this.dropOutMask(batchSize)
    } else {
      null
    }

    val h1Mean = activateHidden(input)
    val h1Sample = sampleHidden(h1Mean)

    var vKMean: SM = null
    var vKSample: SM = null
    var hKMean: SM = null
    var hKSample: SM = h1Sample
    if (dropoutRate > 0) {
      hKSample.toBreeze :*= mask.toBreeze
    }

    for (i <- 0 until cdK) {
      vKMean = activateVisible(hKSample)
      hKMean = activateHidden(vKMean)
      hKSample = sampleHidden(hKMean)
      if (dropoutRate > 0) {
        hKSample.toBreeze :*= mask.toBreeze
      }
    }

    val gradWeight = SDM.zeros(weight.numRows, weight.numCols)
    BLAS.gemm(false, true, 1.0, hKMean,
      new SDM(vKMean.numRows, vKMean.numCols, vKMean.toArray), 1.0, gradWeight)
    BLAS.gemm(false, true, -1.0, h1Mean,
      new SDM(input.numRows, input.numCols, input.toArray), 1.0, gradWeight)

    val diffVisible: BM[Double] = vKMean.toBreeze - input.toBreeze
    val gradVisibleBias = Vectors.fromBreeze(brzSum(diffVisible.toDenseMatrix, BrzAxis._1))


    val diffHidden: BM[Double] = hKMean.toBreeze - h1Mean.toBreeze
    val gradHiddenBias = Vectors.fromBreeze(brzSum(diffHidden.toDenseMatrix, BrzAxis._1))

    val mse = NNUtil.meanSquaredError(input, vKMean)
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
    val gradient = new RBMGradient(rbm.numIn, rbm.numOut, rbm.dropoutRate, batchSize)
    val updater = new RBMAdaDeltaUpdater(numVisible, numHidden, rho, epsilon)
    val optimizer = new GradientDescent(gradient, updater).
      setMiniBatchFraction(fraction).
      setNumIterations(maxNumIterations).
      setRegParam(weightCost).
      setStepSize(learningRate)
    val trainingRDD = data.map(t => (0D, t))
    // TODO: the related jira SPARK-4526
    trainingRDD.persist(StorageLevel.MEMORY_AND_DISK).setName("RBM-dataBatch")
    val weights = optimizer.optimize(trainingRDD, toVector(rbm))
    trainingRDD.unpersist()
    fromVector(rbm, weights)
    rbm
  }

  private[mllib] def fromVector(rbm: RBM, weights: SV): Unit = {
    val (weight, visibleBias, hiddenBias) = vectorToStructure(rbm.numIn, rbm.numOut, weights)
    rbm.weight.toBreeze := weight.toBreeze
    rbm.visibleBias.toBreeze := visibleBias.toBreeze
    rbm.hiddenBias.toBreeze := hiddenBias.toBreeze
  }

  private[mllib] def toVector(rbm: RBM): SV = {
    structureToVector(rbm.weight, rbm.visibleBias, rbm.hiddenBias)
  }

  private[mllib] def structureToVector(
    weight: SM,
    visibleBias: SV,
    hiddenBias: SV): SV = {
    val numVisible = visibleBias.size
    val numHidden = hiddenBias.size
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
    weights: SV): (SM, SV, SV) = {
    val data = weights.toArray
    var offset = 0

    val weight = SDM.zeros(numHidden, numVisible)
    System.arraycopy(data, offset, weight.values, 0, numHidden * numVisible)
    offset += numHidden * numVisible

    val visibleBias = Vectors.zeros(numVisible).asInstanceOf[SDV]
    System.arraycopy(data, offset, visibleBias.values, 0, numVisible)
    offset += numVisible

    val hiddenBias = Vectors.zeros(numHidden).asInstanceOf[SDV]
    System.arraycopy(data, offset, hiddenBias.values, 0, numHidden)
    offset += numHidden

    (weight, visibleBias, hiddenBias)
  }

  private[mllib] def vectorToBreeze(
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
      val (weight, _, _) = RBM.vectorToBreeze(numVisible, numHidden, weightsOld)
      val (gradWeight, _, _) =
        RBM.vectorToBreeze(numVisible, numHidden, gradient)
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
  val dropoutRate: Double,
  val batchSize: Int) extends Gradient {
  override def compute(data: SV, label: Double, weights: SV): (SV, Double) = {
    val (weight, visibleBias, hiddenBias) = RBM.vectorToStructure(numIn, numOut, weights)
    val rbm = new RBM(weight, visibleBias, hiddenBias, dropoutRate)
    val input = new SDM(numIn, 1, data.toArray)
    val (gradWeight, gradVisibleBias, gradHiddenBias, error, _) = rbm.learn(input)
    (RBM.structureToVector(gradWeight, gradVisibleBias, gradHiddenBias), error)
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
    val (weight, visibleBias, hiddenBias) = RBM.vectorToStructure(numIn, numOut, weights)
    val rbm = new RBM(weight, visibleBias, hiddenBias, dropoutRate)
    var loss = 0D
    var count = 0L
    iter.map(_._2).grouped(batchSize).foreach { seq =>
      val numCol = seq.size
      val input: BDM[Double] = BDM.zeros(numIn, numCol)
      seq.zipWithIndex.foreach { case (data, index) =>
        assert(data.size == numIn)
        input(::, index) := data.toBreeze
      }
      var (gradWeight, gradVisibleBias,
      gradHiddenBias, error, _) = rbm.learn(Matrices.fromBreeze(input))
      val w = RBM.structureToVector(gradWeight, gradVisibleBias, gradHiddenBias)
      BLAS.axpy(1, w, cumGradient)
      loss += error
      count += numCol
    }
    (count, loss)
  }
}

@Experimental
private[mllib] class RBMAdaGradUpdater(
  val numIn: Int,
  val numOut: Int,
  rho: Double = 0,
  epsilon: Double = 1e-2,
  gamma: Double = 1e-1,
  momentum: Double = 0.9) extends AdaGradUpdater(rho, epsilon, gamma, momentum) {

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
