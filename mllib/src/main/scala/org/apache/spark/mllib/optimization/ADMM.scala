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

package org.apache.spark.mllib.optimization

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{Vector => BV}
import breeze.optimize.{LBFGS => BreezeLBFGS, CachedDiffFunction, DiffFunction}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vectors, Vector, BLAS}

/**
 * :: DeveloperApi ::
 * The alternating direction method of multipliers (ADMM) is an algorithm that solves
 * convex optimization problems by breaking them into smaller pieces, each of which
 * are then easier to handle. It has recently found wide application in a number of areas.
 *
 * Class used to solove an optimization problem using ADMM. This is an implementation of
 * consensus form ADMM. This class can solve the optimization problems which can be described
 * as follow.
 *                minimize \sum f_i(x_i) + g(z)
 *                      s.t. x_i - z = 0
 * where f_i(x) is the loss function for i_th block of training data, x_i is local variable,
 * z is the global variable. g(z) is an regularization which can be L1 or L2 norm. x_i - z = 0
 * is consistency or consensus constrains.
 * As described in Boyd's paper, x_i, z and scaled dual variable u_i will be updated as follow.
 *              x_i = argmin (f_i(x_i) + 1/2 * rho * (x_i - z + u_i)^2)
 *              u_i = u_i + x_i - z
 *                z = argmin (g(z) + 1/2 * N * rho * (z - w)^2)
 * where N is the number of training data blocks to optimize in parallel, rho is the augmented
 * Lagrangian parameter, w is the average of (x_i + u_i).
 * Reference: [[http://stanford.edu/~boyd/papers/admm_distr_stats.html]]
 * @param gradient Gradient function to be used
 * @param updater Updater to be used to update weights after every iteration
 */
@DeveloperApi
class ADMM(private var gradient: Gradient, private var updater: Updater)
  extends Optimizer with Logging {

  private var primalConvergenceTol = 1e-5
  private var dualConvergenceTol = 1e-5
  private var subModelNum = 20
  private var maxNumIterations = 20
  private var regParam = 0.0
  private var rho = 1.0

  /**
   * Set the maximal number of iterations for ADMM. Default 20.
   */
  def setNumIterations(iterNum: Int): this.type = {
    require(iterNum >= 0, s"Maximum of iterations must be nonnegative but got $iterNum")
    this.maxNumIterations = iterNum
    this
  }

  /**
   * Set the number of sub models to be trained in parallel. Default 20.
   */
  def setNumSubModels(num: Int): this.type = {
    require(num > 0, s"Number of sub models must be nonnegative but got $num")
    this.subModelNum = num
    this
  }

  /**
   * set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    require(regParam >= 0, s"Regularization parameter must be nonnegative but got $regParam")
    this.regParam = regParam
    this
  }

  /**
   * Set rho which is the augmented Lagrangian parameter.
   * kappa = regParam / (rho * numSubModels), if the absolute value of element in model
   * is less than kappa, this element will be assigned to zero.
   * So kappa should be less than 0.01 or 0.001.
   */
  def setRho(rho: Double): this.type = {
    require(rho >= 0, s"Rho must be nonnegative but got $rho")
    this.rho = rho
    this
  }

  /**
   * Set the primal convergence tolerance of iterations.
   *
   */
  def setPrimalTol(tolenrance: Double): this.type = {
    require(tolenrance >= 0,
      s"Primal convergence tolerance must be nonnegative but got $tolenrance")
    this.primalConvergenceTol = tolenrance
    this
  }

  /**
   * Set the dual convergence tolerance of iterations.
   */
  def setDualTol(tolenrance: Double): this.type = {
    require(tolenrance >= 0,
      s"Dual convergence tolerance must be nonnegative but got $tolenrance")
    this.dualConvergenceTol = tolenrance
    this
  }

  /**
   * set the gradient function (of the loss fuction of one single data example)
   * to be used for ADMM
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }

  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  override def optimize(data: RDD[(Double, Vector)], initModel: Vector): Vector = {
    val (weight, _) = ADMM.runADMM(
      data,
      initModel,
      subModelNum,
      gradient,
      updater,
      regParam,
      rho,
      maxNumIterations,
      primalConvergenceTol,
      dualConvergenceTol)
    weight
  }
}

/**
 * :: DeveloperApi ::
 * Top-level method to run ADMM
 */
@DeveloperApi
object ADMM extends Logging {

  private case class State(points: Array[(Double, Vector)])
  private case class Model(x: Vector, u: Vector)
  private val lbfgsMaxIter = 5
  private val numCorrection = 4
  private val lbfgsConvergenceTol = 1e-4

  /**
   * Run Alternating Direction Method of Multipliers(ADMM) in parallel.
   * @param data - Input data for ADMM. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param initModel - Initial model weight.
   * @param numSubModel - Number of training data block to be split for parallel training.
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example).
   * @param updater - Updater function to actually perform a gradient step in a given direction.
   * @param regParam - Regularization parameter.
   * @param rho - Augmented Lagrangian parameter.
   * @param maxNumIterations - Maximal number of iterations that ADMM can be run.
   * @param primalTol - Primal convergence tolerance.
   * @param dualTol - Dual convergence tolerance.
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */
  def runADMM(
      data: RDD[(Double, Vector)],
      initModel: Vector,
      numSubModel: Int,
      gradient: Gradient,
      updater: Updater,
      regParam: Double,
      rho: Double,
      maxNumIterations: Int = 20,
      primalTol: Double = 1e-5,
      dualTol: Double = 1e-5): (Vector, Array[Double]) = {

    var (states, subModels) = initialize(data, initModel, numSubModel)

    var lastZ: Vector = null
    var zModel = initModel
    var zBC = data.context.broadcast(zModel)
    val history = new ArrayBuffer[Double]()
    history += getGlobalLoss(states, zBC, gradient, updater, regParam)

    var numIter = 1
    do {
      logInfo(s"iteration: $numIter begin...")
      val featNum = zModel.size

      val newSubModels = states.join(subModels, subModels.partitioner.get)
        .map { case (id, (state, subModel)) =>
          // update u_i (u_i = u_i + x_i - z)
          val newU = subModel.u
          BLAS.axpy(1, subModel.x, newU)
          BLAS.axpy(-1, zBC.value, newU)
          // update x_i
          val newX = xUpdate(
            numIter,
            state,
            subModel,
            zBC.value,
            gradient,
            rho,
            lbfgsMaxIter,
            numCorrection,
            lbfgsConvergenceTol)
          (id, new Model(newX, newU))
        }.persist(StorageLevel.MEMORY_AND_DISK)
        .partitionBy(new HashPartitioner(numSubModel))

      // use RDD.count to trigger the calculation of newSubModels
      newSubModels.count()
      subModels.unpersist()
      subModels = newSubModels

      // all reduce x + u
      logInfo(s"iteration: $numIter update w")
      val w = subModels.treeAggregate(Vectors.zeros(featNum)) (
        seqOp = (w: Vector, model: (Long, Model)) => {
          val subW = (0 until featNum).map { case index =>
            model._2.x(index) + model._2.u(index)
          }
          Vectors.dense(subW.toArray)
        },
        combOp = (w1: Vector, w2: Vector) => {
          BLAS.axpy(1, w1, w2)
          w2
        }
      )

      // update z
      logInfo(s"iteration: $numIter update z")
      lastZ = zModel
      zModel = zUpdate(numIter, featNum, numSubModel, w, updater, regParam, rho)
      zBC = data.context.broadcast(zModel)

      history += getGlobalLoss(states, zBC, gradient, updater, regParam)
      numIter += 1
    } while (numIter <= maxNumIterations &&
      !isConverged(subModels, lastZ, zBC, rho, primalTol, dualTol))
    logInfo(s"global loss history: ${history.mkString(" ")}")
    (zModel, history.toArray)
  }

  /**
   * Separate the whole data into $subModelNum parts which store as States.
   * and initialize corresponding Models.
   */
  private def initialize(
      data: RDD[(Double, Vector)],
      initModel: Vector,
      numSubModels: Int): (RDD[(Long, State)], RDD[(Long, Model)]) = {

    val zModel = data.context.broadcast(initModel)
    val partitionNum = numSubModels

    val initStates = data.repartition(partitionNum)
      .mapPartitions(iter => List(State(iter.toArray)).toIterator)

    val states = initStates.sortBy(state => state.hashCode())
      .zipWithIndex()
      .map {case (state, id) => (id, state)}
      .persist(StorageLevel.MEMORY_AND_DISK)
      .partitionBy(new HashPartitioner(partitionNum))
    states.count()

    val subModels = states.map { case (id, state) =>
      (id, Model(zModel.value, zModel.value))
    }.persist(StorageLevel.MEMORY_AND_DISK)
      .partitionBy(new HashPartitioner(partitionNum))
    subModels.count()

    (states, subModels)
  }

  /**
   * Optimize x_i with Breeze's L-BFGS,
   * where x_i = argmin (f_i(x_i) + 1/2 * rho * (x_i - z + u_i) * (x_i - z + u_i).
   */
  private def xUpdate(
      numIter: Int,
      state: State,
      subModel: Model,
      z: Vector,
      gradient: Gradient,
      rho: Double,
      lbfgsMaxIter: Int,
      correctionNum: Int,
      convergenceTol: Double): Vector = {
    val costFun = new CostFun(state.points, gradient, subModel.u, z, rho)
    val lbfgs = new BreezeLBFGS[BV[Double]](lbfgsMaxIter, correctionNum, convergenceTol)

    val summaries = lbfgs.iterations(new CachedDiffFunction(costFun), subModel.x.asBreeze)

    val lbfgsHistory = new ArrayBuffer[Double]()
    var summary = summaries.next()
    while (summaries.hasNext) {
      lbfgsHistory += summary.value
      summary = summaries.next()
    }
    lbfgsHistory += summary.value
    logInfo(s"iteration: $numIter, lbfgs loss history: ${lbfgsHistory.mkString(" ")}")
    val weight = Vectors.dense(summary.x.toArray)
    weight
  }

  /**
   * Optimize z, where z = argmin (g(z) + 1/2 * N * rho * (z - w)^2)
   * and w = (1 / N) * \sum {x_i + u_i}.
   * If the objective function is L1 regularized problem, it means g(z) = lambda * ||z||,
   * then z = Shrinkage(w, kappa), where kappa = lambda / rho * N.
   * If the objective function is L2 regularized problem, it means g(z) = lambda * ||z||^2,
   * then z = (N * rho) / (2 * lambda + N * rho) * w
   */
  private def zUpdate(
      numIter: Int,
      featNum: Int,
      modelNum: Int,
      w: Vector,
      updater: Updater,
      regParam: Double,
      rho: Double): Vector = {
    // z update
    val newZ = updater match {
      case u: L1Updater =>
        val kappa = regParam / (rho * modelNum)
        val z = (0 until featNum).map { case i =>
          val wi = w(i) / modelNum
          shrinkage(wi, kappa)
        }
        Vectors.dense(z.toArray)
      case u: SquaredL2Updater =>
        val factor = (modelNum * rho) / (2 * regParam + modelNum * rho)
        val z = Vectors.zeros(featNum)
        BLAS.axpy(factor / modelNum, w, z)
        z
      case u: SimpleUpdater =>
        val z = Vectors.zeros(featNum)
        BLAS.axpy(1.0 / modelNum, w, z)
        z
    }

    logInfo(s"iteration: $numIter model nonzero rate: ${newZ.numNonzeros.toDouble / featNum}")
    newZ
  }

  /**
   * The corresponding proximal operator for the L1 norm is the soft-threshold
   * function. That is, each value is shrunk towards 0 by kappa value.
   *
   * if x < kappa or x > -kappa, return 0
   * if x > kappa, return x - kappa
   * if x < -kappa, return x + kappa
   */
  private def shrinkage(x: Double, kappa: Double): Double = {
    math.max(0, x - kappa) - math.max(0, -x - kappa)
  }

  private def getGlobalLoss(
      states: RDD[(Long, State)],
      zBC: Broadcast[Vector],
      gradient: Gradient,
      updater: Updater,
      regParam: Double): Double = {
    val featNum = zBC.value.size
    val (lossSum, totalCount) = states.map { case (id, state) =>
      val zModel = zBC.value
      val (_, stateLoss) = state.points.aggregate((Vectors.zeros(featNum), 0.0))(
        (c, v) => (c, v) match {
          case ((grad, loss), (label, features)) =>
            val l = gradient.compute(features, label, zModel, grad)
            (grad, loss + l)
        },
        (c1, c2) => (c1, c2) match {
          case ((grad1: Vector, loss1), (grad2: Vector, loss2)) =>
            BLAS.axpy(1.0, grad2, grad1)
            (grad1, loss1 + loss2)
        })
      (stateLoss, state.points.length)
    }.reduce((r1, r2) => (r1._1 + r2._1, r1._2 + r2._2))

    val regVal = updater.compute(zBC.value, Vectors.zeros(featNum), 0, 1, regParam)._2
    lossSum / totalCount + regVal
  }

  /**
   * If both primal residual and dual residual is less than the tolerance,
   * the algorithm is converged.
   * s = rho * (z - z)
   * r = x - z
   */
  private def isConverged(
      models: RDD[(Long, Model)],
      lastZ: Vector,
      zBC: Broadcast[Vector],
      rho: Double,
      primalTol: Double,
      dualTol: Double): Boolean = {

    val featNum = zBC.value.size
    val t = models.map { case (_, subModel) =>
      (0 until featNum).map { case i =>
        math.pow(subModel.x(i) - zBC.value(i), 2)
      }.sum
    }.sum / models.count()

    val zModel = zBC.value.copy
    BLAS.axpy(-1, lastZ, zModel)
    val temp = zModel.toArray.map(fn => fn * fn).sum
    val primalError = rho * temp
    val dualError = math.sqrt(t)
    (primalError <= primalTol) && (dualError <= dualTol)
  }

  /**
   * CostFun implements Breeze's DiffFunction[T], which returns the loss and gradient
   * at a particular point (weights). It's used in Breeze's convex optimization routines.
   */
  private class CostFun(
      data: Array[(Double, Vector)],
      gradient: Gradient,
      u: Vector,
      z: Vector,
      rho: Double) extends DiffFunction[BV[Double]] {

    override def calculate(weights: BV[Double]): (Double, BV[Double]) = {
      val n = weights.length
      val numSamples = data.length
      val mlWeight = Vectors.dense(weights.toArray)

      val (gradSum, lossSum) = data.aggregate((Vectors.zeros(n), 0.0)) (
        (c, v) => (c, v) match { case ((grad, loss), (label, features)) =>
          val l = gradient.compute(features, label, mlWeight, grad)
          (grad, loss + l)
        },
        (c1, c2) => (c1, c2) match { case ((grad1: Vector, loss1), (grad2: Vector, loss2)) =>
          BLAS.axpy(1.0, grad2, grad1)
          (grad1, loss1 + loss2)
        })

      // temp = weights - z + x
      val temp = mlWeight
      BLAS.axpy(-1, z, temp)
      BLAS.axpy(1, u, temp)
      val regLoss = 0.5 * rho * BLAS.dot(temp, temp)
      val gradTotal = temp.copy
      BLAS.scal(rho, gradTotal)

      val loss = lossSum / numSamples + regLoss
      BLAS.axpy(1.0 / numSamples, gradSum, gradTotal)
      (loss, gradTotal.asBreeze)
    }
  }
}
