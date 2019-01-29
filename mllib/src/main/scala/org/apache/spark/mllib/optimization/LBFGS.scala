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

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.axpy
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Class used to solve an optimization problem using Limited-memory BFGS.
 * Reference: <a href="http://en.wikipedia.org/wiki/Limited-memory_BFGS">
 * Wikipedia on Limited-memory BFGS</a>
 * @param gradient Gradient function to be used.
 * @param updater Updater to be used to update weights after every iteration.
 */
@DeveloperApi
class LBFGS(private var gradient: Gradient, private var updater: Updater)
  extends Optimizer with Logging {

  private var numCorrections = 10
  private var convergenceTol = 1E-6
  private var maxNumIterations = 100
  private var regParam = 0.0

  /**
   * Set the number of corrections used in the LBFGS update. Default 10.
   * Values of numCorrections less than 3 are not recommended; large values
   * of numCorrections will result in excessive computing time.
   * numCorrections must be positive, and values from 4 to 9 are generally recommended.
   */
  def setNumCorrections(corrections: Int): this.type = {
    require(corrections > 0,
      s"Number of corrections must be positive but got ${corrections}")
    this.numCorrections = corrections
    this
  }

  /**
   * Set the convergence tolerance of iterations for L-BFGS. Default 1E-6.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * This value must be nonnegative. Lower convergence values are less tolerant
   * and therefore generally cause more iterations to be run.
   */
  def setConvergenceTol(tolerance: Double): this.type = {
    require(tolerance >= 0,
      s"Convergence tolerance must be nonnegative but got ${tolerance}")
    this.convergenceTol = tolerance
    this
  }

  /*
   * Get the convergence tolerance of iterations.
   */
  private[mllib] def getConvergenceTol(): Double = {
    this.convergenceTol
  }

  /**
   * Set the maximal number of iterations for L-BFGS. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    require(iters >= 0,
      s"Maximum of iterations must be nonnegative but got ${iters}")
    this.maxNumIterations = iters
    this
  }

  /**
   * Get the maximum number of iterations for L-BFGS. Defaults to 100.
   */
  private[mllib] def getNumIterations(): Int = {
    this.maxNumIterations
  }

  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    require(regParam >= 0,
      s"Regularization parameter must be nonnegative but got ${regParam}")
    this.regParam = regParam
    this
  }

  /**
   * Get the regularization parameter.
   */
  private[mllib] def getRegParam(): Double = {
    this.regParam
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for L-BFGS.
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

  /**
   * Returns the updater, limited to internal use.
   */
  private[mllib] def getUpdater(): Updater = {
    updater
  }

  override def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = LBFGS.runLBFGS(
      data,
      gradient,
      updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeights)
    weights
  }

}

/**
 * :: DeveloperApi ::
 * Top-level method to run L-BFGS.
 */
@DeveloperApi
object LBFGS extends Logging {
  /**
   * Run Limited-memory BFGS (L-BFGS) in parallel.
   * Averaging the subgradients over different partitions is performed using one standard
   * spark map-reduce in each iteration.
   *
   * @param data - Input data for L-BFGS. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example)
   * @param updater - Updater function to actually perform a gradient step in a given direction.
   * @param numCorrections - The number of corrections used in the L-BFGS update.
   * @param convergenceTol - The convergence tolerance of iterations for L-BFGS which is must be
   *                         nonnegative. Lower values are less tolerant and therefore generally
   *                         cause more iterations to be run.
   * @param maxNumIterations - Maximal number of iterations that L-BFGS can be run.
   * @param regParam - Regularization parameter
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */
  def runLBFGS(
      data: RDD[(Double, Vector)],
      gradient: Gradient,
      updater: Updater,
      numCorrections: Int,
      convergenceTol: Double,
      maxNumIterations: Int,
      regParam: Double,
      initialWeights: Vector): (Vector, Array[Double]) = {

    val lossHistory = mutable.ArrayBuilder.make[Double]

    val numExamples = data.count()

    val costFun =
      new CostFun(data, gradient, updater, regParam, numExamples)

    val lbfgs = new BreezeLBFGS[BDV[Double]](maxNumIterations, numCorrections, convergenceTol)

    val states =
      lbfgs.iterations(new CachedDiffFunction(costFun), initialWeights.asBreeze.toDenseVector)

    /**
     * NOTE: lossSum and loss is computed using the weights from the previous iteration
     * and regVal is the regularization value computed in the previous iteration as well.
     */
    var state = states.next()
    while (states.hasNext) {
      lossHistory += state.value
      state = states.next()
    }
    lossHistory += state.value

    val weights = Vectors.fromBreeze(state.x)

    val lossHistoryArray = lossHistory.result()

    logInfo("LBFGS.runLBFGS finished. Last 10 losses %s".format(
      lossHistoryArray.takeRight(10).mkString(", ")))

    (weights, lossHistoryArray)
  }

  /**
   * CostFun implements Breeze's DiffFunction[T], which returns the loss and gradient
   * at a particular point (weights). It's used in Breeze's convex optimization routines.
   */
  private class CostFun(
    data: RDD[(Double, Vector)],
    gradient: Gradient,
    updater: Updater,
    regParam: Double,
    numExamples: Long) extends DiffFunction[BDV[Double]] {

    override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
      // Have a local copy to avoid the serialization of CostFun object which is not serializable.
      val w = Vectors.fromBreeze(weights)
      val n = w.size
      val bcW = data.context.broadcast(w)
      val localGradient = gradient

      val seqOp = (c: (Vector, Double), v: (Double, Vector)) =>
        (c, v) match {
          case ((grad, loss), (label, features)) =>
            val denseGrad = grad.toDense
            val l = localGradient.compute(features, label, bcW.value, denseGrad)
            (denseGrad, loss + l)
        }

      val combOp = (c1: (Vector, Double), c2: (Vector, Double)) =>
        (c1, c2) match { case ((grad1, loss1), (grad2, loss2)) =>
          val denseGrad1 = grad1.toDense
          val denseGrad2 = grad2.toDense
          axpy(1.0, denseGrad2, denseGrad1)
          (denseGrad1, loss1 + loss2)
       }

      val zeroSparseVector = Vectors.sparse(n, Seq.empty)
      val (gradientSum, lossSum) = data.treeAggregate((zeroSparseVector, 0.0))(seqOp, combOp)

      // broadcasted model is not needed anymore
      bcW.destroy()

      /**
       * regVal is sum of weight squares if it's L2 updater;
       * for other updater, the same logic is followed.
       */
      val regVal = updater.compute(w, Vectors.zeros(n), 0, 1, regParam)._2

      val loss = lossSum / numExamples + regVal
      /**
       * It will return the gradient part of regularization using updater.
       *
       * Given the input parameters, the updater basically does the following,
       *
       * w' = w - thisIterStepSize * (gradient + regGradient(w))
       * Note that regGradient is function of w
       *
       * If we set gradient = 0, thisIterStepSize = 1, then
       *
       * regGradient(w) = w - w'
       *
       * TODO: We need to clean it up by separating the logic of regularization out
       *       from updater to regularizer.
       */
      // The following gradientTotal is actually the regularization part of gradient.
      // Will add the gradientSum computed from the data with weights in the next step.
      val gradientTotal = w.copy
      axpy(-1.0, updater.compute(w, Vectors.zeros(n), 1, 1, regParam)._1, gradientTotal)

      // gradientTotal = gradientSum / numExamples + gradientTotal
      axpy(1.0 / numExamples, gradientSum, gradientTotal)

      (loss, gradientTotal.asBreeze.asInstanceOf[BDV[Double]])
    }
  }
}
