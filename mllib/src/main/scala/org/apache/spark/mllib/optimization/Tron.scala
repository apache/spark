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

import breeze.linalg.{DenseVector => BDV}
import breeze.linalg.operators.{OpMulMatrix, BinaryOp}
import breeze.optimize.{CachedDiffFunction, SecondOrderFunction, TruncatedNewtonMinimizer => BreezeTRON}
import breeze.util.Implicits._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.{axpy,dot,scal}
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.DataValidators
import org.apache.spark.rdd.RDD


/**
 * :: DeveloperApi ::
 * Class used to solve an optimization problem using trust region truncated Newton method (TRON)
 * Reference: [[http://www.csie.ntu.edu.tw/~cjlin/papers/logistic.pdf]]
 * @param gradient Gradient function to be used.
 * @param hessian Hessian-vector function to be used.
 */
@DeveloperApi
class TRON(private var gradient: Gradient, private var hessian: HessianVector)
  extends Optimizer with Logging {
  private var convergenceTol = 1E-2
  private var maxNumIterations = 1000
  private var regParam = 0.0
  /**
   * Set the convergence tolerance of iterations for TRON. Default 1E-2.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   */
  def setConvergenceTol(tolerance: Double): this.type = {
    this.convergenceTol = tolerance
    this
  }

  /**
   * Set the maximal number of iterations for TRON. Default 1000.
   */
  def setNumIterations(iters: Int): this.type = {
    this.maxNumIterations = iters
    this
  }
  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }
  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for TRON.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }
  /**
   * Set the Hessian-vector function (of the loss function of one single data example)
   * to be used for TRON.
   */
  def setHessian(hessian: HessianVector): this.type = {
    this.hessian = hessian
    this
  }

  override def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = TRON.runTRON(
      data,
      gradient,
      hessian,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeights)
    weights
  }

}
/**
 * :: DeveloperApi ::
 * Top-level method to run TRON.
 */
@DeveloperApi
object TRON extends Logging {
  /**
   * Run TRON in parallel.
   * summing the gradient and summing the hessian vector product over different partitions is
   * performed using one standard
   * spark map-reduce in each iteration.
   *
   * @param data - Input data for TRON. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example)
   * @param hessian - HessianVector object (used to compute the hessian-vector product of the loss
   * function of one single data example)
   * @param convergenceTol - The convergence tolerance of iterations for TRON
   * @param maxNumIterations - Maximal number of iterations that TRON can be run.
   * @param regParam - Regularization parameter
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */
  def runTRON(
      data: RDD[(Double, Vector)],
      gradient: Gradient,
      hessian: HessianVector,
      convergenceTol: Double,
      maxNumIterations: Int,
      regParam: Double,
      initialWeights: Vector): (Vector, Array[Double]) = {

    val lossHistory = new ArrayBuffer[Double](maxNumIterations)

    val numExamples = data.count()
    val H = new HessianMatrix
    H.setHv(hessian)
    H.setdata(data)

    val costFun =
      new SecondOrderCostFun(data, gradient, H, numExamples)

    val tron =
      new BreezeTRON[BDV[Double], HessianMatrix](maxNumIterations, convergenceTol, regParam)

    val states =
      tron.iterations(costFun, initialWeights.toBreeze.toDenseVector).takeUpToWhere(_.converged)

    var state = states.next()
    while(states.hasNext) {
      lossHistory.append(state.adjFval)
      state = states.next()
    }
    lossHistory.append(state.adjFval)
    val weights = Vectors.fromBreeze(state.x)

    logInfo("TRON.runTRON finished. Last 10 losses %s".format(
      lossHistory.takeRight(10).mkString(", ")))

    (weights, lossHistory.toArray)
  }
}
  /**
   * SecondOrderCostFun implements Breeze's SecondOrderFunction[T, H], which returns the loss,
   * gradient and the Hessian
   * at a particular point (weights). It's used in Breeze's convex optimization routines.
   * In this implementation, the Hessian is an object that only implements the matrix-vector
   * product, but the values of the matrix are not directly accessible.
   */

class SecondOrderCostFun(
    data: RDD[(Double, Vector)],
    gradient: Gradient,
    hessian: HessianMatrix,
    numExamples: Long) extends SecondOrderFunction[BDV[Double], HessianMatrix] {
  override def calculate2(weights: BDV[Double]): (Double, BDV[Double], HessianMatrix) = {
      val w = Vectors.fromBreeze(weights)
      val n = w.size
      val bcW = data.context.broadcast(w)
      val localGradient = gradient

      val (gradientSum, lossSum) = data.treeAggregate((Vectors.zeros(n), 0.0))(
          seqOp = (c, v) => (c, v) match { case ((grad, loss), (label, features)) =>
            val l = localGradient.compute(features, label, bcW.value, grad)
            (grad, loss + l)
          },
          combOp = (c1, c2) => (c1, c2) match { case ((grad1, loss1), (grad2, loss2)) =>
            axpy(1.0, grad2, grad1)
            (grad1, loss1 + loss2)
          })
      hessian.setw(bcW)
      scal(1.0 / numExamples, gradientSum)
      (lossSum / numExamples, gradientSum.toBreeze.toDenseVector, hessian)
  }
}

class HessianMatrix extends Serializable{
  private var data: RDD[(Double, Vector)] = null
  private var w: Broadcast[Vector] = null
  private var Hv: HessianVector = null
  private var numExamples: Long = 0

  def setHv(Hv: HessianVector) = {
    this.Hv = Hv
    this
  }

  def setw(w: Broadcast[Vector]) = {
    this.w = w
    this
  }

  def setdata(data: RDD[(Double, Vector)]) = {
    this.data = data
    this.numExamples = data.count()
    this
  }

  def *(direction: BDV[Double]): BDV[Double] = {
    val n = data.first()._2.size
    val sc = data.sparkContext
    val d = Vectors.fromBreeze(direction)
    val bcD = sc.broadcast(d)
    var hv = data.treeAggregate(Vectors.zeros(n))(
    seqOp = (c, v) => (c, v) match { case (hv, (label, features)) =>
      val hv1 = Hv.compute(features, label, w.value, bcD.value)
      axpy(1.0, hv1, hv)
      hv
    },
    combOp = (c1, c2) => (c1, c2) match { case (hv1, hv2) =>
      axpy(1.0, hv2, hv1)
      hv1
    })
    bcD.unpersist()
    scal(1.0 / numExamples, hv)
    hv.toBreeze.toDenseVector
  }
}

object HessianMatrix {
  implicit def mult: OpMulMatrix.Impl2[HessianMatrix, BDV[Double], BDV[Double]] = {
    new OpMulMatrix.Impl2[HessianMatrix, BDV[Double], BDV[Double]] {
      def apply (a: HessianMatrix, b: BDV[Double]): BDV[Double] = {
        a * b
      }
    }
  }
}

