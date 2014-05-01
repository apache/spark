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

import breeze.linalg.{Vector => BV, DenseVector => BDV, DenseMatrix => BDM, cholesky, norm}
import com.github.fommil.netlib.LAPACK.{getInstance=>lapack}
import org.netlib.util.intW


import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.{Partitioner, HashPartitioner, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Class used to solve the optimization problem in ADMMLasso
 */
@DeveloperApi
class ADMMLasso
  extends Optimizer with Logging
{
  private var numPartitions: Int = 10
  private var numIterations: Int = 100
  private var l1RegParam: Double = 1.0
  private var l2RegParam: Double = .0
  private var penalty: Double = 10.0

  /**
   * Set the number of partitions for ADMM. Default 10
   */
  def setNumPartitions(parts: Int): this.type = {
    this.numPartitions = parts
    this
  }

  /**
   * Set the number of iterations for ADMM. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the l1-regularization parameter. Default 1.0.
   */
  def setL1RegParam(regParam: Double): this.type = {
    this.l1RegParam = regParam
    this
  }

  /**
   * Set the l2-regularization parameter. Default .0
   */
  def setL2RegParam(regParam: Double): this.type = {
    this.l2RegParam = regParam
    this
  }

  /**
   * Set the penalty parameter. Default 10.0
   */
  def setPenalty(penalty: Double): this.type = {
    this.penalty = penalty
    this
  }

  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = ADMMLasso.runADMM(data, numPartitions, numIterations, l1RegParam,
      l2RegParam, penalty, initialWeights)
    weights
  }

}

/**
 * :: DeveloperApi ::
 * Top-level method to run ADMMLasso.
 */
@DeveloperApi
object ADMMLasso extends Logging {

  /**
   * @param data  Input data for ADMMLasso. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param numPartitions  number of data blocks to partition the RDD into
   * @param numIterations  number of iterations that ADMM should be run.
   * @param l1RegParam  l1-regularization parameter
   * @param l2RegParam  l2-regularization parameter
   * @param penalty  The penalty parameter in ADMM
   * @param initialWeights  Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */
  def runADMM(
      data: RDD[(Double, Vector)],
      numPartitions: Int,
      numIterations: Int,
      l1RegParam: Double,
      l2RegParam: Double,
      penalty: Double,
      initialWeights: Vector): (Vector, Array[Double]) = {

    val lossHistory = new ArrayBuffer[Double](numIterations)

    /* Initialize weights as a column vector */
    val p = initialWeights.size

    /* Consensus variable */
    var z =  BDV.zeros[Double](p)

    /* Transform the input data into ADMM format */
    def collectBlock(it: Iterator[(Vector, Double)]):
        Iterator[((BDV[Double], BDM[Double], BDM[Double]), (BDV[Double], BDV[Double]))] = {
      val lab = new ArrayBuffer[Double]()
      val features = new ArrayBuffer[Double]()
      var row = 0
      it.foreach {case (f, l) =>
        lab += l
        features ++= f.toArray
        row += 1
      }
      val col = features.length/row

      val designMatrix = new BDM(col, features.toArray).t

      // Precompute the cholesky decomposition for solving linear system inside each partition
      val chol = if (row >= col) {
        cholesky((designMatrix.t * designMatrix) + (BDM.eye[Double](col) :* penalty))
      }
      else cholesky(((designMatrix * designMatrix.t) :/ penalty) + BDM.eye[Double](row))

      Iterator(((BDV(lab.toArray), designMatrix, chol),
        (BDV(initialWeights.toArray), BDV.zeros[Double](col))))
    }

    val partitionedData = data.map{case (x, y) => (y, x)}
      .partitionBy(new HashPartitioner(numPartitions)).cache()

    /* ((lab, design, chol), (x, u)) */
    var dividedData = partitionedData.mapPartitions(collectBlock, true)

    var iter = 1
    var minorChange: Boolean = false
    while (iter <= numIterations && !minorChange) {
      val zBroadcast = z
      def localUpdate(
          it: Iterator[((BDV[Double], BDM[Double], BDM[Double]), (BDV[Double], BDV[Double]))]
          ):Iterator[((BDV[Double], BDM[Double], BDM[Double]), (BDV[Double], BDV[Double]))] = {
        if (it.hasNext) {
          val localData = it.next()
          val (x, u) = localData._2
          val updatedU = u + ((x - zBroadcast) :* penalty)
          /* Update local x by solving linear system Ax = q */
          val (lab, design, chol) = localData._1
          val (row, col) = (design.rows, design.cols)
          val q = (design.t * lab) + (zBroadcast :* penalty) - u

          /* Solve linear system of equations while a is lower-triangular */
          def strtrsLapack(trans: String, a: BDM[Double], y: BDV[Double]): BDV[Double] = {
            val tmp = new BDM(a.rows, y.toArray)
            val info = new intW(0)

            lapack.dtrtrs("L", trans, "N", a.rows, tmp.cols, a.data, a.majorStride,
              tmp.data, tmp.majorStride, info)
            tmp.toDenseVector
          }

          val updatedX = if (row >= col) {
            strtrsLapack("T", chol, strtrsLapack("N", chol, q))
          }
          else {
            (q :/ penalty) - ((design.t *
              strtrsLapack("T", chol, strtrsLapack("N", chol, design * q))) :/ (penalty * penalty))
          }
          Iterator((localData._1, (updatedX, updatedU)))
        }
        else {
          it
        }
      }
      dividedData = dividedData.mapPartitions(localUpdate).cache()
      val (last, zSum) = dividedData.map{case (u, v) =>
        val (lab, design, chol) = u
        val residual = design * zBroadcast - lab
        (0.5 * residual.dot(residual), (v._2 :/ penalty) + v._1)
      }.reduce{case (x, y) => (x._1 + y._1, x._2 + y._2)}

      z = (zSum :/ (numPartitions.toDouble + l2RegParam/penalty)).values.map{v =>
        val threshold = l1RegParam / (l2RegParam + penalty * numPartitions)
        math.max(v - threshold, 0) - math.max(- v - threshold, 0)
      }

      val lastLoss = last + l1RegParam * norm(zBroadcast, 1) +
        (l2RegParam/2) * zBroadcast.dot(zBroadcast)
      if (iter > 200 && math.abs(lastLoss - lossHistory.last) < 1e-5 * lossHistory.last) {
        minorChange = true
      }

      lossHistory += lastLoss
      iter += 1
    }

    logInfo("ADMMLasso finished. Last 10 values of the objective function %s".format(
      lossHistory.takeRight(10).mkString(", ")))

    (Vectors.fromBreeze(z), lossHistory.toArray)
  }
}
