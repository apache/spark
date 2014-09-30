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

package org.apache.spark.examples

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.BitSet

/**
 * Generalized Low Rank Models for Spark
 *
 * Run these commands from the spark root directory.
 *
 * Compile with:
 * sbt/sbt assembly
 *
 * Run with:
 * ./bin/spark-submit  --class org.apache.spark.examples.SparkGLRM  \
 * ./examples/target/scala-2.10/spark-examples-1.1.0-SNAPSHOT-hadoop1.0.4.jar \
 * --executor-memory 1G \
 * --driver-memory 1G
 */

object SparkGLRM {
  // Number of movies
  var M = 1000000
  // Number of users
  var U = 1000000
  // Number of nonzeros per row
  var NNZ = 10
  // Number of features
  var rank = 5
  // Number of iterations
  var ITERATIONS = 3
  // Regularization parameter
  var REG = 0.1
  // Number of partitions for data
  var NUMCHUNKS = 4

  // GLRM settings, change prox and loss here
  val lossFunction = funnyLossL1Grad _
  val moviesProx = proxL1 _
  val usersProx = proxL2 _

  /*********************************
   * GLRM: Bank of loss functions
   *********************************/
  def lossL2Grad(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    prediction - actual
  }

  def lossL1Grad(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    // gradient of L1 loss
    math.signum(prediction - actual)
  }

  def funnyLossL1Grad(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    // weird loss function for demonstration
    if (i + j % 2 == 0) lossL1Grad(i, j, prediction, actual) else lossL2Grad(i, j, prediction, actual)
  }

  /***********************************
   * GLRM: Bank of prox functions
   **********************************/
  // L2 prox
  def proxL2(v:BDV[Double], stepSize:Double): BDV[Double] = {
    val arr = v.toArray.map(x => x / (1.0 + stepSize * REG))
    new BDV[Double](arr)
  }

  // L1 prox
  def proxL1(v:BDV[Double], stepSize:Double): BDV[Double] = {
    val arr = v.toArray.map(x =>
      if (math.abs(x) < REG) 0
      else if (x < -REG) x + REG
      else x - REG
    )
    new BDV[Double](arr)
  }

  // Non-negative prox
  def proxNonneg(v:BDV[Double], stepSize:Double): BDV[Double] = {
    val arr = v.toArray.map(x => math.max(x, 0))
    new BDV[Double](arr)
  }

  /* End of GLRM libarry */



  // Helper functions for updating
  def computeLossGrads(ms: Broadcast[Array[BDV[Double]]], us: Broadcast[Array[BDV[Double]]],
                       R: RDD[(Int, Int, Double)],
                       lossGrad: (Int, Int, Double, Double) => Double) : RDD[(Int, Int, Double)] = {
    R.map { case (i, j, rij) => (i, j, lossGrad(i, j, ms.value(i).dot(us.value(j)), rij))}
  }

  // Update factors
  def update(us: Broadcast[Array[BDV[Double]]], ms: Broadcast[Array[BDV[Double]]],
             loss_grads: RDD[(Int, Int, Double)], stepSize: Double,
             norms: Array[Double],
             prox: (BDV[Double], Double) => BDV[Double])
  : Array[BDV[Double]] = {
    val ret = Array.fill(ms.value.size)(BDV.zeros[Double](rank))

    val retu = loss_grads.map { case (i, j, lossij) => (i, us.value(j) * lossij) } // vector/scalar multiply
                .reduceByKey(_ + _).collect() // vector addition through breeze

    for (entry <- retu) {
      ret(entry._1) = prox(ms.value(entry._1) - entry._2 * (stepSize / (norms(entry._1) + 1.0)), stepSize)
    }

    ret
  }

  def main(args: Array[String]) {
    printf("Running with M=%d, U=%d, nnz=%d, rank=%d, iters=%d, reg=%f\n", M, U, NNZ, rank, ITERATIONS, REG)

    val sparkConf = new SparkConf().setAppName("SparkGLRM")
    val sc = new SparkContext(sparkConf)

    // Create data
    val R = sc.parallelize(0 until M, NUMCHUNKS).flatMap{i =>
      val inds = new scala.collection.mutable.TreeSet[Int]()
      while (inds.size < NNZ) {
        inds += scala.util.Random.nextInt(U)
      }
      inds.toArray.map(j => (i, j, scala.math.random))
    }

    // Transpose data
    val RT = R.map { case (i, j, rij) => (j, i, rij) }

    // Initialize m and u
    var ms = Array.fill(M)(BDV[Double](Array.tabulate(rank)(x => math.random / (M * U))))
    var us = Array.fill(U)(BDV[Double](Array.tabulate(rank)(x => math.random / (M * U))))

    // Compute number of nonzeros per row and column
    val mCountRDD = R.map { case (i, j, rij) => (i, 1) }.reduceByKey(_ + _).collect()
    val mCount = Array.ofDim[Double](M)
    for (entry <- mCountRDD)
      mCount(entry._1) = entry._2
    val maxM = mCount.max
    val uCountRDD = R.map { case (i, j, rij) => (j, 1) }.reduceByKey(_ + _).collect()
    val uCount = Array.ofDim[Double](U)
    for (entry <- uCountRDD)
      uCount(entry._1) = entry._2
    val maxU = uCount.max


    // Iteratively update movies then users
    var msb = sc.broadcast(ms)
    var usb = sc.broadcast(us)

    val errs = Array.ofDim[Double](ITERATIONS)

    val stepSize = 0.1 / (maxU + maxM)

    for (iter <- 1 to ITERATIONS) {
      println("Iteration " + iter + ":")

      // Update ms
      println("Computing gradient losses")
      var lg = computeLossGrads(msb, usb, R, lossFunction)
      println("Updating M factors")
      ms = update(usb, msb, lg, stepSize, mCount, moviesProx)
      msb = sc.broadcast(ms) // Re-broadcast ms because it was updated

      // Update us
      println("Computing gradient losses")
      lg = computeLossGrads(usb, msb, RT, lossFunction)
      println("Updating U factors")
      us = update(msb, usb, lg, stepSize, uCount, usersProx)
      usb = sc.broadcast(us) // Re-broadcast us because it was updated

      // Comment this out in large runs to avoid an extra pass
      //errs(iter-1) = R.map { case (i, j, rij) =>
      //  val err = msb.value(i).dot(usb.value(j)) - rij
      //  err * err
      //}.mean()
    }

    sc.stop()
  }
}
