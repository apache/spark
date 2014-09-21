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
 */
object SparkGLRM {
  // Number of movies
  var M = 1000000
  // Number of users
  var U = 1000000
  // Number of nonzeros per row
  var NNZ = 10
  // Number of features
  var rank = 2
  // Number of iterations
  var ITERATIONS = 2
  // Regularization parameter
  var REG = 100
  // Number of partitions for data
  var NUMCHUNKS = 2



  /*
   * GLRM settings: Change the Loss function and prox here
   */

  def loss(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    (prediction - actual) * (prediction - actual)
  }

  def loss_grad(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    prediction - actual
  }

  def computeLossGrads(ms: Broadcast[Array[BDV[Double]]], us: Broadcast[Array[BDV[Double]]],
                       R: RDD[(Int, Int, Double)])
  : RDD[(Int, Int, Double)] = {
    R.map { case (i, j, rij) => (i, j, loss_grad(i, j, ms.value(i).dot(us.value(j)), rij))}
  }

  def computeLoss(ms: Broadcast[Array[BDV[Double]]], us: Broadcast[Array[BDV[Double]]],
                   R: RDD[(Int, Int, Double)])
  : RDD[(Int, Int, Double)] = {
    R.map { case (i, j, rij) => (i, j, loss(i, j, ms.value(i).dot(us.value(j)), rij))}
  }

  def proxL2(v:BDV[Double], stepSize:Double): BDV[Double] = {
    // L2 prox
    v / (1.0 + stepSize * REG)
  }

  // Update factors
  def update(us: Broadcast[Array[BDV[Double]]], ms: Broadcast[Array[BDV[Double]]],
             loss_grads: RDD[(Int, Int, Double)], stepSize: Double, prox: (BDV[Double], Double) => BDV[Double])
  : Array[BDV[Double]] = {
    val ret = Array.fill(ms.value.size)(BDV.zeros[Double](rank))

    val retu = loss_grads.map { case (i, j, lossij) => (i, us.value(j) * lossij)} // vector/scalar multiply
                .reduceByKey(_ + _).collect() // vector addition through breeze

    for (entry <- retu) {
      ret(entry._1) = prox(ms.value(entry._1) - entry._2 * stepSize, stepSize)
    }

    ret
  }


  def main(args: Array[String]) {
    printf("Running with M=%d, U=%d, nnz=%d, rank=%d, iters=%d, reg=%d\n", M, U, NNZ, rank, ITERATIONS, REG)

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
    var ms = Array.fill(M)(BDV[Double](Array.tabulate(rank)(x => math.random)))
    var us = Array.fill(U)(BDV[Double](Array.tabulate(rank)(x => math.random)))

    // Iteratively update movies then users
    var msb = sc.broadcast(ms)
    var usb = sc.broadcast(us)

    for (iter <- 1 to ITERATIONS) {
      println("Iteration " + iter + ":")

      // Update ms
      println("Computing gradient losses")
      var lg = computeLossGrads(msb, usb, R)
      println("Updating M factors")
      ms = update(usb, msb, lg, 1.0/iter, proxL2)
      msb = sc.broadcast(ms) // Re-broadcast ms because it was updated

      // Update us
      println("Computing gradient losses")
      lg = computeLossGrads(usb, msb, RT)
      println("Updating U factors")
      us = update(msb, usb, lg, 1.0/iter, proxL2)
      usb = sc.broadcast(us) // Re-broadcast us because it was updated

      println("error = " + computeLoss(msb, usb, R).map { case (_, _, lij) => lij}.mean())
      //println(us.mkString(", "))
      //println(ms.mkString(", "))
      println()
    }

    sc.stop()
  }
}
