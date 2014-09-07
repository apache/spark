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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Generalized Low Rank Models for Spark
 */
object SparkGLRM {
  // Number of movies
  var M = 0
  // Number of users
  var U = 0
  // Number of nonzeros
  var NNZ = 0
  // Number of features
  var rank = 0
  // Number of iterations
  var ITERATIONS = 0
  // Regularization parameter
  var REG = 0


  /*
   * GLRM settings: Change the Loss function and prox here
   */

  def loss(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    return (prediction - actual) * (prediction - actual)
  }

  def loss_grad(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    return (prediction - actual)
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

  def prox(v:BDV[Double], stepSize:Double): BDV[Double] = {
    // L2 prox
    v / (1.0 + stepSize * REG)
  }

  // Update factors
  def update(us: Broadcast[Array[BDV[Double]]], ms: Broadcast[Array[BDV[Double]]],
             loss_grads: RDD[(Int, Int, Double)], stepSize: Double)
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
    val options = (0 to 5).map(i => if (i < args.length) Some(args(i)) else None)

    options.toArray match {
      case Array(m, u, nn, trank, iters, reg) =>
        M = m.getOrElse("10").toInt
        U = u.getOrElse("5").toInt
        NNZ = nn.getOrElse("23").toInt
        rank = trank.getOrElse("2").toInt
        ITERATIONS = iters.getOrElse("5").toInt
        REG = reg.getOrElse("100").toInt

      case _ =>
        System.err.println("Usage: SparkGLRM [M] [U] [nnz] [rank] [iters] [regularization]")
        System.exit(1)
    }

    printf("Running with M=%d, U=%d, nnz=%d, rank=%d, iters=%d, reg=%d\n", M, U, NNZ, rank, ITERATIONS, REG)

    val sparkConf = new SparkConf().setAppName("SparkGLRM")
    val sc = new SparkContext(sparkConf)

    // Create data
    val R = sc.parallelize(1 to NNZ).flatMap{x =>
      val i = math.abs(math.round(math.random * (M - 1)).toInt)
      val j = math.abs(math.round(math.random * (U - 1)).toInt)
      List.fill(10)(((i, j), math.random))
    }.reduceByKey(_ + _).map{case (a, b) => (a._1, a._2, b)}

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
      ms = update(usb, msb, lg, 1.0/iter)
      msb = sc.broadcast(ms) // Re-broadcast ms because it was updated

      // Update us
      println("Computing gradient losses")
      lg = computeLossGrads(usb, msb, RT)
      println("Updating U factors")
      us = update(msb, usb, lg, 1.0/iter)
      usb = sc.broadcast(us) // Re-broadcast us because it was updated

      println("error = " + computeLoss(msb, usb, R).map { case (_, _, lij) => lij}.mean())
      //println(us.mkString(", "))
      //println(ms.mkString(", "))
      println()
    }

    sc.stop()
  }
}
