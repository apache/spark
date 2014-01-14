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

import scala.math.sqrt
import cern.jet.math._
import cern.colt.matrix._
import cern.colt.matrix.linalg._
import org.apache.spark._

/**
 * Alternating least squares matrix factorization.
 */
object SparkALS {
  // Parameters set through command line arguments
  var M = 0 // Number of movies
  var U = 0 // Number of users
  var F = 0 // Number of features
  var ITERATIONS = 0

  val LAMBDA = 0.01 // Regularization coefficient

  // Some COLT objects
  val factory2D = DoubleFactory2D.dense
  val factory1D = DoubleFactory1D.dense
  val algebra = Algebra.DEFAULT
  val blas = SeqBlas.seqBlas

  def generateR(): DoubleMatrix2D = {
    val mh = factory2D.random(M, F)
    val uh = factory2D.random(U, F)
    algebra.mult(mh, algebra.transpose(uh))
  }

  def rmse(targetR: DoubleMatrix2D, ms: Array[DoubleMatrix1D],
    us: Array[DoubleMatrix1D]): Double =
  {
    val r = factory2D.make(M, U)
    for (i <- 0 until M; j <- 0 until U) {
      r.set(i, j, blas.ddot(ms(i), us(j)))
    }
    //println("R: " + r)
    blas.daxpy(-1, targetR, r)
    val sumSqs = r.aggregate(Functions.plus, Functions.square)
    sqrt(sumSqs / (M * U))
  }

  def update(i: Int, m: DoubleMatrix1D, us: Array[DoubleMatrix1D],
    R: DoubleMatrix2D) : DoubleMatrix1D =
  {
    val U = us.size
    val F = us(0).size
    val XtX = factory2D.make(F, F)
    val Xty = factory1D.make(F)
    // For each user that rated the movie
    for (j <- 0 until U) {
      val u = us(j)
      // Add u * u^t to XtX
      blas.dger(1, u, u, XtX)
      // Add u * rating to Xty
      blas.daxpy(R.get(i, j), u, Xty)
    }
    // Add regularization coefs to diagonal terms
    for (d <- 0 until F) {
      XtX.set(d, d, XtX.get(d, d) + LAMBDA * U)
    }
    // Solve it with Cholesky
    val ch = new CholeskyDecomposition(XtX)
    val Xty2D = factory2D.make(Xty.toArray, F)
    val solved2D = ch.solve(Xty2D)
    solved2D.viewColumn(0)
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkALS <master> [<M> <U> <F> <iters> <slices>]")
      System.exit(1)
    }

    var host = ""
    var slices = 0

    val options = (0 to 5).map(i => if (i < args.length) Some(args(i)) else None)

    options.toArray match {
      case Array(host_, m, u, f, iters, slices_) =>
        host = host_.get
        M = m.getOrElse("100").toInt
        U = u.getOrElse("500").toInt
        F = f.getOrElse("10").toInt
        ITERATIONS = iters.getOrElse("5").toInt
        slices = slices_.getOrElse("2").toInt
      case _ =>
        System.err.println("Usage: SparkALS <master> [<M> <U> <F> <iters> <slices>]")
        System.exit(1)
    }
    printf("Running with M=%d, U=%d, F=%d, iters=%d\n", M, U, F, ITERATIONS)

    val sc = new SparkContext(host, "SparkALS",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    
    val R = generateR()

    // Initialize m and u randomly
    var ms = Array.fill(M)(factory1D.random(F))
    var us = Array.fill(U)(factory1D.random(F))

    // Iteratively update movies then users
    val Rc  = sc.broadcast(R)
    var msb = sc.broadcast(ms)
    var usb = sc.broadcast(us)
    for (iter <- 1 to ITERATIONS) {
      println("Iteration " + iter + ":")
      ms = sc.parallelize(0 until M, slices)
                .map(i => update(i, msb.value(i), usb.value, Rc.value))
                .toArray
      msb = sc.broadcast(ms) // Re-broadcast ms because it was updated
      us = sc.parallelize(0 until U, slices)
                .map(i => update(i, usb.value(i), msb.value, algebra.transpose(Rc.value)))
                .toArray
      usb = sc.broadcast(us) // Re-broadcast us because it was updated
      println("RMSE = " + rmse(R, ms, us))
      println()
    }

    System.exit(0)
  }
}
