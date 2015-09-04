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

package org.apache.spark.mllib.stat

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Kernel density estimation. Given a sample from a population, estimate its probability density
 * function at each of the given evaluation points using kernels. Only Gaussian kernel is supported.
 *
 * Scala example:
 *
 * {{{
 * val sample = sc.parallelize(Seq(0.0, 1.0, 4.0, 4.0))
 * val kd = new KernelDensity()
 *   .setSample(sample)
 *   .setBandwidth(3.0)
 * val densities = kd.estimate(Array(-1.0, 2.0, 5.0))
 * }}}
 */
@Since("1.4.0")
@Experimental
class KernelDensity extends Serializable {

  import KernelDensity._

  /** Bandwidth of the kernel function. */
  private var bandwidth: Double = 1.0

  /** A sample from a population. */
  private var sample: RDD[Double] = _

  /**
   * Sets the bandwidth (standard deviation) of the Gaussian kernel (default: `1.0`).
   */
  @Since("1.4.0")
  def setBandwidth(bandwidth: Double): this.type = {
    require(bandwidth > 0, s"Bandwidth must be positive, but got $bandwidth.")
    this.bandwidth = bandwidth
    this
  }

  /**
   * Sets the sample to use for density estimation.
   */
  @Since("1.4.0")
  def setSample(sample: RDD[Double]): this.type = {
    this.sample = sample
    this
  }

  /**
   * Sets the sample to use for density estimation (for Java users).
   */
  @Since("1.4.0")
  def setSample(sample: JavaRDD[java.lang.Double]): this.type = {
    this.sample = sample.rdd.asInstanceOf[RDD[Double]]
    this
  }

  /**
   * Estimates probability density function at the given array of points.
   */
  @Since("1.4.0")
  def estimate(points: Array[Double]): Array[Double] = {
    val sample = this.sample
    val bandwidth = this.bandwidth

    require(sample != null, "Must set sample before calling estimate.")

    val n = points.length
    // This gets used in each Gaussian PDF computation, so compute it up front
    val logStandardDeviationPlusHalfLog2Pi = math.log(bandwidth) + 0.5 * math.log(2 * math.Pi)
    val (densities, count) = sample.aggregate((new Array[Double](n), 0L))(
      (x, y) => {
        var i = 0
        while (i < n) {
          x._1(i) += normPdf(y, bandwidth, logStandardDeviationPlusHalfLog2Pi, points(i))
          i += 1
        }
        (x._1, x._2 + 1)
      },
      (x, y) => {
        blas.daxpy(n, 1.0, y._1, 1, x._1, 1)
        (x._1, x._2 + y._2)
      })
    blas.dscal(n, 1.0 / count, densities, 1)
    densities
  }
}

private object KernelDensity {

  /** Evaluates the PDF of a normal distribution. */
  def normPdf(
      mean: Double,
      standardDeviation: Double,
      logStandardDeviationPlusHalfLog2Pi: Double,
      x: Double): Double = {
    val x0 = x - mean
    val x1 = x0 / standardDeviation
    val logDensity = -0.5 * x1 * x1 - logStandardDeviationPlusHalfLog2Pi
    math.exp(logDensity)
  }
}
