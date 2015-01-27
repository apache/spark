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

package org.apache.spark.mllib.kernels

import breeze.linalg.{norm, DenseVector}
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import breeze.numerics.{sqrt => brsqrt}


class GaussianDensityKernel
  extends DensityKernel with KernelEstimator with Logging with Serializable {
  private val exp = scala.math.exp _
  private val pow = scala.math.pow _
  private val sqrt = scala.math.sqrt _
  private val Pi = scala.math.Pi
  protected var bandwidth: Vector = Vectors.zeros(10)

  private def evalForDimension(x: Double, pilot: Double): Double =
    exp(-1*pow(x/pilot, 2)/2)/sqrt(Pi * 2)

  private def evalWithBandwidth(x: Vector, b: Vector): Double = {
    assert(x.size == b.size,
      "Dimensions of vector x and the bandwidth of the kernel must match")
    val buff = x.toBreeze
    val bw = b.toBreeze
    val normalizedbuff: breeze.linalg.DenseVector[Double] = DenseVector.tabulate(
      bw.size)(
        (i) => buff(i)/bw(i)
      )
    exp(-1*pow(norm(normalizedbuff), 2)/2)/pow(sqrt(Pi * 2), b.size)
  }

  /*
  * Calculate the value of the hermite polynomials 
  * tail recursively. This is needed to calculate 
  * the Gaussian derivatives at a point x.
  * */
  private def hermite(n: Int, x: Double): Double = {
    def hermiteHelper(k: Int, x: Double, a: Double, b: Double): Double =
      k match {
        case 0 => a
        case 1 => b
        case _ => hermiteHelper(k-1, x, b, x*b - (k-1)*a)
      }
    hermiteHelper(n, x, 1, x)
  }

  def setBandwidth(b: linalg.Vector): Unit = {
    this.bandwidth = b
  }

  override def eval(x: linalg.Vector) = evalWithBandwidth(x, this.bandwidth)


  /**
   * Calculates the derivative at point x for the Gaussian
   * Density Kernel, for only one dimension.
   *
   * @param n The number of times the gaussian has to be differentiated
   * @param x The point x at which the derivative has to evaluated
   * @return The value of the nth derivative at the point x
   * */
  override def derivative(n: Int, x: Double): Double = {
    (1/sqrt(2*Pi))*(1/pow(-1.0,n))*exp(-1*pow(x,2)/2)*hermite(n, x)
  }

  /**
   * Implementation of the estimator for the R integral
   * for a multivariate Gaussian Density Kernel.
   * Evaluates R(D_r(f(x))).
   *
   * @param r the degree of the derivative of the kernel
   *
   * @param N The size of the original data set from which
   *          kernel matrix [[RDD]] was constructed.
   *
   * @param pilot The pilot bandwidth to be used to calculate
   *              the kernel values. (Note that we have not calculated
   *              the AMISE bandwidth yet and we use this estimator
   *              as a means to get the AMISE bandwidth)
   *
   * @param kernel The RDD containing the kernel matrix
   *               consisting of pairs Xi - Xj, where Xi and Xj
   *               are drawn from the original data set.
   *
   * @return R the estimated value of the integral of the square
   *         of the rth derivative of the kernel over the Real domain.
   * */
  override protected def R(r: Int, N: Long, pilot: breeze.linalg.Vector[Double],
                           kernel: RDD[((Long, Long), Vector)]): breeze.linalg.Vector[Double] = {


    /*
    * Apply map to get values of the derivative of the kernel
    * at various point pairs.
    * */
    val kernelNormalized = kernel.map((couple) =>
      (couple._1, Vectors.fromBreeze(DenseVector.tabulate(pilot.size)
        ((i) => (1/(pow(N, 2)*pow(pilot(i), r + 1)))*
          this.derivative(r, couple._2.toBreeze(i)/pilot(i)))
      )))

    /*
    * Sum up all the individual values to get the estimated
    * value of the integral
    * */
    val integralvalue = kernelNormalized.reduce((a,b) =>
      ((0,0), Vectors.fromBreeze(a._2.toBreeze + b._2.toBreeze)))

    integralvalue._2.toBreeze
  }

  override protected val mu = (1/4)*(1/sqrt(Pi))
  override protected val r = (1/2)*(1/sqrt(Pi))

  /**
   * Use the Sheather and Jones plug-in
   * method to calculate the optimal bandwidth
   * http://bit.ly/1EoBY7q
   *
   * */
  override def optimalBandwidth(data: RDD[Vector]): Unit = {
    val dataSize: Long = data.count()

    //First calculate variance of all dimensions
    val columnStats = Statistics.colStats(data)
    // And then the standard deviation
    val colvar = columnStats.variance.toBreeze
    val colstd = colvar.map((v) => sqrt(v))

    //Now calculate the initial estimates of R(f^6) and R(f^8)

    /*val Rf6: DenseVector[Double] = DenseVector.tabulate(colstd.size)(
      (i) => -15.0*pow(colstd(i), -7.0)/(16*sqrt(Pi)))*/

    val Rf8: DenseVector[Double] = DenseVector.tabulate(colstd.size)(
      (i) => 105*pow(colstd(i), -9.0)/(32*sqrt(Pi)))

    /*
    * Use the earlier result to calculate
    * h1 and h2 bandwidths for each dimension
    * */

    /*val h1: DenseVector[Double] = DenseVector.tabulate(colstd.size)((i) =>
      pow(-2*this.derivative(4, 0.0)/(dataSize*this.mu*Rf6(i)), 1/7))*/
    val h2: DenseVector[Double] = DenseVector.tabulate(colstd.size)((i) =>
      pow(-2*this.derivative(6, 0.0)/(dataSize*this.mu*Rf8(i)), 1/9))


    /*
    * Use h1 and h2 to calculate more
    * refined estimates of R(f^6) and R(f^8)
    * */

    //Get an 0-indexed version of the original data set
    val mappedData = SVMKernel.indexedRDD(data)

    /*
    * Apply cartesian product on the indexed data set
    * and then map it to a RDD of type [(i,j), Xi - Xj]
    * */
    val kernel = mappedData.cartesian(mappedData)
      .map((prod) => ((prod._1._1, prod._2._1),
      Vectors.fromBreeze(prod._1._2.toBreeze -
        prod._2._2.toBreeze))
      )
    kernel.cache()


    val newRf6: breeze.linalg.Vector[Double] = this.R(8, dataSize, h2, kernel)

    val hAMSE: breeze.linalg.Vector[Double] = DenseVector.tabulate(colstd.size)((i) =>
      pow((-2*this.derivative(4, 0.0))/(dataSize*this.mu*newRf6(i)), 1/7))

    val newRf4: breeze.linalg.Vector[Double] = this.R(4, dataSize, hAMSE, kernel)

    val hAMISE: breeze.linalg.Vector[Double] = DenseVector.tabulate(colstd.size)((i) =>
      pow(this.r/(dataSize*this.mu*this.mu*newRf4(i)), 1/5))

    this.bandwidth = Vectors.fromBreeze(hAMISE)
  }
}
