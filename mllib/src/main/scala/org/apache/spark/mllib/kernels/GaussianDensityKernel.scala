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


class GaussianDensityKernel(protected var bandwidth: Vector)
  extends DensityKernel with Logging {

  def setBandwidth(b: linalg.Vector): Unit = {
    this.bandwidth = b
  }

  override def eval(x: linalg.Vector) = evalWithBandwidth(x, this.bandwidth)

  private def evalWithBandwidth(x: Vector, b: Vector): Double = {
    val exp = scala.math.exp _
    val pow = scala.math.pow _ _
    val sqrt = scala.math.sqrt _
    val Pi = scala.math.Pi

    val buff = x.toBreeze

    val normalizedbuff: breeze.linalg.DenseVector[Double] = DenseVector.tabulate(
      b.size)(
        (i) => buff(i)/b.apply(i)
      )
    exp(-1*pow(norm(normalizedbuff), 2)/2)/sqrt(Pi * 2)
  }

  //TODO: Implement derivative function
  private def derivative(n: Int)(x: Vector): Vector = {
    Vectors.zeros(x.size)
  }

  //TODO: Implement R integral
  private def R(r: Int, pilot: Vector): Vector = {
    Vectors.zeros(pilot.size)
  }

  //TODO: Implement mu integral
  private val mu: Vector = Vectors.zeros(this.bandwidth.size)

  override def optimalBandwidth(data: RDD[Vector]): Unit = {

    //First calculate variance of all dimensions
    val columnStats = Statistics.colStats(data)

    val colvariance = columnStats.variance

    //Now calculate the initial estimates of R(f'''') and R(f'''''')

    //Use the earlier result to calculate h1 and h2 bandwidths for each
    //dimension separately

    //Use the Sheathon and Jones 1991 result to calculate
    //the optimal bandwidth

    //Vectors.fromBreeze(breeze.linalg.DenseVector.ones[Double](10))
  }
}
