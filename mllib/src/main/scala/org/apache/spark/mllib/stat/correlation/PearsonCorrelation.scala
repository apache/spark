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

package org.apache.spark.mllib.stat.correlation

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
 * Compute Pearson correlation for two RDDs of the type RDD[Double] or the correlation matrix
 * for an RDD of the type RDD[Vector].
 *
 * Definition of Pearson correlation can be found at
 * http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
 */
object PearsonCorrelation extends Correlation {

  //TODO clean everything up here

  override def computeCorrelation(x: RDD[Double], y: RDD[Double]): Double = {

    var paired: Option[RDD[(Double, Double)]] = None

    try {
      paired = Some(x.zip(y))
    } catch {
      case se: SparkException => throw new IllegalArgumentException("Cannot compute correlation"
        + "for RDDs of different sizes.")
    }

    val stats = paired.get.aggregate(new Stats)((m: Stats, i: (Double, Double)) => m.merge(i),
      (m1: Stats, m2: Stats) => m1.merge(m2))

    val cov = stats.xyMean - stats.xMean * stats.yMean

    cov / (stats.xStdev * stats.yStdev)
  }

  override def computeCorrelationMatrix(X: RDD[Vector]): Matrix = {
    val rowMatrix = new RowMatrix(X)
    val cov = rowMatrix.computeCovariance().toBreeze.asInstanceOf[BDM[Double]]
    val n = cov.cols

    // Compute the standard deviation on the diagonals first
    var i = 0
    while (i < n) {
      cov(i, i) = math.sqrt(cov(i, i))
      i +=1
    }
    // or we could put the stddev in its own array

    // TODO: we could use blas.dspr instead to compute the correlation matrix if the covariance matrix
    // is upper triangular.
    // Loop through columns since cov is column major

    var j = 0
    var sigma = 0.0
    while (j < n) {
      sigma = cov(j, j)
      i = 0
      while (i < j) {
        val covariance = cov(i, j) / (sigma * cov(i, i))
        cov(i, j) = covariance
        cov(j, i) = covariance
        i += 1
      }
      j += 1
    }

    // put 1.0 on the diagonals
    i = 0
    while (i < n) {
      cov(i, i) = 1.0
      i +=1
    }

    Matrices.fromBreeze(cov)
  }
}

/**
 * Custom version of StatCounter to allow for computation of all necessary statistics in one pass
 * over both input RDDs since passes over large RDDs are expensive
 */
private class Stats extends Serializable {
  private var n: Long = 0L
  private var Exy: Double = 0.0
  private var Ex: Double = 0.0
  private var Sx: Double = 0.0
  private var Ey: Double = 0.0
  private var Sy: Double = 0.0

  def count: Long = n

  def xyMean: Double = Exy

  def xMean: Double = Ex

  def xStdev: Double = if (n == 0) Double.NaN else math.sqrt(Sx / n)

  def yMean: Double = Ey

  def yStdev: Double = if (n == 0) Double.NaN else math.sqrt(Sy / n)

  def merge(xy:(Double, Double)): Stats = {
    val dX = xy._1 - Ex
    val dY = xy._2 - Ey
    n += 1
    Ex += dX / n
    Sx += dX * (xy._1 - Ex)
    Ey += dY / n
    Sy += dY * (xy._2 - Ey)
    Exy += (xy._1 * xy._2 - Exy) / n
    this
  }

  def merge(other: Stats): Stats = {
    if (n == 0) {
      return other
    } else if (other.n > 0) {
      val dX = other.Ex - Ex
      val dY = other.Ey - Ey
      val sum = n + other.n
      if (other.n * 10 < n) {
        Ex += dX * other.n / sum
        Ey += dY * other.n / sum
        Exy += (other.Exy - Exy) * other.n / sum
      } else if (n * 10 < other.n) {
        Ex = other.Ex - dX * n / sum
        Ey = other.Ey - dY * n / sum
        Exy += other.Exy - (other.Exy - Exy) * n / sum
      } else {
        Ex = (Ex * n + other.Ex * other.n) / sum
        Ey = (Ey * n + other.Ey * other.n) / sum
        Exy = (Exy * n + other.Exy * other.n) / sum
      }
      Sx += other.Sx + (dX * dX * n * other.n) / sum
      Sy += other.Sy + (dY * dY * n * other.n) / sum
      n += other.n
    }
    this
  }
}