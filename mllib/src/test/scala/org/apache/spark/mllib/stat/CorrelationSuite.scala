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

import breeze.linalg.{DenseMatrix => BDM, Matrix => BM}

import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.correlation.{Correlations, PearsonCorrelation,
  SpearmanCorrelation}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class CorrelationSuite extends SparkFunSuite with MLlibTestSparkContext with Logging {

  // test input data
  val xData = Array(1.0, 0.0, -2.0)
  val yData = Array(4.0, 5.0, 3.0)
  val zeros = new Array[Double](3)
  val data = Seq(
    Vectors.dense(1.0, 0.0, 0.0, -2.0),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.dense(9.0, 0.0, 0.0, 1.0)
  )

  test("corr(x, y) pearson, 1 value in data") {
    val x = sc.parallelize(Array(1.0))
    val y = sc.parallelize(Array(4.0))
    intercept[RuntimeException] {
      Statistics.corr(x, y, "pearson")
    }
    intercept[RuntimeException] {
      Statistics.corr(x, y, "spearman")
    }
  }

  test("corr(x, y) default, pearson") {
    val x = sc.parallelize(xData)
    val y = sc.parallelize(yData)
    val expected = 0.6546537
    val default = Statistics.corr(x, y)
    val p1 = Statistics.corr(x, y, "pearson")
    assert(approxEqual(expected, default))
    assert(approxEqual(expected, p1))

    // numPartitions >= size for input RDDs
    for (numParts <- List(xData.size, xData.size * 2)) {
      val x1 = sc.parallelize(xData, numParts)
      val y1 = sc.parallelize(yData, numParts)
      val p2 = Statistics.corr(x1, y1)
      assert(approxEqual(expected, p2))
    }

    // RDD of zero variance
    val z = sc.parallelize(zeros)
    assert(Statistics.corr(x, z).isNaN)
  }

  test("corr(x, y) spearman") {
    val x = sc.parallelize(xData)
    val y = sc.parallelize(yData)
    val expected = 0.5
    val s1 = Statistics.corr(x, y, "spearman")
    assert(approxEqual(expected, s1))

    // numPartitions >= size for input RDDs
    for (numParts <- List(xData.size, xData.size * 2)) {
      val x1 = sc.parallelize(xData, numParts)
      val y1 = sc.parallelize(yData, numParts)
      val s2 = Statistics.corr(x1, y1, "spearman")
      assert(approxEqual(expected, s2))
    }

    // RDD of zero variance => zero variance in ranks
    val z = sc.parallelize(zeros)
    assert(Statistics.corr(x, z, "spearman").isNaN)
  }

  test("corr(X) default, pearson") {
    val X = sc.parallelize(data)
    val defaultMat = Statistics.corr(X)
    val pearsonMat = Statistics.corr(X, "pearson")
    // scalastyle:off
    val expected = BDM(
      (1.00000000, 0.05564149, Double.NaN, 0.4004714),
      (0.05564149, 1.00000000, Double.NaN, 0.9135959),
      (Double.NaN, Double.NaN, 1.00000000, Double.NaN),
      (0.40047142, 0.91359586, Double.NaN, 1.0000000))
    // scalastyle:on
    assert(matrixApproxEqual(defaultMat.toBreeze, expected))
    assert(matrixApproxEqual(pearsonMat.toBreeze, expected))
  }

  test("corr(X) spearman") {
    val X = sc.parallelize(data)
    val spearmanMat = Statistics.corr(X, "spearman")
    // scalastyle:off
    val expected = BDM(
      (1.0000000,  0.1054093,  Double.NaN, 0.4000000),
      (0.1054093,  1.0000000,  Double.NaN, 0.9486833),
      (Double.NaN, Double.NaN, 1.00000000, Double.NaN),
      (0.4000000,  0.9486833,  Double.NaN, 1.0000000))
    // scalastyle:on
    assert(matrixApproxEqual(spearmanMat.toBreeze, expected))
  }

  test("method identification") {
    val pearson = PearsonCorrelation
    val spearman = SpearmanCorrelation

    assert(Correlations.getCorrelationFromName("pearson") === pearson)
    assert(Correlations.getCorrelationFromName("spearman") === spearman)

    // Should throw IllegalArgumentException
    try {
      Correlations.getCorrelationFromName("kendall")
      assert(false)
    } catch {
      case ie: IllegalArgumentException =>
    }
  }

  def approxEqual(v1: Double, v2: Double, threshold: Double = 1e-6): Boolean = {
    if (v1.isNaN) {
      v2.isNaN
    } else {
      math.abs(v1 - v2) <= threshold
    }
  }

  def matrixApproxEqual(A: BM[Double], B: BM[Double], threshold: Double = 1e-6): Boolean = {
    for (i <- 0 until A.rows; j <- 0 until A.cols) {
      if (!approxEqual(A(i, j), B(i, j), threshold)) {
        logInfo("i, j = " + i + ", " + j + " actual: " + A(i, j) + " expected:" + B(i, j))
        return false
      }
    }
    true
  }
}
