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

import org.scalatest.FunSuite

import breeze.linalg.{DenseMatrix => BDM, Matrix => BM}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.correlation.{Correlations, PearsonCorrelation,
SpearmansCorrelation}
import org.apache.spark.mllib.util.LocalSparkContext

class CorrelationSuite extends FunSuite with LocalSparkContext {

  // test input data
  val xData = Array(1.0, 0.0, -2.0)
  val yData = Array(4.0, 5.0, 3.0)
  val data = Seq(
    Vectors.dense(1.0, 0.0, -2.0),
    Vectors.dense(4.0, 5.0, 3.0),
    Vectors.dense(6.0, 7.0, 8.0),
    Vectors.dense(9.0, 0.0, 1.0)
  )

  test("corr(x, y) default, pearson") {
    val x = sc.parallelize(xData)
    val y = sc.parallelize(yData)
    val expected = 0.6546537
    val default = Statistics.corr(x, y)
    val p1 = Statistics.corr(x, y, "pearson")
    assert(approxEqual(expected, default))
    assert(approxEqual(expected, p1))
  }

  test("corr(x, y) spearman") {
    val x = sc.parallelize(xData)
    val y = sc.parallelize(yData)
    val expected = 0.5
    val s1 = Statistics.corr(x, y, "spearman")
    assert(approxEqual(expected, s1))
  }

  test("corr(X) default, pearson") {
    val X = sc.parallelize(data)
    val defaultMat = Statistics.corr(X)
    val pearsonMat = Statistics.corr(X, "pearson")
    val expected = BDM(
      (1.00000000, 0.05564149, 0.4004714),
      (0.05564149, 1.00000000, 0.9135959),
      (0.40047142, 0.91359586, 1.0000000))
    assert(matrixApproxEqual(defaultMat.toBreeze, expected))
    assert(matrixApproxEqual(pearsonMat.toBreeze, expected))
  }

  test("corr(X) spearman") {
    val X = sc.parallelize(data)
    val spearmanMat = Statistics.corr(X, "spearman")
    val expected = BDM(
      (1.0000000, 0.1054093, 0.4000000),
      (0.1054093, 1.0000000, 0.9486833),
      (0.4000000, 0.9486833, 1.0000000))
    assert(matrixApproxEqual(spearmanMat.toBreeze, expected))
  }

  test("method identification") {
    val pearson = PearsonCorrelation
    val spearman = SpearmansCorrelation

    // Examples of accepted alternatives for "pearson"
    assert(Correlations.getCorrelationFromName("pearson") === pearson)
    assert(Correlations.getCorrelationFromName("P") === pearson)
    assert(Correlations.getCorrelationFromName("PEARSON") === pearson)
    assert(Correlations.getCorrelationFromName("pearsons") === pearson)

    // Examples of accepted alternatives for "spearman"
    assert(Correlations.getCorrelationFromName("spearman") === spearman)
    assert(Correlations.getCorrelationFromName("S") === spearman)
    assert(Correlations.getCorrelationFromName("SPEARMAN") === spearman)
    assert(Correlations.getCorrelationFromName("spearmans") === spearman)

    // Should throw IllegalArgumentException
    try{
      Correlations.getCorrelationFromName("kendall")
      assert(false)
    } catch {
      case ie: IllegalArgumentException =>
    }
  }

  def approxEqual(v1: Double, v2: Double, threshold: Double = 1e-6): Boolean = {
    math.abs(v1 - v2) <= threshold
  }

  def matrixApproxEqual(A: BM[Double], B: BM[Double], threshold: Double = 1e-6): Boolean = {
    for (i <- 0 until A.rows; j <- 0 until A.cols) {
      if (!approxEqual(A(i, j), B(i, j), threshold)) {
        return false
      }
    }
    true
  }
}
