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

package org.apache.spark.ml.stat

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

class CorrelationSuite extends SparkFunSuite with MLlibTestSparkContext {

  val xData = Array(1.0, 0.0, -2.0)
  val yData = Array(4.0, 5.0, 3.0)
  val zeros = new Array[Double](3)
  val data = Seq(
    Vectors.dense(1.0, 0.0, 0.0, -2.0),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.dense(9.0, 0.0, 0.0, 1.0)
  )

  private def X = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

  private def extract(df: DataFrame): BDM[Double] = {
    val Array(Row(mat: Matrix)) = df.collect()
    mat.asBreeze.toDenseMatrix
  }


  test("corr(X) default, pearson") {
    val defaultMat = Correlation.corr(X, "features")
    val pearsonMat = Correlation.corr(X, "features", "pearson")
    // scalastyle:off
    val expected = Matrices.fromBreeze(BDM(
      (1.00000000, 0.05564149, Double.NaN, 0.4004714),
      (0.05564149, 1.00000000, Double.NaN, 0.9135959),
      (Double.NaN, Double.NaN, 1.00000000, Double.NaN),
      (0.40047142, 0.91359586, Double.NaN, 1.0000000)))
    // scalastyle:on

    assert(Matrices.fromBreeze(extract(defaultMat)) ~== expected absTol 1e-4)
    assert(Matrices.fromBreeze(extract(pearsonMat)) ~== expected absTol 1e-4)
  }

  test("corr(X) spearman") {
    val spearmanMat = Correlation.corr(X, "features", "spearman")
    // scalastyle:off
    val expected = Matrices.fromBreeze(BDM(
      (1.0000000,  0.1054093,  Double.NaN, 0.4000000),
      (0.1054093,  1.0000000,  Double.NaN, 0.9486833),
      (Double.NaN, Double.NaN, 1.00000000, Double.NaN),
      (0.4000000,  0.9486833,  Double.NaN, 1.0000000)))
    // scalastyle:on
    assert(Matrices.fromBreeze(extract(spearmanMat)) ~== expected absTol 1e-4)
  }

}
