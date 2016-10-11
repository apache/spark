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

package org.apache.spark.ml.feature

import breeze.numerics.{cos, sin}
import breeze.numerics.constants.Pi

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext

class SignRandomProjectionSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("SignRandomProjection") {
    val data = {
      for (i <- -5 until 5; j <- -5 until 5) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    val srp = new SignRandomProjection()
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(0)

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(df, srp, 1.6, 0.4)
    assert(falsePositive < 0.1)
    assert(falseNegative < 0.1)
  }

  test("approxNearestNeighbors for cosine distance") {
    val data = {
      for (i <- -5 until 5; j <- -5 until 5) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")
    val key = Vectors.dense(1.2, 3.4)

    val mh = new SignRandomProjection()
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(0)

    val (precision, recall) = LSHTest.calculateApproxNearestNeighbors(mh, df, key, 30,
      singleProbing = true)
    assert(precision >= 0.8)
    assert(recall >= 0.8)
  }

  test("approxSimilarityJoin for cosine distance") {
    val dataA = {
      for (i <- -5 until 5; j <- -5 until 5) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    val dfA = spark.createDataFrame(dataA.map(Tuple1.apply)).toDF("keys")

    val dataB = {
      for (i <- 0 until 24) yield Vectors.dense(10 * sin(Pi / 12 * i), 10 * cos(Pi / 12 * i))
    }
    val dfB = spark.createDataFrame(dataB.map(Tuple1.apply)).toDF("keys")

    val mh = new SignRandomProjection()
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(0)

    val (precision, recall) = LSHTest.calculateApproxSimilarityJoin(mh, dfA, dfB, 0.5)
    assert(precision == 1.0)
    assert(recall >= 0.8)
  }
}
