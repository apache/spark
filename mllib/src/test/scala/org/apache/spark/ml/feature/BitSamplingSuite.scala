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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class BitSamplingSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("BitSampling") {
    val data = {
      for (i <- 0 to 10) yield Vectors.sparse(10, (0 until i).map((_, 1.0)))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    val bs = new BitSampling()
      .setSampleSize(3)
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(0)

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(df, bs, 5.0, 2.0)
    assert(falsePositive < 0.1)
    assert(falseNegative < 0.15)
  }

  test("BitSampling for max sample size") {
    val data = {
      for (i <- 0 to 100) yield Vectors.sparse(100, (0 until i).map((_, 1.0)))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    val bs = new BitSampling()
      .setSampleSize(63)
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(0)

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(df, bs, 10.0, 5.0)
    assert(falsePositive == 0.0)
    assert(falseNegative <= 0.07)
  }

  test("approxNearestNeighbors for bit sampling") {
    val data = {
      for (i <- 0 to 100) yield Vectors.sparse(100, (0 until i).map((_, 1.0)))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    val bs = new BitSampling()
      .setSampleSize(3)
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(0)

    val key: Vector = Vectors.sparse(100, (50 until 100).map((_, 1.0)))

    val (precision, recall) = LSHTest.calculateApproxNearestNeighbors(bs, df, key, 40,
      singleProbing = false)
    assert(precision == 1.0)
    assert(recall == 1.0)
  }

  test("approxSimilarityJoin for bit sampling on different dataset") {
    val dataA = {
      for (i <- 0 to 100) yield Vectors.sparse(100, (0 until i).map((_, 1.0)))
    }
    val dfA = spark.createDataFrame(dataA.map(Tuple1.apply)).toDF("keys")

    val dataB = {
      for (i <- 0 to 100) yield Vectors.sparse(100, (i until 100).map((_, 1.0)))
    }
    val dfB = spark.createDataFrame(dataB.map(Tuple1.apply)).toDF("keys")

    val bs = new BitSampling()
      .setSampleSize(3)
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(0)

    val (precision, recall) = LSHTest.calculateApproxSimilarityJoin(bs, dfA, dfB, 10.0)
    assert(precision == 1.0)
    assert(recall == 1.0)
  }
}
