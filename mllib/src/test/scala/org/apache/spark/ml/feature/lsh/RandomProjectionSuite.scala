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

package org.apache.spark.ml.feature.lsh

import breeze.numerics.{cos, sin}
import breeze.numerics.constants.Pi

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RandomProjectionSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("RandomProjection") {
    val data = {
      for (i <- -5 until 5; j <- -5 until 5) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    // Project from 2 dimensional Euclidean Space to 1 dimensions
    val rp = new RandomProjection()
      .setOutputDim(1)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)

    val (falsePositive, falseNegative) = LSHTest.checkLSHProperty(df, rp, 8.0, 2.0)
    assert(falsePositive < 0.1)
    assert(falseNegative < 0.1)
  }

  test("RandomProjection with high dimension data") {
    val numDim = 100
    val data = {
      for (i <- 0 until numDim; j <- Seq(-2, -1, 1, 2))
        yield Vectors.sparse(numDim, Seq((i, j.toDouble)))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    // Project from 100 dimensional Euclidean Space to 10 dimensions
    val rp = new RandomProjection()
      .setOutputDim(10)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(2.5)

    val (falsePositive, falseNegative) = LSHTest.checkLSHProperty(df, rp, 3.0, 2.0)
    assert(falsePositive < 0.1)
    assert(falseNegative < 0.1)
  }

  test("approxNearestNeighbors for random projection") {
    val data = {
      for (i <- -10 until 10; j <- -10 until 10) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")
    val key = Vectors.dense(1.2, 3.4)

    val rp = new RandomProjection()
      .setOutputDim(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(4.0)

    val (precision, recall) = LSHTest.checkApproxNearestNeighbors(rp, df, key, 10)
    assert(precision >= 0.7)
    assert(recall >= 0.7)
  }

  test("approxNearestNeighbors for small bucket and large k") {
    val data = {
      for (i <- -10 until 10; j <- -10 until 10) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")
    val key = Vectors.dense(1.2, 3.4)

    val rp = new RandomProjection()
      .setOutputDim(20)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)

    val (precision, recall) = LSHTest.checkApproxNearestNeighbors(rp, df, key, 100)
    assert(precision >= 0.7)
    assert(recall >= 0.7)
  }

  test("approxSimilarityJoin for random projection on different dataset") {
    val dataA = {
      for (i <- -10 until 10; j <- -10 until 10) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    val dfA = spark.createDataFrame(dataA.map(Tuple1.apply)).toDF("keys")

    val dataB = {
      for (i <- 0 until 24) yield Vectors.dense(10 * sin(Pi / 12 * i), 10 * cos(Pi / 12 * i))
    }
    val dfB = spark.createDataFrame(dataB.map(Tuple1.apply)).toDF("keys")

    val rp = new RandomProjection()
      .setOutputDim(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(4.0)

    val (precision, recall) = LSHTest.checkApproxSimilarityJoin(rp, dfA, dfB, 1.0)
    assert(precision == 1.0)
    assert(recall >= 0.9)
  }

  test("approxSimilarityJoin for self join") {
    val data = {
      for (i <- 0 until 24) yield Vectors.dense(10 * sin(Pi / 12 * i), 10 * cos(Pi / 12 * i))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    val rp = new RandomProjection()
      .setOutputDim(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(4.0)

    val (precision, recall) = LSHTest.checkApproxSimilarityJoin(rp, df, df, 3.0)
    assert(precision == 1.0)
    assert(recall >= 0.7)
  }
}
