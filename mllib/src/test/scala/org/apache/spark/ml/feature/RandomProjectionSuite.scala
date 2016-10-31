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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset

class RandomProjectionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val data = {
      for (i <- -10 until 10; j <- -10 until 10) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    dataset = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")
  }

  test("params") {
    ParamsSuite.checkParams(new RandomProjection)
    val model = new RandomProjectionModel("rp", randUnitVectors = Array(Vectors.dense(1.0, 0.0)))
    ParamsSuite.checkParams(model)
  }

  test("RandomProjection: default params") {
    val rp = new RandomProjection
    assert(rp.getOutputDim === 1.0)
  }

  test("read/write") {
    def checkModelData(model: RandomProjectionModel, model2: RandomProjectionModel): Unit = {
      model.randUnitVectors.zip(model2.randUnitVectors)
        .foreach(pair => assert(pair._1 === pair._2))
    }
    val mh = new RandomProjection()
    val settings = Map("inputCol" -> "keys", "outputCol" -> "values", "bucketLength" -> 1.0)
    testEstimatorAndModelReadWrite(mh, dataset, settings, checkModelData)
  }

  test("hashFunction") {
    val randUnitVectors = Array(Vectors.dense(0.0, 1.0), Vectors.dense(1.0, 0.0))
    val model = new RandomProjectionModel("rp", randUnitVectors)
    model.set(model.bucketLength, 0.5)
    val res = model.hashFunction(Vectors.dense(1.23, 4.56))
    assert(res.equals(Vectors.dense(9.0, 2.0)))
  }

  test("keyDistance and hashDistance") {
    val model = new RandomProjectionModel("rp", Array(Vectors.dense(0.0, 1.0)))
    val keyDist = model.keyDistance(Vectors.dense(1, 2), Vectors.dense(-2, -2))
    val hashDist = model.hashDistance(Vectors.dense(-5, 5), Vectors.dense(1, 2))
    assert(keyDist === 5)
    assert(hashDist === 3)
  }

  test("RandomProjection: randUnitVectors") {
    val rp = new RandomProjection()
      .setOutputDim(20)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)
      .setSeed(12345)
    val unitVectors = rp.fit(dataset).randUnitVectors
    unitVectors.foreach { v: Vector =>
      assert(Vectors.norm(v, 2.0) ~== 1.0 absTol 1e-14)
    }
  }

  test("RandomProjection: test of LSH property") {
    // Project from 2 dimensional Euclidean Space to 1 dimensions
    val rp = new RandomProjection()
      .setOutputDim(1)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)
      .setSeed(12345)

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(dataset, rp, 8.0, 2.0)
    assert(falsePositive < 0.4)
    assert(falseNegative < 0.4)
  }

  test("RandomProjection with high dimension data: test of LSH property") {
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
      .setSeed(12345)

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(df, rp, 3.0, 2.0)
    assert(falsePositive < 0.3)
    assert(falseNegative < 0.3)
  }

  test("approxNearestNeighbors for random projection") {
    val key = Vectors.dense(1.2, 3.4)

    val rp = new RandomProjection()
      .setOutputDim(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(4.0)
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxNearestNeighbors(rp, dataset, key, 100,
      singleProbing = true)
    assert(precision >= 0.6)
    assert(recall >= 0.6)
  }

  test("approxNearestNeighbors with multiple probing") {
    val key = Vectors.dense(1.2, 3.4)

    val rp = new RandomProjection()
      .setOutputDim(20)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxNearestNeighbors(rp, dataset, key, 100,
      singleProbing = false)
    assert(precision >= 0.7)
    assert(recall >= 0.7)
  }

  test("approxSimilarityJoin for random projection on different dataset") {
    val data2 = {
      for (i <- 0 until 24) yield Vectors.dense(10 * sin(Pi / 12 * i), 10 * cos(Pi / 12 * i))
    }
    val dataset2 = spark.createDataFrame(data2.map(Tuple1.apply)).toDF("keys")

    val rp = new RandomProjection()
      .setOutputDim(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(4.0)
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxSimilarityJoin(rp, dataset, dataset2, 1.0)
    assert(precision == 1.0)
    assert(recall >= 0.7)
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
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxSimilarityJoin(rp, df, df, 3.0)
    assert(precision == 1.0)
    assert(recall >= 0.7)
  }
}
