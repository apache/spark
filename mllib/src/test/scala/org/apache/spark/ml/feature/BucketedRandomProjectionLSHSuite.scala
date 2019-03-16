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

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{Dataset, Row}

class BucketedRandomProjectionLSHSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val data = {
      for (i <- -10 until 10; j <- -10 until 10) yield Vectors.dense(i.toDouble, j.toDouble)
    }
    dataset = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")
  }

  test("params") {
    ParamsSuite.checkParams(new BucketedRandomProjectionLSH)
    val model = new BucketedRandomProjectionLSHModel(
      "brp", randUnitVectors = Array(Vectors.dense(1.0, 0.0)))
    ParamsSuite.checkParams(model)
  }

  test("setters") {
    val model = new BucketedRandomProjectionLSHModel("brp", Array(Vectors.dense(0.0, 1.0)))
      .setInputCol("testkeys")
      .setOutputCol("testvalues")
    assert(model.getInputCol  === "testkeys")
    assert(model.getOutputCol === "testvalues")
  }

  test("BucketedRandomProjectionLSH: default params") {
    val brp = new BucketedRandomProjectionLSH
    assert(brp.getNumHashTables === 1.0)
  }

  test("read/write") {
    def checkModelData(
      model: BucketedRandomProjectionLSHModel,
      model2: BucketedRandomProjectionLSHModel): Unit = {
      model.randUnitVectors.zip(model2.randUnitVectors)
        .foreach(pair => assert(pair._1 === pair._2))
    }
    val mh = new BucketedRandomProjectionLSH()
    val settings = Map("inputCol" -> "keys", "outputCol" -> "values", "bucketLength" -> 1.0)
    testEstimatorAndModelReadWrite(mh, dataset, settings, settings, checkModelData)
  }

  test("hashFunction") {
    val randUnitVectors = Array(Vectors.dense(0.0, 1.0), Vectors.dense(1.0, 0.0))
    val model = new BucketedRandomProjectionLSHModel("brp", randUnitVectors)
    model.set(model.bucketLength, 0.5)
    val res = model.hashFunction(Vectors.dense(1.23, 4.56))
    assert(res.length == 2)
    assert(res(0).equals(Vectors.dense(9.0)))
    assert(res(1).equals(Vectors.dense(2.0)))
  }

  test("keyDistance") {
    val model = new BucketedRandomProjectionLSHModel("brp", Array(Vectors.dense(0.0, 1.0)))
    val keyDist = model.keyDistance(Vectors.dense(1, 2), Vectors.dense(-2, -2))
    assert(keyDist === 5)
  }

  test("BucketedRandomProjectionLSH: randUnitVectors") {
    val brp = new BucketedRandomProjectionLSH()
      .setNumHashTables(20)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)
      .setSeed(12345)
    val brpModel = brp.fit(dataset)
    val unitVectors = brpModel.randUnitVectors
    unitVectors.foreach { v: Vector =>
      assert(Vectors.norm(v, 2.0) ~== 1.0 absTol 1e-14)
    }

    MLTestingUtils.checkCopyAndUids(brp, brpModel)
  }

  test("BucketedRandomProjectionLSH: streaming transform") {
    val brp = new BucketedRandomProjectionLSH()
      .setNumHashTables(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)
      .setSeed(12345)
    val brpModel = brp.fit(dataset)

    testTransformer[Tuple1[Vector]](dataset.toDF(), brpModel, "values") {
      case Row(values: Seq[_]) =>
        assert(values.length === brp.getNumHashTables)
    }
  }

  test("BucketedRandomProjectionLSH: test of LSH property") {
    // Project from 2 dimensional Euclidean Space to 1 dimensions
    val brp = new BucketedRandomProjectionLSH()
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)
      .setSeed(12345)

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(dataset, brp, 8.0, 2.0)
    assert(falsePositive < 0.4)
    assert(falseNegative < 0.4)
  }

  test("BucketedRandomProjectionLSH with high dimension data: test of LSH property") {
    val numDim = 100
    val data = {
      for (i <- 0 until numDim; j <- Seq(-2, -1, 1, 2))
        yield Vectors.sparse(numDim, Seq((i, j.toDouble)))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    // Project from 100 dimensional Euclidean Space to 10 dimensions
    val brp = new BucketedRandomProjectionLSH()
      .setNumHashTables(10)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(2.5)
      .setSeed(12345)

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(df, brp, 3.0, 2.0)
    assert(falsePositive < 0.3)
    assert(falseNegative < 0.3)
  }

  test("approxNearestNeighbors for bucketed random projection") {
    val key = Vectors.dense(1.2, 3.4)

    val brp = new BucketedRandomProjectionLSH()
      .setNumHashTables(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(4.0)
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxNearestNeighbors(brp, dataset, key, 100,
      singleProbe = true)
    assert(precision >= 0.6)
    assert(recall >= 0.6)
  }

  test("approxNearestNeighbors with multiple probing") {
    val key = Vectors.dense(1.2, 3.4)

    val brp = new BucketedRandomProjectionLSH()
      .setNumHashTables(20)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(1.0)
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxNearestNeighbors(brp, dataset, key, 100,
      singleProbe = false)
    assert(precision >= 0.7)
    assert(recall >= 0.7)
  }

  test("approxNearestNeighbors for numNeighbors <= 0") {
    val key = Vectors.dense(1.2, 3.4)

    val model = new BucketedRandomProjectionLSHModel(
      "brp", randUnitVectors = Array(Vectors.dense(1.0, 0.0)))

    intercept[IllegalArgumentException] {
      model.approxNearestNeighbors(dataset, key, 0)
    }
    intercept[IllegalArgumentException] {
      model.approxNearestNeighbors(dataset, key, -1)
    }
  }

  test("approxSimilarityJoin for bucketed random projection on different dataset") {
    val data2 = {
      for (i <- 0 until 24) yield Vectors.dense(10 * sin(Pi / 12 * i), 10 * cos(Pi / 12 * i))
    }
    val dataset2 = spark.createDataFrame(data2.map(Tuple1.apply)).toDF("keys")

    val brp = new BucketedRandomProjectionLSH()
      .setNumHashTables(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(4.0)
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxSimilarityJoin(brp, dataset, dataset2, 1.0)
    assert(precision == 1.0)
    assert(recall >= 0.7)
  }

  test("approxSimilarityJoin for self join") {
    val data = {
      for (i <- 0 until 24) yield Vectors.dense(10 * sin(Pi / 12 * i), 10 * cos(Pi / 12 * i))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    val brp = new BucketedRandomProjectionLSH()
      .setNumHashTables(2)
      .setInputCol("keys")
      .setOutputCol("values")
      .setBucketLength(4.0)
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxSimilarityJoin(brp, df, df, 3.0)
    assert(precision == 1.0)
    assert(recall >= 0.7)
  }
}
