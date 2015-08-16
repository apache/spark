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

package org.apache.spark.ml.clustering

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

private[clustering] case class TestRow(features: Vector)

object KMeansSuite {
  def generateKMeansData(sql: SQLContext, rows: Int, dim: Int, k: Int): DataFrame = {
    val sc = sql.sparkContext
    val rdd = sc.parallelize(1 to rows).map(i => Vectors.dense(Array.fill(dim)((i % k).toDouble)))
      .map(v => new TestRow(v))
    sql.createDataFrame(rdd)
  }
}

class KMeansSuite extends SparkFunSuite with MLlibTestSparkContext {

  final val k = 5
  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = KMeansSuite.generateKMeansData(sqlContext, 50, 3, k)
  }

  test("default parameters") {
    val kmeans = new KMeans()

    assert(kmeans.getK === 2)
    assert(kmeans.getFeaturesCol === "features")
    assert(kmeans.getPredictionCol === "prediction")
    assert(kmeans.getMaxIter === 20)
    assert(kmeans.getInitMode === MLlibKMeans.K_MEANS_PARALLEL)
    assert(kmeans.getInitSteps === 5)
    assert(kmeans.getTol === 1e-4)
  }

  test("set parameters") {
    val kmeans = new KMeans()
      .setK(9)
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")
      .setMaxIter(33)
      .setInitMode(MLlibKMeans.RANDOM)
      .setInitSteps(3)
      .setSeed(123)
      .setTol(1e-3)

    assert(kmeans.getK === 9)
    assert(kmeans.getFeaturesCol === "test_feature")
    assert(kmeans.getPredictionCol === "test_prediction")
    assert(kmeans.getMaxIter === 33)
    assert(kmeans.getInitMode === MLlibKMeans.RANDOM)
    assert(kmeans.getInitSteps === 3)
    assert(kmeans.getSeed === 123)
    assert(kmeans.getTol === 1e-3)
  }

  test("parameters validation") {
    intercept[IllegalArgumentException] {
      new KMeans().setK(1)
    }
    intercept[IllegalArgumentException] {
      new KMeans().setInitMode("no_such_a_mode")
    }
    intercept[IllegalArgumentException] {
      new KMeans().setInitSteps(0)
    }
  }

  test("fit & transform") {
    val predictionColName = "kmeans_prediction"
    val kmeans = new KMeans().setK(k).setPredictionCol(predictionColName).setSeed(1)
    val model = kmeans.fit(dataset)
    assert(model.clusterCenters.length === k)

    val transformed = model.transform(dataset)
    val expectedColumns = Array("features", predictionColName)
    expectedColumns.foreach { column =>
      assert(transformed.columns.contains(column))
    }
    val clusters = transformed.select(predictionColName).map(_.getInt(0)).distinct().collect().toSet
    assert(clusters.size === k)
    assert(clusters === Set(0, 1, 2, 3, 4))
  }
}
