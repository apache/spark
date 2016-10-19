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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans, KMeansModel => MLlibKMeansModel}
import org.apache.spark.mllib.linalg.{Vectors => MLlibVectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

private[clustering] case class TestRow(features: Vector)

class KMeansSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  final val k: Int = 5
  final val dim: Int = 3
  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = KMeansSuite.generateKMeansData(spark, 50, dim, k)
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

  test("fit, transform, and summary") {
    val predictionColName = "kmeans_prediction"
    val kmeans = new KMeans().setK(k).setPredictionCol(predictionColName).setSeed(1)
    val model = kmeans.fit(dataset)
    assert(model.clusterCenters.length === k)

    val transformed = model.transform(dataset)
    val expectedColumns = Array("features", predictionColName)
    expectedColumns.foreach { column =>
      assert(transformed.columns.contains(column))
    }
    val clusters =
      transformed.select(predictionColName).rdd.map(_.getInt(0)).distinct().collect().toSet
    assert(clusters.size === k)
    assert(clusters === Set(0, 1, 2, 3, 4))
    assert(model.computeCost(dataset) < 0.1)
    assert(model.hasParent)

    // Check validity of model summary
    val numRows = dataset.count()
    assert(model.hasSummary)
    val summary: KMeansSummary = model.summary
    assert(summary.predictionCol === predictionColName)
    assert(summary.featuresCol === "features")
    assert(summary.predictions.count() === numRows)
    for (c <- Array(predictionColName, "features")) {
      assert(summary.predictions.columns.contains(c))
    }
    assert(summary.cluster.columns === Array(predictionColName))
    val clusterSizes = summary.clusterSizes
    assert(clusterSizes.length === k)
    assert(clusterSizes.sum === numRows)
    assert(clusterSizes.forall(_ >= 0))
  }

  test("KMeansModel transform with non-default feature and prediction cols") {
    val featuresColName = "kmeans_model_features"
    val predictionColName = "kmeans_model_prediction"

    val model = new KMeans().setK(k).setSeed(1).fit(dataset)
    model.setFeaturesCol(featuresColName).setPredictionCol(predictionColName)

    val transformed = model.transform(dataset.withColumnRenamed("features", featuresColName))
    Seq(featuresColName, predictionColName).foreach { column =>
      assert(transformed.columns.contains(column))
    }
    assert(model.getFeaturesCol == featuresColName)
    assert(model.getPredictionCol == predictionColName)
  }

  test("read/write") {
    def checkModelData(model: KMeansModel, model2: KMeansModel): Unit = {
      assert(model.clusterCenters === model2.clusterCenters)
    }
    val kmeans = new KMeans()
    testEstimatorAndModelReadWrite(kmeans, dataset, KMeansSuite.allParamSettings, checkModelData,
      Map("initialModel" -> (checkModelData _).asInstanceOf[(Any, Any) => Unit]))
  }

  test("Initialize using a trained model") {
    val kmeans = new KMeans().setK(k).setSeed(1).setMaxIter(1)
    val oneIterModel = kmeans.fit(dataset)
    val twoIterModel = kmeans.copy(ParamMap(ParamPair(kmeans.maxIter, 2))).fit(dataset)
    val oneMoreIterModel = kmeans.setInitialModel(oneIterModel).fit(dataset)

    assert(oneMoreIterModel.getK === k)

    twoIterModel.clusterCenters.zip(oneMoreIterModel.clusterCenters)
      .foreach { case (center1, center2) => assert(center1 ~== center2 absTol 1E-8) }
  }

  test("Initialize using a model with wrong dimension of cluster centers") {
    val kmeans = new KMeans().setK(k).setSeed(1).setMaxIter(1)

    val wrongDimModel = KMeansSuite.generateRandomKMeansModel(4, k)
    val wrongDimModelThrown = intercept[IllegalArgumentException] {
      kmeans.setInitialModel(wrongDimModel).fit(dataset)
    }
    assert(wrongDimModelThrown.getMessage.contains("mismatched dimension"))
  }

  test("Infer K from an initial model") {
    val kmeans = new KMeans().setK(5)
    val testNewK = 10
    val randomModel = KMeansSuite.generateRandomKMeansModel(dim, testNewK)
    assert(kmeans.setInitialModel(randomModel).getK === testNewK)
  }

  test("Ignore k if initialModel is set") {
    val kmeans = new KMeans()

    val randomModel = KMeansSuite.generateRandomKMeansModel(dim, k)
    // ignore k if initialModel is set
    assert(kmeans.setInitialModel(randomModel).setK(k - 1).getK === k)
    kmeans.clear(kmeans.initialModel)
    // k is not ignored after initialModel is cleared
    assert(kmeans.setK(k - 1).getK === k - 1)
  }

  test("Eliminate possible initialModels of the direct initialModel") {
    val randomModel = KMeansSuite.generateRandomKMeansModel(dim, k)
    val kmeans = new KMeans().setK(k).setMaxIter(1).setInitialModel(randomModel)
    val firstLevelModel = kmeans.fit(dataset)
    val secondLevelModel = kmeans.setInitialModel(firstLevelModel).fit(dataset)
    assert(secondLevelModel.getInitialModel
      .isSet(secondLevelModel.getInitialModel.getParam("initialModel")))
    val savedThenLoadedModel = testDefaultReadWrite(secondLevelModel, testParams = false)
    assert(!savedThenLoadedModel.getInitialModel
      .isSet(savedThenLoadedModel.getInitialModel.getParam("initialModel")))
  }
}

object KMeansSuite {

  def generateKMeansData(spark: SparkSession, rows: Int, dim: Int, k: Int): DataFrame = {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(1 to rows).map(i => Vectors.dense(Array.fill(dim)((i % k).toDouble)))
      .map(v => TestRow(v))
    spark.createDataFrame(rdd)
  }

  def generateRandomKMeansModel(dim: Int, k: Int, seed: Int = 42): KMeansModel = {
    val rng = new Random(seed)
    val clusterCenters = (1 to k)
      .map(i => MLlibVectors.dense(Array.fill(dim)(rng.nextDouble)))
    new KMeansModel("test model", new MLlibKMeansModel(clusterCenters.toArray))
  }

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "k" -> 3,
    "maxIter" -> 2,
    "tol" -> 0.01,
    "initialModel" -> generateRandomKMeansModel(3, 3)
  )
}
