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

import org.dmg.pmml.PMML
import org.dmg.pmml.clustering.ClusteringModel

import org.apache.spark.SparkException
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils, PMMLReadWriteTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.clustering.{DistanceMeasure, KMeans => MLlibKMeans,
  KMeansModel => MLlibKMeansModel}
import org.apache.spark.mllib.linalg.{Vectors => MLlibVectors}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

private[clustering] case class TestRow(features: Vector)

class KMeansSuite extends MLTest with DefaultReadWriteTest with PMMLReadWriteTest {

  import testImplicits._

  final val k = 5
  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = KMeansSuite.generateKMeansData(spark, 50, 3, k)
  }

  test("default parameters") {
    val kmeans = new KMeans()

    assert(kmeans.getK === 2)
    assert(kmeans.getFeaturesCol === "features")
    assert(kmeans.getPredictionCol === "prediction")
    assert(kmeans.getMaxIter === 20)
    assert(kmeans.getInitMode === MLlibKMeans.K_MEANS_PARALLEL)
    assert(kmeans.getInitSteps === 2)
    assert(kmeans.getTol === 1e-4)
    assert(kmeans.getDistanceMeasure === DistanceMeasure.EUCLIDEAN)
    val model = kmeans.setMaxIter(1).fit(dataset)

    MLTestingUtils.checkCopyAndUids(kmeans, model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
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
      .setDistanceMeasure(DistanceMeasure.COSINE)

    assert(kmeans.getK === 9)
    assert(kmeans.getFeaturesCol === "test_feature")
    assert(kmeans.getPredictionCol === "test_prediction")
    assert(kmeans.getMaxIter === 33)
    assert(kmeans.getInitMode === MLlibKMeans.RANDOM)
    assert(kmeans.getInitSteps === 3)
    assert(kmeans.getSeed === 123)
    assert(kmeans.getTol === 1e-3)
    assert(kmeans.getDistanceMeasure === DistanceMeasure.COSINE)
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
    intercept[IllegalArgumentException] {
      new KMeans().setDistanceMeasure("no_such_a_measure")
    }
  }

  test("fit, transform and summary") {
    val predictionColName = "kmeans_prediction"
    val kmeans = new KMeans().setK(k).setPredictionCol(predictionColName).setSeed(1)
    val model = kmeans.fit(dataset)
    assert(model.clusterCenters.length === k)

    testTransformerByGlobalCheckFunc[Tuple1[Vector]](dataset.toDF(), model,
      "features", predictionColName) { rows =>
      val clusters = rows.map(_.getAs[Int](predictionColName)).toSet
      assert(clusters.size === k)
      assert(clusters === Set(0, 1, 2, 3, 4))
    }

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
    assert(summary.trainingCost < 0.1)
    val clusterSizes = summary.clusterSizes
    assert(clusterSizes.length === k)
    assert(clusterSizes.sum === numRows)
    assert(clusterSizes.forall(_ >= 0))
    assert(summary.numIter == 1)

    model.setSummary(None)
    assert(!model.hasSummary)
  }

  test("KMeansModel transform with non-default feature and prediction cols") {
    val featuresColName = "kmeans_model_features"
    val predictionColName = "kmeans_model_prediction"

    val model = new KMeans().setK(k).setSeed(1).fit(dataset)
    model.setFeaturesCol(featuresColName).setPredictionCol(predictionColName)

    val transformed = model.transform(dataset.withColumnRenamed("features", featuresColName))
    assert(transformed.schema.fieldNames.toSet === Set(featuresColName, predictionColName))
    assert(model.getFeaturesCol == featuresColName)
    assert(model.getPredictionCol == predictionColName)
  }

  test("KMeans using cosine distance") {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Array(
      Vectors.dense(1.0, 1.0),
      Vectors.dense(10.0, 10.0),
      Vectors.dense(1.0, 0.5),
      Vectors.dense(10.0, 4.4),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-100.0, 90.0)
    )).map(v => TestRow(v)))

    val model = new KMeans()
      .setK(3)
      .setSeed(42)
      .setInitMode(MLlibKMeans.RANDOM)
      .setTol(1e-6)
      .setDistanceMeasure(DistanceMeasure.COSINE)
      .fit(df)

    val predictionDf = model.transform(df)
    assert(predictionDf.select("prediction").distinct().count() == 3)
    val predictionsMap = predictionDf.collect().map(row =>
      row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
    assert(predictionsMap(Vectors.dense(1.0, 1.0)) ==
      predictionsMap(Vectors.dense(10.0, 10.0)))
    assert(predictionsMap(Vectors.dense(1.0, 0.5)) ==
      predictionsMap(Vectors.dense(10.0, 4.4)))
    assert(predictionsMap(Vectors.dense(-1.0, 1.0)) ==
      predictionsMap(Vectors.dense(-100.0, 90.0)))

    model.clusterCenters.forall(Vectors.norm(_, 2) == 1.0)
  }

  test("KMeans with cosine distance is not supported for 0-length vectors") {
    val model = new KMeans().setDistanceMeasure(DistanceMeasure.COSINE).setK(2)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Array(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(10.0, 10.0),
      Vectors.dense(1.0, 0.5)
    )).map(v => TestRow(v)))
    val e = intercept[SparkException](model.fit(df))
    assert(e.getCause.isInstanceOf[AssertionError])
    assert(e.getCause.getMessage.contains("Cosine distance is not defined"))
  }

  test("KMean with Array input") {
    def trainAndGetCost(dataset: Dataset[_]): Double = {
      val model = new KMeans().setK(k).setMaxIter(1).setSeed(1).fit(dataset)
      model.summary.trainingCost
    }

    val (newDataset, newDatasetD, newDatasetF) = MLTestingUtils.generateArrayFeatureDataset(dataset)
    val trueCost = trainAndGetCost(newDataset)
    val doubleArrayCost = trainAndGetCost(newDatasetD)
    val floatArrayCost = trainAndGetCost(newDatasetF)

    // checking the cost is fine enough as a sanity check
    assert(trueCost ~== doubleArrayCost absTol 1e-6)
    assert(trueCost ~== floatArrayCost absTol 1e-6)
  }


  test("read/write") {
    def checkModelData(model: KMeansModel, model2: KMeansModel): Unit = {
      assert(model.clusterCenters === model2.clusterCenters)
    }
    val kmeans = new KMeans()
    testEstimatorAndModelReadWrite(kmeans, dataset, KMeansSuite.allParamSettings,
      KMeansSuite.allParamSettings, checkModelData)
  }

  test("pmml export") {
    val clusterCenters = Array(
      MLlibVectors.dense(1.0, 2.0, 6.0),
      MLlibVectors.dense(1.0, 3.0, 0.0),
      MLlibVectors.dense(1.0, 4.0, 6.0))
    val oldKmeansModel = new MLlibKMeansModel(clusterCenters)
    val kmeansModel = new KMeansModel("", oldKmeansModel)
    def checkModel(pmml: PMML): Unit = {
      // Check the header description is what we expect
      assert(pmml.getHeader.getDescription === "k-means clustering")
      // check that the number of fields match the single vector size
      assert(pmml.getDataDictionary.getNumberOfFields === clusterCenters(0).size)
      // This verify that there is a model attached to the pmml object and the model is a clustering
      // one. It also verifies that the pmml model has the same number of clusters of the spark
      // model.
      val pmmlClusteringModel = pmml.getModels.get(0).asInstanceOf[ClusteringModel]
      assert(pmmlClusteringModel.getNumberOfClusters === clusterCenters.length)
    }
    testPMMLWrite(sc, kmeansModel, checkModel)
  }

  test("prediction on single instance") {
    val kmeans = new KMeans().setSeed(123L)
    val model = kmeans.fit(dataset)
    testClusteringModelSinglePrediction(model, model.predict, dataset,
      model.getFeaturesCol, model.getPredictionCol)
  }
}

object KMeansSuite {
  def generateKMeansData(spark: SparkSession, rows: Int, dim: Int, k: Int): DataFrame = {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(1 to rows).map(i => Vectors.dense(Array.fill(dim)((i % k).toDouble)))
      .map(v => new TestRow(v))
    spark.createDataFrame(rdd)
  }

  def generateSparseData(spark: SparkSession, rows: Int, dim: Int, seed: Int): DataFrame = {
    val sc = spark.sparkContext
    val random = new Random(seed)
    val nnz = random.nextInt(dim)
    val rdd = sc.parallelize(1 to rows)
      .map(i => Vectors.sparse(dim, random.shuffle(0 to dim - 1).slice(0, nnz).sorted.toArray,
        Array.fill(nnz)(random.nextDouble())))
      .map(v => new TestRow(v))
    spark.createDataFrame(rdd)
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
    "distanceMeasure" -> DistanceMeasure.EUCLIDEAN
  )
}
