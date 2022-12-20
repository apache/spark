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

import org.apache.spark.SparkException
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.clustering.DistanceMeasure
import org.apache.spark.sql.DataFrame


class BisectingKMeansSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  final val k = 5
  @transient var dataset: DataFrame = _
  @transient var sparseDataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = KMeansSuite.generateKMeansData(spark, 50, 3, k)
    sparseDataset = KMeansSuite.generateSparseData(spark, 10, 1000, 42)
  }

  test("default parameters") {
    val bkm = new BisectingKMeans()

    assert(bkm.getK === 4)
    assert(bkm.getFeaturesCol === "features")
    assert(bkm.getPredictionCol === "prediction")
    assert(bkm.getMaxIter === 20)
    assert(bkm.getMinDivisibleClusterSize === 1.0)
    val model = bkm.setMaxIter(1).fit(dataset)

    MLTestingUtils.checkCopyAndUids(bkm, model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
  }

  test("SPARK-16473: Verify Bisecting K-Means does not fail in edge case where" +
    "one cluster is empty after split") {
    val bkm = new BisectingKMeans()
      .setK(k)
      .setMinDivisibleClusterSize(4)
      .setMaxIter(4)
      .setSeed(123)

    // Verify fit does not fail on very sparse data
    val model = bkm.fit(sparseDataset)

    testTransformerByGlobalCheckFunc[Tuple1[Vector]](sparseDataset.toDF(), model, "prediction") {
      rows =>
        val numClusters = rows.distinct.length
        // Verify we hit the edge case
        assert(numClusters > 1)
    }
  }

  test("setter/getter") {
    val bkm = new BisectingKMeans()
      .setK(9)
      .setMinDivisibleClusterSize(2.0)
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")
      .setMaxIter(33)
      .setSeed(123)

    assert(bkm.getK === 9)
    assert(bkm.getFeaturesCol === "test_feature")
    assert(bkm.getPredictionCol === "test_prediction")
    assert(bkm.getMaxIter === 33)
    assert(bkm.getMinDivisibleClusterSize === 2.0)
    assert(bkm.getSeed === 123)

    intercept[IllegalArgumentException] {
      new BisectingKMeans().setK(1)
    }

    intercept[IllegalArgumentException] {
      new BisectingKMeans().setMinDivisibleClusterSize(0)
    }
  }

  test("fit, transform and summary") {
    val predictionColName = "bisecting_kmeans_prediction"
    val bkm = new BisectingKMeans().setK(k).setPredictionCol(predictionColName).setSeed(1)
    val model = bkm.fit(dataset)
    assert(model.clusterCenters.length === k)
    assert(model.summary.trainingCost < 0.1)
    assert(model.hasParent)

    testTransformerByGlobalCheckFunc[Tuple1[Vector]](dataset.toDF(), model,
      "features", predictionColName) { rows =>
      val clusters = rows.map(_.getAs[Int](predictionColName)).toSet
      assert(clusters.size === k)
      assert(clusters === Set(0, 1, 2, 3, 4))
    }

    // Check validity of model summary
    val numRows = dataset.count()
    assert(model.hasSummary)
    val summary: BisectingKMeansSummary = model.summary
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
    assert(summary.numIter == 20)
    assert(summary.trainingCost < 0.1)
    assert(model.summary.trainingCost == summary.trainingCost)

    model.setSummary(None)
    assert(!model.hasSummary)
  }

  test("read/write") {
    def checkModelData(model: BisectingKMeansModel, model2: BisectingKMeansModel): Unit = {
      assert(model.clusterCenters === model2.clusterCenters)
    }
    val bisectingKMeans = new BisectingKMeans()
    testEstimatorAndModelReadWrite(bisectingKMeans, dataset, BisectingKMeansSuite.allParamSettings,
      BisectingKMeansSuite.allParamSettings, checkModelData)
  }

  test("BisectingKMeans with cosine distance is not supported for 0-length vectors") {
    val model = new BisectingKMeans().setK(2).setDistanceMeasure(DistanceMeasure.COSINE).setSeed(1)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(10.0, 10.0),
      Vectors.dense(1.0, 0.5)
    )).map(v => TestRow(v)))
    val e = intercept[SparkException](model.fit(df))
    assert(e.getCause.isInstanceOf[AssertionError])
    assert(e.getCause.getMessage.contains("Cosine distance is not defined"))
  }

  test("BisectingKMeans with cosine distance") {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Vectors.dense(1.0, 1.0),
      Vectors.dense(10.0, 10.0),
      Vectors.dense(1.0, 0.5),
      Vectors.dense(10.0, 4.4),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-100.0, 90.0)
    )).map(v => TestRow(v)))
    val model = new BisectingKMeans()
      .setK(3)
      .setDistanceMeasure(DistanceMeasure.COSINE)
      .setSeed(1)
      .fit(df)
    val predictionDf = model.transform(df)
    checkNominalOnDF(predictionDf, "prediction", model.getK)

    assert(predictionDf.select("prediction").distinct().count() == 3)
    val predictionsMap = predictionDf.collect().map(row =>
      row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
    assert(predictionsMap(Vectors.dense(1.0, 1.0)) ==
      predictionsMap(Vectors.dense(10.0, 10.0)))
    assert(predictionsMap(Vectors.dense(1.0, 0.5)) ==
      predictionsMap(Vectors.dense(10.0, 4.4)))
    assert(predictionsMap(Vectors.dense(-1.0, 1.0)) ==
      predictionsMap(Vectors.dense(-100.0, 90.0)))

    assert(model.clusterCenters.forall(Vectors.norm(_, 2) ~== 1.0 absTol 1e-6))
  }

  test("Comparing with and without weightCol with cosine distance") {
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Vectors.dense(1.0, 1.0),
      Vectors.dense(10.0, 10.0),
      Vectors.dense(1.0, 0.5),
      Vectors.dense(10.0, 4.4),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-100.0, 90.0)
    )).map(v => TestRow(v)))

    val model1 = new BisectingKMeans()
      .setK(3)
      .setDistanceMeasure(DistanceMeasure.COSINE)
      .setSeed(1)
      .fit(df1)
    val predictionDf1 = model1.transform(df1)
    checkNominalOnDF(predictionDf1, "prediction", model1.getK)

    assert(predictionDf1.select("prediction").distinct().count() == 3)
    val predictionsMap1 = predictionDf1.collect().map(row =>
      row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
    assert(predictionsMap1(Vectors.dense(1.0, 1.0)) ==
      predictionsMap1(Vectors.dense(10.0, 10.0)))
    assert(predictionsMap1(Vectors.dense(1.0, 0.5)) ==
      predictionsMap1(Vectors.dense(10.0, 4.4)))
    assert(predictionsMap1(Vectors.dense(-1.0, 1.0)) ==
      predictionsMap1(Vectors.dense(-100.0, 90.0)))

    assert(model1.clusterCenters.forall(Vectors.norm(_, 2) ~== 1.0 absTol 1e-6))

    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      (Vectors.dense(1.0, 1.0), 2.0), (Vectors.dense(10.0, 10.0), 2.0),
      (Vectors.dense(1.0, 0.5), 2.0), (Vectors.dense(10.0, 4.4), 2.0),
      (Vectors.dense(-1.0, 1.0), 2.0), (Vectors.dense(-100.0, 90.0), 2.0))))
        .toDF("features", "weightCol")

    val model2 = new BisectingKMeans()
      .setK(3)
      .setDistanceMeasure(DistanceMeasure.COSINE)
      .setSeed(1)
      .setWeightCol("weightCol")
      .fit(df2)
    val predictionDf2 = model2.transform(df2)
    checkNominalOnDF(predictionDf2, "prediction", model2.getK)

    assert(predictionDf2.select("prediction").distinct().count() == 3)
    val predictionsMap2 = predictionDf2.collect().map(row =>
      row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
    assert(predictionsMap2(Vectors.dense(1.0, 1.0)) ==
      predictionsMap2(Vectors.dense(10.0, 10.0)))
    assert(predictionsMap2(Vectors.dense(1.0, 0.5)) ==
      predictionsMap2(Vectors.dense(10.0, 4.4)))
    assert(predictionsMap2(Vectors.dense(-1.0, 1.0)) ==
      predictionsMap2(Vectors.dense(-100.0, 90.0)))

    assert(model2.clusterCenters.forall(Vectors.norm(_, 2) ~== 1.0 absTol 1e-6))
    assert(model1.clusterCenters === model2.clusterCenters)
  }

  test("Comparing with and without weightCol") {
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Vectors.dense(1.0, 1.0),
      Vectors.dense(10.0, 10.0),
      Vectors.dense(10.0, 10.0),
      Vectors.dense(1.0, 0.5),
      Vectors.dense(1.0, 0.5),
      Vectors.dense(10.0, 4.4),
      Vectors.dense(10.0, 4.4),
      Vectors.dense(10.0, 4.4),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-100.0, 90.0),
      Vectors.dense(-100.0, 90.0),
      Vectors.dense(-100.0, 90.0),
      Vectors.dense(-100.0, 90.0)
    )).map(v => TestRow(v)))

    val model1 = new BisectingKMeans()
      .setK(3)
      .setSeed(1)
      .fit(df1)
    val predictionDf1 = model1.transform(df1)
    checkNominalOnDF(predictionDf1, "prediction", model1.getK)

    assert(predictionDf1.select("prediction").distinct().count() == 3)
    val predictionsMap1 = predictionDf1.collect().map(row =>
      row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
    assert(predictionsMap1(Vectors.dense(1.0, 1.0)) ==
      predictionsMap1(Vectors.dense(1.0, 0.5)))
    assert(predictionsMap1(Vectors.dense(1.0, 1.0)) ==
      predictionsMap1(Vectors.dense(-1.0, 1.0)))
    assert(predictionsMap1(Vectors.dense(10.0, 10.0)) ==
      predictionsMap1(Vectors.dense(10.0, 4.4)))

    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      (Vectors.dense(1.0, 1.0), 1.0), (Vectors.dense(10.0, 10.0), 2.0),
      (Vectors.dense(1.0, 0.5), 2.0), (Vectors.dense(10.0, 4.4), 3.0),
      (Vectors.dense(-1.0, 1.0), 3.0), (Vectors.dense(-100.0, 90.0), 4.0))))
      .toDF("features", "weightCol")

    val model2 = new BisectingKMeans()
      .setK(3)
      .setSeed(1)
      .setWeightCol("weightCol")
      .fit(df2)
    val predictionDf2 = model2.transform(df2)
    checkNominalOnDF(predictionDf2, "prediction", model2.getK)

    assert(predictionDf2.select("prediction").distinct().count() == 3)
    val predictionsMap2 = predictionDf2.collect().map(row =>
      row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
    assert(predictionsMap2(Vectors.dense(1.0, 1.0)) ==
      predictionsMap2(Vectors.dense(1.0, 0.5)))
    assert(predictionsMap2(Vectors.dense(1.0, 1.0)) ==
      predictionsMap2(Vectors.dense(-1.0, 1.0)))
    assert(predictionsMap2(Vectors.dense(10.0, 10.0)) ==
      predictionsMap2(Vectors.dense(10.0, 4.4)))

    assert(model1.clusterCenters(0) === model2.clusterCenters(0))
    assert(model1.clusterCenters(1) === model2.clusterCenters(1))
    assert(model1.clusterCenters(2) ~== model2.clusterCenters(2) absTol 1e-6)
  }

  test("BisectingKMeans with Array input") {
    def trainAndComputeCost(dataset: DataFrame): Double = {
      val model = new BisectingKMeans().setK(k).setMaxIter(1).setSeed(1).fit(dataset)
      model.summary.trainingCost
    }

    val (newDataset, newDatasetD, newDatasetF) = MLTestingUtils.generateArrayFeatureDataset(dataset)
    val trueCost = trainAndComputeCost(newDataset)
    val doubleArrayCost = trainAndComputeCost(newDatasetD)
    val floatArrayCost = trainAndComputeCost(newDatasetF)

    // checking the cost is fine enough as a sanity check
    assert(trueCost ~== doubleArrayCost absTol 1e-6)
    assert(trueCost ~== floatArrayCost absTol 1e-6)
  }

  test("prediction on single instance") {
    val bikm = new BisectingKMeans().setSeed(123L)
    val model = bikm.fit(dataset)
    testClusteringModelSinglePrediction(model, model.predict, dataset,
      model.getFeaturesCol, model.getPredictionCol)
  }
}

object BisectingKMeansSuite {
  val allParamSettings: Map[String, Any] = Map(
    "k" -> 3,
    "maxIter" -> 2,
    "seed" -> -1L,
    "minDivisibleClusterSize" -> 2.0
  )
}
