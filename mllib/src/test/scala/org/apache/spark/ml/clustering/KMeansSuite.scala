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
import org.apache.spark.mllib.clustering.{DistanceMeasure, KMeans => MLlibKMeans, KMeansModel => MLlibKMeansModel}
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
    assert(kmeans.getInitMode === KMeans.K_MEANS_PARALLEL)
    assert(kmeans.getInitSteps === 2)
    assert(kmeans.getTol === 1e-4)
    assert(kmeans.getSolver === KMeans.AUTO)
    assert(kmeans.getDistanceMeasure === KMeans.EUCLIDEAN)
    val model = kmeans.setMaxIter(1).fit(dataset)

    val transformed = model.transform(dataset)
    checkNominalOnDF(transformed, "prediction", model.clusterCenters.length)

    MLTestingUtils.checkCopyAndUids(kmeans, model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
  }

  test("KMeans validate input dataset") {
    testInvalidWeights(new KMeans().setWeightCol("weight").fit(_))
    testInvalidVectors(new KMeans().fit(_))
  }

  test("set parameters") {
    val kmeans = new KMeans()
      .setK(9)
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")
      .setMaxIter(33)
      .setInitMode(KMeans.RANDOM)
      .setInitSteps(3)
      .setSeed(123)
      .setTol(1e-3)
      .setDistanceMeasure(KMeans.COSINE)

    assert(kmeans.getK === 9)
    assert(kmeans.getFeaturesCol === "test_feature")
    assert(kmeans.getPredictionCol === "test_prediction")
    assert(kmeans.getMaxIter === 33)
    assert(kmeans.getInitMode === KMeans.RANDOM)
    assert(kmeans.getInitSteps === 3)
    assert(kmeans.getSeed === 123)
    assert(kmeans.getTol === 1e-3)
    assert(kmeans.getDistanceMeasure === KMeans.COSINE)
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
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Vectors.dense(1.0, 1.0),
      Vectors.dense(10.0, 10.0),
      Vectors.dense(1.0, 0.5),
      Vectors.dense(10.0, 4.4),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-100.0, 90.0)
    )).map(v => TestRow(v)))

    Seq(KMeans.AUTO, KMeans.ROW, KMeans.BLOCK).foreach { solver =>
      val model = new KMeans()
        .setK(3)
        .setSeed(42)
        .setInitMode(MLlibKMeans.RANDOM)
        .setTol(1e-6)
        .setDistanceMeasure(DistanceMeasure.COSINE)
        .setSolver(solver)
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

      assert(model.clusterCenters.forall(Vectors.norm(_, 2) ~== 1.0 absTol 1e-6))
    }
  }

  test("KMeans with cosine distance is not supported for 0-length vectors") {
    val model = new KMeans().setDistanceMeasure(DistanceMeasure.COSINE).setK(2)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
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

  test("compare with weightCol and without weightCol") {
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Vectors.dense(1.0, 1.0),
      Vectors.dense(10.0, 10.0), Vectors.dense(10.0, 10.0),
      Vectors.dense(1.0, 0.5),
      Vectors.dense(10.0, 4.4), Vectors.dense(10.0, 4.4),
      Vectors.dense(-1.0, 1.0),
      Vectors.dense(-100.0, 90.0), Vectors.dense(-100.0, 90.0)
    )).map(v => TestRow(v)))

    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      (Vectors.dense(1.0, 1.0), 1.0),
      (Vectors.dense(10.0, 10.0), 2.0),
      (Vectors.dense(1.0, 0.5), 1.0),
      (Vectors.dense(10.0, 4.4), 2.0),
      (Vectors.dense(-1.0, 1.0), 1.0),
      (Vectors.dense(-100.0, 90.0), 2.0)))).toDF("features", "weightCol")

    Seq(KMeans.AUTO, KMeans.ROW, KMeans.BLOCK).foreach { solver =>
      val model1 = new KMeans()
        .setK(3)
        .setSeed(42)
        .setInitMode(MLlibKMeans.RANDOM)
        .setTol(1e-6)
        .setSolver(solver)
        .setDistanceMeasure(DistanceMeasure.COSINE)
        .fit(df1)

      val predictionDf1 = model1.transform(df1)
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

      val model2 = new KMeans()
        .setK(3)
        .setSeed(42)
        .setInitMode(MLlibKMeans.RANDOM)
        .setTol(1e-6)
        .setSolver(solver)
        .setDistanceMeasure(DistanceMeasure.COSINE)
        .setWeightCol("weightCol")
        .fit(df2)

      val predictionDf2 = model2.transform(df2)
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

      // compare if model1 and model2 have the same cluster centers
      assert(model1.clusterCenters.length === model2.clusterCenters.length)
      assert(model1.clusterCenters.toSet.subsetOf((model2.clusterCenters.toSet)))
    }
  }

  test("Two centers with weightCol") {
    // use the same weight for all samples.
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      (Vectors.dense(0.0, 0.0), 2.0),
      (Vectors.dense(0.0, 0.1), 2.0),
      (Vectors.dense(0.1, 0.0), 2.0),
      (Vectors.dense(9.0, 0.0), 2.0),
      (Vectors.dense(9.0, 0.2), 2.0),
      (Vectors.dense(9.2, 0.0), 2.0)))).toDF("features", "weightCol")


    // use different weight
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      (Vectors.dense(0.0, 0.0), 1.0),
      (Vectors.dense(0.0, 0.1), 2.0),
      (Vectors.dense(0.1, 0.0), 3.0),
      (Vectors.dense(9.0, 0.0), 2.5),
      (Vectors.dense(9.0, 0.2), 1.0),
      (Vectors.dense(9.2, 0.0), 2.0)))).toDF("features", "weightCol")

    Seq(KMeans.AUTO, KMeans.ROW, KMeans.BLOCK).foreach { solver =>
      val model1 = new KMeans()
        .setK(2)
        .setInitMode(MLlibKMeans.RANDOM)
        .setWeightCol("weightCol")
        .setMaxIter(10)
        .setSolver(solver)
        .fit(df1)

      val predictionDf1 = model1.transform(df1)
      assert(predictionDf1.select("prediction").distinct().count() == 2)
      val predictionsMap1 = predictionDf1.collect().map(row =>
        row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
      assert(predictionsMap1(Vectors.dense(0.0, 0.0)) ==
        predictionsMap1(Vectors.dense(0.0, 0.1)))
      assert(predictionsMap1(Vectors.dense(0.0, 0.0)) ==
        predictionsMap1(Vectors.dense(0.1, 0.0)))
      assert(predictionsMap1(Vectors.dense(9.0, 0.0)) ==
        predictionsMap1(Vectors.dense(9.0, 0.2)))
      assert(predictionsMap1(Vectors.dense(9.0, 0.2)) ==
        predictionsMap1(Vectors.dense(9.2, 0.0)))

      // center 1:
      // total weights in cluster 1: 2.0 + 2.0 + 2.0 = 6.0
      // x: 9.0 * (2.0/6.0) + 9.0 * (2.0/6.0) + 9.2 * (2.0/6.0) = 9.066666666666666
      // y: 0.0 * (2.0/6.0) + 0.2 * (2.0/6.0) + 0.0 * (2.0/6.0) = 0.06666666666666667
      // center 2:
      // total weights in cluster 2: 2.0 + 2.0 + 2.0 = 6.0
      // x: 0.0 * (2.0/6.0) + 0.0 * (2.0/6.0) + 0.1 * (2.0/6.0) = 0.03333333333333333
      // y: 0.0 * (2.0/6.0) + 0.1 * (2.0/6.0) + 0.0 * (2.0/6.0) = 0.03333333333333333
      val model1_center1 = Vectors.dense(9.066666666666666, 0.06666666666666667)
      val model1_center2 = Vectors.dense(0.03333333333333333, 0.03333333333333333)
      assert(model1.clusterCenters(0) === model1_center1)
      assert(model1.clusterCenters(1) === model1_center2)

      val model2 = new KMeans()
        .setK(2)
        .setInitMode(MLlibKMeans.RANDOM)
        .setWeightCol("weightCol")
        .setMaxIter(10)
        .setSolver(solver)
        .fit(df2)

      val predictionDf2 = model2.transform(df2)
      assert(predictionDf2.select("prediction").distinct().count() == 2)
      val predictionsMap2 = predictionDf2.collect().map(row =>
        row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
      assert(predictionsMap2(Vectors.dense(0.0, 0.0)) ==
        predictionsMap2(Vectors.dense(0.0, 0.1)))
      assert(predictionsMap2(Vectors.dense(0.0, 0.0)) ==
        predictionsMap2(Vectors.dense(0.1, 0.0)))
      assert(predictionsMap2(Vectors.dense(9.0, 0.0)) ==
        predictionsMap2(Vectors.dense(9.0, 0.2)))
      assert(predictionsMap2(Vectors.dense(9.0, 0.2)) ==
        predictionsMap2(Vectors.dense(9.2, 0.0)))

      // center 1:
      // total weights in cluster 1: 2.5 + 1.0 + 2.0 = 5.5
      // x: 9.0 * (2.5/5.5) + 9.0 * (1.0/5.5) + 9.2 * (2.0/5.5) = 9.072727272727272
      // y: 0.0 * (2.5/5.5) + 0.2 * (1.0/5.5) + 0.0 * (2.0/5.5) = 0.03636363636363637
      // center 2:
      // total weights in cluster 2: 1.0 + 2.0 + 3.0 = 6.0
      // x: 0.0 * (1.0/6.0) + 0.0 * (2.0/6.0) + 0.1 * (3.0/6.0) = 0.05
      // y: 0.0 * (1.0/6.0) + 0.1 * (2.0/6.0) + 0.0 * (3.0/6.0) = 0.03333333333333333
      val model2_center1 = Vectors.dense(9.072727272727272, 0.03636363636363637)
      val model2_center2 = Vectors.dense(0.05, 0.03333333333333333)
      assert(model2.clusterCenters(0) === model2_center1)
      assert(model2.clusterCenters(1) === model2_center2)
    }
  }

  test("Four centers with weightCol") {
    // no weight
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Vectors.dense(0.1, 0.1),
      Vectors.dense(5.0, 0.2),
      Vectors.dense(10.0, 0.0),
      Vectors.dense(15.0, 0.5),
      Vectors.dense(32.0, 18.0),
      Vectors.dense(30.1, 20.0),
      Vectors.dense(-6.0, -6.0),
      Vectors.dense(-10.0, -10.0))).map(v => TestRow(v)))

    // use same weight, should have the same result as no weight
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      (Vectors.dense(0.1, 0.1), 2.0),
      (Vectors.dense(5.0, 0.2), 2.0),
      (Vectors.dense(10.0, 0.0), 2.0),
      (Vectors.dense(15.0, 0.5), 2.0),
      (Vectors.dense(32.0, 18.0), 2.0),
      (Vectors.dense(30.1, 20.0), 2.0),
      (Vectors.dense(-6.0, -6.0), 2.0),
      (Vectors.dense(-10.0, -10.0), 2.0)))).toDF("features", "weightCol")

    Seq(KMeans.AUTO, KMeans.ROW, KMeans.BLOCK).foreach { solver =>

      val model1 = new KMeans()
        .setK(4)
        .setInitMode(MLlibKMeans.K_MEANS_PARALLEL)
        .setMaxIter(10)
        .setSolver(solver)
        .fit(df1)

      val predictionDf1 = model1.transform(df1)
      assert(predictionDf1.select("prediction").distinct().count() == 4)
      val predictionsMap1 = predictionDf1.collect().map(row =>
        row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
      assert(predictionsMap1(Vectors.dense(0.1, 0.1)) ==
        predictionsMap1(Vectors.dense(5.0, 0.2)) )
      assert(predictionsMap1(Vectors.dense(10.0, 0.0)) ==
        predictionsMap1(Vectors.dense(15.0, 0.5)) )
      assert(predictionsMap1(Vectors.dense(32.0, 18.0)) ==
        predictionsMap1(Vectors.dense(30.1, 20.0)))
      assert(predictionsMap1(Vectors.dense(-6.0, -6.0)) ==
        predictionsMap1(Vectors.dense(-10.0, -10.0)))

      val model2 = new KMeans()
        .setK(4)
        .setInitMode(MLlibKMeans.K_MEANS_PARALLEL)
        .setWeightCol("weightCol")
        .setMaxIter(10)
        .setSolver(solver)
        .fit(df2)

      val predictionDf2 = model2.transform(df2)
      assert(predictionDf2.select("prediction").distinct().count() == 4)
      val predictionsMap2 = predictionDf2.collect().map(row =>
        row.getAs[Vector]("features") -> row.getAs[Int]("prediction")).toMap
      assert(predictionsMap2(Vectors.dense(0.1, 0.1)) ==
        predictionsMap2(Vectors.dense(5.0, 0.2)))
      assert(predictionsMap2(Vectors.dense(10.0, 0.0)) ==
        predictionsMap2(Vectors.dense(15.0, 0.5)))
      assert(predictionsMap2(Vectors.dense(32.0, 18.0)) ==
        predictionsMap2(Vectors.dense(30.1, 20.0)))
      assert(predictionsMap2(Vectors.dense(-6.0, -6.0)) ==
        predictionsMap2(Vectors.dense(-10.0, -10.0)))

      assert(model1.clusterCenters === model2.clusterCenters)
    }
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
