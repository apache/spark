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
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset

class BisectingKMeansSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  final val k = 5
  @transient var dataset: Dataset[_] = _

  @transient var sparseDataset: Dataset[_] = _

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

    // copied model must have the same parent
    MLTestingUtils.checkCopy(model)
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
    val result = model.transform(sparseDataset)
    val numClusters = result.select("prediction").distinct().collect().length
    // Verify we hit the edge case
    assert(numClusters < k && numClusters > 1)
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

    model.setSummary(None)
    assert(!model.hasSummary)
  }

  test("read/write") {
    def checkModelData(model: BisectingKMeansModel, model2: BisectingKMeansModel): Unit = {
      assert(model.clusterCenters === model2.clusterCenters)
    }
    val bisectingKMeans = new BisectingKMeans()
    testEstimatorAndModelReadWrite(
      bisectingKMeans, dataset, BisectingKMeansSuite.allParamSettings, checkModelData)
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
