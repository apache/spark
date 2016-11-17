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


class GaussianMixtureSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  final val k = 5
  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = KMeansSuite.generateKMeansData(spark, 50, 3, k)
  }

  test("default parameters") {
    val gm = new GaussianMixture()

    assert(gm.getK === 2)
    assert(gm.getFeaturesCol === "features")
    assert(gm.getPredictionCol === "prediction")
    assert(gm.getMaxIter === 100)
    assert(gm.getTol === 0.01)
    val model = gm.setMaxIter(1).fit(dataset)

    // copied model must have the same parent
    MLTestingUtils.checkCopy(model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
  }

  test("set parameters") {
    val gm = new GaussianMixture()
      .setK(9)
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")
      .setProbabilityCol("test_probability")
      .setMaxIter(33)
      .setSeed(123)
      .setTol(1e-3)

    assert(gm.getK === 9)
    assert(gm.getFeaturesCol === "test_feature")
    assert(gm.getPredictionCol === "test_prediction")
    assert(gm.getProbabilityCol === "test_probability")
    assert(gm.getMaxIter === 33)
    assert(gm.getSeed === 123)
    assert(gm.getTol === 1e-3)
  }

  test("parameters validation") {
    intercept[IllegalArgumentException] {
      new GaussianMixture().setK(1)
    }
  }

  test("fit, transform and summary") {
    val predictionColName = "gm_prediction"
    val probabilityColName = "gm_probability"
    val gm = new GaussianMixture().setK(k).setMaxIter(2).setPredictionCol(predictionColName)
        .setProbabilityCol(probabilityColName).setSeed(1)
    val model = gm.fit(dataset)
    assert(model.hasParent)
    assert(model.weights.length === k)
    assert(model.gaussians.length === k)

    val transformed = model.transform(dataset)
    val expectedColumns = Array("features", predictionColName, probabilityColName)
    expectedColumns.foreach { column =>
      assert(transformed.columns.contains(column))
    }

    // Check validity of model summary
    val numRows = dataset.count()
    assert(model.hasSummary)
    val summary: GaussianMixtureSummary = model.summary
    assert(summary.predictionCol === predictionColName)
    assert(summary.probabilityCol === probabilityColName)
    assert(summary.featuresCol === "features")
    assert(summary.predictions.count() === numRows)
    for (c <- Array(predictionColName, probabilityColName, "features")) {
      assert(summary.predictions.columns.contains(c))
    }
    assert(summary.cluster.columns === Array(predictionColName))
    assert(summary.probability.columns === Array(probabilityColName))
    val clusterSizes = summary.clusterSizes
    assert(clusterSizes.length === k)
    assert(clusterSizes.sum === numRows)
    assert(clusterSizes.forall(_ >= 0))

    model.setSummary(None)
    assert(!model.hasSummary)
  }

  test("read/write") {
    def checkModelData(model: GaussianMixtureModel, model2: GaussianMixtureModel): Unit = {
      assert(model.weights === model2.weights)
      assert(model.gaussians.map(_.mean) === model2.gaussians.map(_.mean))
      assert(model.gaussians.map(_.cov) === model2.gaussians.map(_.cov))
    }
    val gm = new GaussianMixture()
    testEstimatorAndModelReadWrite(gm, dataset,
      GaussianMixtureSuite.allParamSettings, checkModelData)
  }
}

object GaussianMixtureSuite {
  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "probabilityCol" -> "myProbability",
    "k" -> 3,
    "maxIter" -> 2,
    "tol" -> 0.01
  )
}
