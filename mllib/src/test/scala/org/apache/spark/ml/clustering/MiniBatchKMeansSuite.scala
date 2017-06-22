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
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset

class MiniBatchKMeansSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  final val k = 5
  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = KMeansSuite.generateKMeansData(spark, 50, 3, k)
  }

  test("default parameters") {
    val mbkm = new MiniBatchKMeans()

    assert(mbkm.getK === 2)
    assert(mbkm.getFeaturesCol === "features")
    assert(mbkm.getPredictionCol === "prediction")
    assert(mbkm.getMaxIter === 20)
    assert(mbkm.getInitMode === MLlibKMeans.K_MEANS_PARALLEL)
    assert(mbkm.getInitSteps === 2)
    assert(mbkm.getTol === 1e-4)
    assert(mbkm.getFraction === 1.0)
    val model = mbkm.setMaxIter(1).fit(dataset)

    MLTestingUtils.checkCopyAndUids(mbkm, model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
  }

  test("set parameters") {
    val mbkm = new MiniBatchKMeans()
      .setK(9)
      .setFeaturesCol("test_feature")
      .setPredictionCol("test_prediction")
      .setMaxIter(33)
      .setInitMode(MLlibKMeans.RANDOM)
      .setInitSteps(3)
      .setSeed(123)
      .setTol(1e-3)
      .setFraction(0.1)

    assert(mbkm.getK === 9)
    assert(mbkm.getFeaturesCol === "test_feature")
    assert(mbkm.getPredictionCol === "test_prediction")
    assert(mbkm.getMaxIter === 33)
    assert(mbkm.getInitMode === MLlibKMeans.RANDOM)
    assert(mbkm.getInitSteps === 3)
    assert(mbkm.getSeed === 123)
    assert(mbkm.getTol === 1e-3)
    assert(mbkm.getFraction === 0.1)
  }

  test("parameters validation") {
    intercept[IllegalArgumentException] {
      new MiniBatchKMeans().setK(1)
    }
    intercept[IllegalArgumentException] {
      new MiniBatchKMeans().setInitMode("no_such_a_mode")
    }
    intercept[IllegalArgumentException] {
      new MiniBatchKMeans().setInitSteps(0)
    }
    intercept[IllegalArgumentException] {
      new MiniBatchKMeans().setFraction(0)
    }
    intercept[IllegalArgumentException] {
      new MiniBatchKMeans().setFraction(1.01)
    }
    intercept[IllegalArgumentException] {
      new MiniBatchKMeans().setFraction(-0.01)
    }
  }

  test("fit, transform and summary") {
    val predictionColName = "minibatch_kmeans_prediction"
    val mbkm = new MiniBatchKMeans().setK(k).setPredictionCol(predictionColName).setSeed(1)
    val model = mbkm.fit(dataset)
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
    val summary = model.summary
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

  test("KMeansModel transform with non-default feature and prediction cols") {
    val featuresColName = "minibatch_kmeans_model_features"
    val predictionColName = "minibatch_kmeans_model_prediction"

    val model = new MiniBatchKMeans().setK(k).setSeed(1).fit(dataset)
    model.setFeaturesCol(featuresColName).setPredictionCol(predictionColName)

    val transformed = model.transform(dataset.withColumnRenamed("features", featuresColName))
    Seq(featuresColName, predictionColName).foreach { column =>
      assert(transformed.columns.contains(column))
    }
    assert(model.getFeaturesCol == featuresColName)
    assert(model.getPredictionCol == predictionColName)
  }

  test("read/write") {
    def checkModelData(model: MiniBatchKMeansModel, model2: MiniBatchKMeansModel): Unit = {
      assert(model.clusterCenters === model2.clusterCenters)
    }
    val mbkm = new MiniBatchKMeans()
    testEstimatorAndModelReadWrite(mbkm, dataset, MiniBatchKMeansSuite.allParamSettings,
      MiniBatchKMeansSuite.allParamSettings, checkModelData)
  }
}

object MiniBatchKMeansSuite {
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
    "fraction" -> 0.1
  )
}
