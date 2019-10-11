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
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.stat.distribution.MultivariateGaussian
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class GaussianMixtureSuite extends MLTest with DefaultReadWriteTest {

  import GaussianMixtureSuite._
  import testImplicits._

  final val k = 5
  private val seed = 538009335
  @transient var dataset: DataFrame = _
  @transient var denseDataset: DataFrame = _
  @transient var sparseDataset: DataFrame = _
  @transient var decompositionDataset: DataFrame = _
  @transient var rDataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = KMeansSuite.generateKMeansData(spark, 50, 3, k)
    denseDataset = denseData.map(FeatureData).toDF()
    sparseDataset = denseData.map { point =>
      FeatureData(point.toSparse)
    }.toDF()
    decompositionDataset = decompositionData.map(FeatureData).toDF()
    rDataset = rData.map(FeatureData).toDF()
  }

  test("gmm fails on high dimensional data") {
    val df = Seq(
      Vectors.sparse(GaussianMixture.MAX_NUM_FEATURES + 1, Array(0, 4), Array(3.0, 8.0)),
      Vectors.sparse(GaussianMixture.MAX_NUM_FEATURES + 1, Array(1, 5), Array(4.0, 9.0)))
      .map(Tuple1.apply).toDF("features")
    val gm = new GaussianMixture()
    withClue(s"GMM should restrict the maximum number of features to be < " +
      s"${GaussianMixture.MAX_NUM_FEATURES}") {
      intercept[IllegalArgumentException] {
        gm.fit(df)
      }
    }
  }

  test("default parameters") {
    val gm = new GaussianMixture()

    assert(gm.getK === 2)
    assert(gm.getFeaturesCol === "features")
    assert(gm.getPredictionCol === "prediction")
    assert(gm.getMaxIter === 100)
    assert(gm.getTol === 0.01)
    val model = gm.setMaxIter(1).fit(dataset)

    MLTestingUtils.checkCopyAndUids(gm, model)
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

    // Check prediction matches the highest probability, and probabilities sum to one.
    testTransformer[Tuple1[Vector]](dataset.toDF(), model,
      "features", predictionColName, probabilityColName) {
      case Row(_, pred: Int, prob: Vector) =>
        val probArray = prob.toArray
        val predFromProb = probArray.zipWithIndex.maxBy(_._1)._2
        assert(pred === predFromProb)
        assert(probArray.sum ~== 1.0 absTol 1E-5)
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
    assert(summary.numIter == 2)

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
    testEstimatorAndModelReadWrite(gm, dataset, GaussianMixtureSuite.allParamSettings,
      GaussianMixtureSuite.allParamSettings, checkModelData)
  }

  test("univariate dense/sparse data with two clusters") {
    val weights = Array(2.0 / 3.0, 1.0 / 3.0)
    val means = Array(Vectors.dense(5.1604), Vectors.dense(-4.3673))
    val covs = Array(Matrices.dense(1, 1, Array(0.86644)), Matrices.dense(1, 1, Array(1.1098)))
    val gaussians = means.zip(covs).map { case (mean, cov) =>
      new MultivariateGaussian(mean, cov)
    }
    val expected = new GaussianMixtureModel("dummy", weights, gaussians)

    Seq(denseDataset, sparseDataset).foreach { dataset =>
      val actual = new GaussianMixture().setK(2).setSeed(seed).fit(dataset)
      modelEquals(expected, actual)
    }
  }

  test("check distributed decomposition") {
    val k = 5
    val d = decompositionData.head.size
    assert(GaussianMixture.shouldDistributeGaussians(k, d))

    val gmm = new GaussianMixture().setK(k).setSeed(seed).fit(decompositionDataset)
    assert(gmm.getK === k)
  }

  test("multivariate data and check againt R mvnormalmixEM") {
    /*
      Using the following R code to generate data and train the model using mixtools package.
      library(mvtnorm)
      library(mixtools)
      set.seed(1)
      a <- rmvnorm(7, c(0, 0))
      b <- rmvnorm(8, c(10, 10))
      data <- rbind(a, b)
      model <- mvnormalmixEM(data, k = 2)
      model$lambda

      [1] 0.4666667 0.5333333

      model$mu

      [1] 0.11731091 -0.06192351
      [1] 10.363673  9.897081

      model$sigma

      [[1]]
                 [,1]       [,2]
      [1,] 0.62049934 0.06880802
      [2,] 0.06880802 1.27431874

      [[2]]
                [,1]     [,2]
      [1,] 0.2961543 0.160783
      [2,] 0.1607830 1.008878

      model$loglik

      [1] -46.89499
     */
    val weights = Array(0.5333333, 0.4666667)
    val means = Array(Vectors.dense(10.363673, 9.897081), Vectors.dense(0.11731091, -0.06192351))
    val covs = Array(Matrices.dense(2, 2, Array(0.2961543, 0.1607830, 0.160783, 1.008878)),
      Matrices.dense(2, 2, Array(0.62049934, 0.06880802, 0.06880802, 1.27431874)))
    val gaussians = means.zip(covs).map { case (mean, cov) =>
      new MultivariateGaussian(mean, cov)
    }

    val expected = new GaussianMixtureModel("dummy", weights, gaussians)
    val actual = new GaussianMixture().setK(2).setSeed(seed).fit(rDataset)
    modelEquals(expected, actual)

    val llk = actual.summary.logLikelihood
    assert(llk ~== -46.89499 absTol 1E-5)
  }

  test("upper triangular matrix unpacking") {
    /*
       The full symmetric matrix is as follows:
       1.0 2.5 3.8 0.9
       2.5 2.0 7.2 3.8
       3.8 7.2 3.0 1.0
       0.9 3.8 1.0 4.0
     */
    val triangularValues = Array(1.0, 2.5, 2.0, 3.8, 7.2, 3.0, 0.9, 3.8, 1.0, 4.0)
    val symmetricValues = Array(1.0, 2.5, 3.8, 0.9, 2.5, 2.0, 7.2, 3.8,
      3.8, 7.2, 3.0, 1.0, 0.9, 3.8, 1.0, 4.0)
    val symmetricMatrix = new DenseMatrix(4, 4, symmetricValues)
    val expectedMatrix = GaussianMixture.unpackUpperTriangularMatrix(4, triangularValues)
    assert(symmetricMatrix === expectedMatrix)
  }

  test("GaussianMixture with Array input") {
    def trainAndComputlogLikelihood(dataset: Dataset[_]): Double = {
      val model = new GaussianMixture().setK(k).setMaxIter(1).setSeed(1).fit(dataset)
      model.summary.logLikelihood
    }

    val (newDataset, newDatasetD, newDatasetF) = MLTestingUtils.generateArrayFeatureDataset(dataset)
    val trueLikelihood = trainAndComputlogLikelihood(newDataset)
    val doubleLikelihood = trainAndComputlogLikelihood(newDatasetD)
    val floatLikelihood = trainAndComputlogLikelihood(newDatasetF)

    // checking the cost is fine enough as a sanity check
    assert(trueLikelihood ~== doubleLikelihood absTol 1e-6)
    assert(trueLikelihood ~== floatLikelihood absTol 1e-6)
  }

  test("prediction on single instance") {
    val gmm = new GaussianMixture().setSeed(123L)
    val model = gmm.fit(dataset)
    testClusteringModelSinglePrediction(model, model.predict, dataset,
      model.getFeaturesCol, model.getPredictionCol)

    testClusteringModelSingleProbabilisticPrediction(model, model.predictProbability, dataset,
      model.getFeaturesCol, model.getProbabilityCol)
  }
}

object GaussianMixtureSuite extends SparkFunSuite {
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

  val denseData = Seq(
    Vectors.dense(-5.1971), Vectors.dense(-2.5359), Vectors.dense(-3.8220),
    Vectors.dense(-5.2211), Vectors.dense(-5.0602), Vectors.dense( 4.7118),
    Vectors.dense( 6.8989), Vectors.dense( 3.4592), Vectors.dense( 4.6322),
    Vectors.dense( 5.7048), Vectors.dense( 4.6567), Vectors.dense( 5.5026),
    Vectors.dense( 4.5605), Vectors.dense( 5.2043), Vectors.dense( 6.2734)
  )

  val decompositionData: Seq[Vector] = Seq.tabulate(25) { i: Int =>
    Vectors.dense(Array.tabulate(50)(i + _.toDouble))
  }

  val rData = Seq(
    Vectors.dense(-0.6264538, 0.1836433), Vectors.dense(-0.8356286, 1.5952808),
    Vectors.dense(0.3295078, -0.8204684), Vectors.dense(0.4874291, 0.7383247),
    Vectors.dense(0.5757814, -0.3053884), Vectors.dense(1.5117812, 0.3898432),
    Vectors.dense(-0.6212406, -2.2146999), Vectors.dense(11.1249309, 9.9550664),
    Vectors.dense(9.9838097, 10.9438362), Vectors.dense(10.8212212, 10.5939013),
    Vectors.dense(10.9189774, 10.7821363), Vectors.dense(10.0745650, 8.0106483),
    Vectors.dense(10.6198257, 9.9438713), Vectors.dense(9.8442045, 8.5292476),
    Vectors.dense(9.5218499, 10.4179416)
  )

  case class FeatureData(features: Vector)

  def modelEquals(m1: GaussianMixtureModel, m2: GaussianMixtureModel): Unit = {
    assert(m1.weights.length === m2.weights.length)
    for (i <- m1.weights.indices) {
      assert(m1.weights(i) ~== m2.weights(i) absTol 1E-3)
      assert(m1.gaussians(i).mean ~== m2.gaussians(i).mean absTol 1E-3)
      assert(m1.gaussians(i).cov ~== m2.gaussians(i).cov absTol 1E-3)
    }
  }
}
