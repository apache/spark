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

package org.apache.spark.ml.classification

import org.apache.spark.ml.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.FMRegressorSuite._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}

class FMClassifierSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val seed = 42
  @transient var smallBinaryDataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    smallBinaryDataset = generateLogisticInput(1.0, 1.0, nPoints = 100, seed = seed).toDF()
  }

  test("params") {
    ParamsSuite.checkParams(new FMClassifier)
    val model = new FMClassifierModel("fmc_test", Vectors.dense(0.0), 0, 2)
    ParamsSuite.checkParams(model)
  }

  test("FMClassifier: Predictor, Classifier methods") {
    val sqlContext = smallBinaryDataset.sqlContext
    import sqlContext.implicits._
    val fm = new FMClassifier()

    val model = fm.fit(smallBinaryDataset)
    assert(model.numClasses === 2)
    val numFeatures = smallBinaryDataset.select("features").first().getAs[Vector](0).size
    assert(model.numFeatures === numFeatures)

    testTransformer[(Double, Vector)](smallBinaryDataset.toDF(),
      model, "rawPrediction", "probability", "prediction") {
      case Row(raw: Vector, prob: Vector, pred: Double) =>
        // Compare rawPrediction with probability
        assert(raw.size === 2)
        assert(prob.size === 2)
        val probFromRaw1 = 1.0 / (1.0 + math.exp(-raw(1)))
        assert(prob(1) ~== probFromRaw1 relTol 1E-6)
        assert(prob(0) ~== 1.0 - probFromRaw1 relTol 1E-6)
        // Compare prediction with probability
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, FMClassifierModel](this, model, smallBinaryDataset)
  }

  test("factorization machines logisticLoss") {
    // This testcase only tests whether the FM logloss part is valid and does not test the
    // pairwise interaction logic. The pairwise interaction logic be tested in FMRegressor.
    // When there is only one feature, FM will degenerate into LR. So two models will get
    // almost same loss value.
    def logLoss(modelRes: DataFrame): Double = {
      modelRes.select("label", "probability").rdd.map {
        case Row(label: Double, probability: DenseVector) =>
          if (label > 0) -math.log(probability(1))
          else -math.log(probability(0))
      }.mean()
    }

    val fm = new FMClassifier().setMaxIter(200)
    val fmModel = fm.fit(smallBinaryDataset)
    val fmRes = fmModel.transform(smallBinaryDataset)
    val fmLogLoss = logLoss(fmRes)

    val lr = new LogisticRegression()
    val lrModel = lr.fit(smallBinaryDataset)
    val lrRes = lrModel.transform(smallBinaryDataset)
    val lrLogLoss = logLoss(lrRes)

    assert(fmLogLoss ~== lrLogLoss absTol 1E-4)
  }

  test("read/write") {
    def checkModelData(
      model: FMClassifierModel,
      model2: FMClassifierModel
    ): Unit = {
      assert(model.coefficients.toArray === model2.coefficients.toArray)
      assert(model.numFeatures === model2.numFeatures)
    }
    val fm = new FMClassifier()
    val data = smallBinaryDataset
      .withColumnRenamed("features", allParamSettings("featuresCol").toString)
      .withColumnRenamed("label", allParamSettings("labelCol").toString)
    testEstimatorAndModelReadWrite(fm, data, allParamSettings,
      allParamSettings, checkModelData)
  }
}

object FMClassifierSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "featuresCol" -> "myFeatures",
    "labelCol" -> "myLabel",
    "predictionCol" -> "prediction",
    "rawPredictionCol" -> "rawPrediction",
    "probabilityCol" -> "probability",
    "numFactors" -> 4,
    "fitBias" -> false,
    "fitLinear" -> false,
    "regParam" -> 0.01,
    "miniBatchFraction" -> 0.1,
    "initStd" -> 0.01,
    "maxIter" -> 2,
    "stepSize" -> 0.1,
    "tol" -> 1e-4,
    "solver" -> "gd",
    "thresholds" -> Array(0.4, 0.6)
  )
}
