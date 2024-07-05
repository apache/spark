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
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.FMRegressorSuite._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.ArrayImplicits._

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
    val model = new FMClassificationModel("fmc_test", 0.0, Vectors.dense(0.0),
      new DenseMatrix(1, 8, new Array[Double](8)))
    ParamsSuite.checkParams(model)
  }

  test("FMClassifier validate input dataset") {
    testInvalidClassificationLabels(new FMClassifier().fit(_), Some(2))
    testInvalidVectors(new FMClassifier().fit(_))
  }

  test("FMClassifier: Predictor, Classifier methods") {
    val session = smallBinaryDataset.sparkSession
    import session.implicits._
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
      Vector, FMClassificationModel](this, model, smallBinaryDataset)
  }

  def logLoss(modelRes: DataFrame): Double = {
    modelRes.select("label", "probability").rdd.map {
      case Row(label: Double, probability: DenseVector) =>
        if (label > 0) -math.log(probability(1))
        else -math.log(probability(0))
    }.mean()
  }

  test("check logisticLoss with AdamW") {
    // This testcase only tests whether the FM logloss part is valid and does not test the
    // pairwise interaction logic. The pairwise interaction logic be tested in FMRegressor.
    // When there is only one feature, FM will degenerate into LR. So two models will get
    // almost same loss value.

    val fm = new FMClassifier().setMaxIter(50)
    val fmModel = fm.fit(smallBinaryDataset)
    val fmRes = fmModel.transform(smallBinaryDataset)
    val fmLogLoss = logLoss(fmRes)

    /*
      Use following code to fit the dataset, the resulting logloss is 0.4756465459065247.
      val lr = new LogisticRegression()
      val lrModel = lr.fit(smallBinaryDataset)
      val lrRes = lrModel.transform(smallBinaryDataset)
      val lrLogLoss = logLoss(lrRes)
     */
    assert(fmLogLoss ~== 0.4756465459065247 absTol 1E-3)
  }

  test("check logisticLoss with GD") {
    val fm = new FMClassifier().setSolver("gd")
    val fmModel = fm.fit(smallBinaryDataset)
    val fmRes = fmModel.transform(smallBinaryDataset)
    val fmLogLoss = logLoss(fmRes)
    assert(fmLogLoss ~== 0.4756465459065247 absTol 1E-2)
  }

  test("sparse datasets") {
    // test sparse input will not throw exception
    val dataset = spark.createDataFrame(Array(
      (1.0, Vectors.dense(Array(1.0, 2.0, 3.0))),
      (0.0, Vectors.sparse(3, Array(0, 2), Array(-1.0, 2.0))),
      (0.0, Vectors.sparse(3, Array.emptyIntArray, Array.emptyDoubleArray)),
      (1.0, Vectors.sparse(3, Array(0, 1), Array(2.0, 3.0)))
    ).toImmutableArraySeq).toDF("label", "features")
    val fm = new FMClassifier().setMaxIter(10)
    fm.fit(dataset)
  }

  test("setThreshold, getThreshold") {
    val fm = new FMClassifier()

    // default
    withClue("FMClassifier should not have thresholds set by default.") {
      intercept[NoSuchElementException] {
        fm.getThresholds
      }
    }

    // Set via thresholds
    val fm2 = new FMClassifier()
    val threshold = Array(0.3, 0.7)
    fm2.setThresholds(threshold)
    assert(fm2.getThresholds === threshold)
  }

  test("thresholds prediction") {
    val fm = new FMClassifier()
    val df = smallBinaryDataset.toDF()
    val fmModel = fm.fit(df)

    // should predict all zeros
    fmModel.setThresholds(Array(0.0, 1.0))
    testTransformer[(Double, Vector)](df, fmModel, "prediction") {
      case Row(prediction: Double) => prediction === 0.0
    }

    // should predict all ones
    fmModel.setThresholds(Array(1.0, 0.0))
    testTransformer[(Double, Vector)](df, fmModel, "prediction") {
      case Row(prediction: Double) => prediction === 1.0
    }

    val fmBase = new FMClassifier()
    val model = fmBase.fit(df)
    val basePredictions = model.transform(df).select("prediction").collect()

    // constant threshold scaling is the same as no thresholds
    fmModel.setThresholds(Array(1.0, 1.0))
    testTransformerByGlobalCheckFunc[(Double, Vector)](df, fmModel, "prediction") {
      scaledPredictions: Seq[Row] =>
        assert(scaledPredictions.zip(basePredictions).forall { case (scaled, base) =>
          scaled.getDouble(0) === base.getDouble(0)
        })
    }

    // force it to use the predict method
    model.setRawPredictionCol("").setProbabilityCol("").setThresholds(Array(0, 1))
    testTransformer[(Double, Vector)](df, model, "prediction") {
      case Row(prediction: Double) => prediction === 0.0
    }
  }

  test("FMClassifier doesn't fit intercept when fitIntercept is off") {
    val fm = new FMClassifier().setFitIntercept(false)
    val model = fm.fit(smallBinaryDataset)
    assert(model.intercept === 0.0)
  }

  test("FMClassifier doesn't fit linear when fitLinear is off") {
    val fm = new FMClassifier().setFitLinear(false)
    val model = fm.fit(smallBinaryDataset)
    assert(model.linear === Vectors.sparse(model.numFeatures, Seq.empty))
  }

  test("prediction on single instance") {
    val fm = new FMClassifier()
    val fmModel = fm.fit(smallBinaryDataset)
    testPredictionModelSinglePrediction(fmModel, smallBinaryDataset)
  }

  test("summary and training summary") {
    val fm = new FMClassifier()
    val model = fm.setMaxIter(5).fit(smallBinaryDataset)

    val summary = model.evaluate(smallBinaryDataset)

    assert(model.summary.accuracy === summary.accuracy)
    assert(model.summary.weightedPrecision === summary.weightedPrecision)
    assert(model.summary.weightedRecall === summary.weightedRecall)
    assert(model.summary.pr.collect() === summary.pr.collect())
    assert(model.summary.roc.collect() === summary.roc.collect())
    assert(model.summary.areaUnderROC === summary.areaUnderROC)
  }

  test("FMClassifier training summary totalIterations") {
    Seq(1, 5, 10, 20, 100).foreach { maxIter =>
      val trainer = new FMClassifier().setMaxIter(maxIter)
      val model = trainer.fit(smallBinaryDataset)
      if (maxIter == 1) {
        assert(model.summary.totalIterations === maxIter)
      } else {
        assert(model.summary.totalIterations <= maxIter)
      }
    }
  }

  test("read/write") {
    def checkModelData(
      model: FMClassificationModel,
      model2: FMClassificationModel
    ): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.linear.toArray === model2.linear.toArray)
      assert(model.factors.toArray === model2.factors.toArray)
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
    "factorSize" -> 4,
    "fitIntercept" -> false,
    "fitLinear" -> false,
    "regParam" -> 0.01,
    "miniBatchFraction" -> 0.1,
    "initStd" -> 0.01,
    "maxIter" -> 2,
    "stepSize" -> 0.1,
    "tol" -> 1e-4,
    "solver" -> "gd",
    "seed" -> 10L,
    "thresholds" -> Array(0.4, 0.6)
  )
}
