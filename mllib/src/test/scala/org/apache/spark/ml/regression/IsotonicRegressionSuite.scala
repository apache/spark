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

package org.apache.spark.ml.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

class IsotonicRegressionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {
  import testImplicits._

  private def generateIsotonicInput(labels: Seq[Double]): DataFrame = {
    labels.zipWithIndex.map { case (label, i) => (label, i.toDouble, 1.0) }
      .toDF("label", "features", "weight")
  }

  private def generatePredictionInput(features: Seq[Double]): DataFrame = {
    features.map(Tuple1.apply).toDF("features")
  }

  test("isotonic regression predictions") {
    val dataset = generateIsotonicInput(Seq(1, 2, 3, 1, 6, 17, 16, 17, 18))
    val ir = new IsotonicRegression().setIsotonic(true)

    val model = ir.fit(dataset)

    val predictions = model
      .transform(dataset)
      .select("prediction").rdd.map { case Row(pred) =>
        pred
      }.collect()

    assert(predictions === Array(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18))

    assert(model.boundaries === Vectors.dense(0, 1, 3, 4, 5, 6, 7, 8))
    assert(model.predictions === Vectors.dense(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
    assert(model.getIsotonic)
  }

  test("antitonic regression predictions") {
    val dataset = generateIsotonicInput(Seq(7, 5, 3, 5, 1))
    val ir = new IsotonicRegression().setIsotonic(false)

    val model = ir.fit(dataset)
    val features = generatePredictionInput(Seq(-2.0, -1.0, 0.5, 0.75, 1.0, 2.0, 9.0))

    val predictions = model
      .transform(features)
      .select("prediction").rdd.map {
        case Row(pred) => pred
      }.collect()

    assert(predictions === Array(7, 7, 6, 5.5, 5, 4, 1))
  }

  test("params validation") {
    val dataset = generateIsotonicInput(Seq(1, 2, 3))
    val ir = new IsotonicRegression
    ParamsSuite.checkParams(ir)
    val model = ir.fit(dataset)
    ParamsSuite.checkParams(model)
  }

  test("default params") {
    val dataset = generateIsotonicInput(Seq(1, 2, 3))
    val ir = new IsotonicRegression()
    assert(ir.getLabelCol === "label")
    assert(ir.getFeaturesCol === "features")
    assert(ir.getPredictionCol === "prediction")
    assert(!ir.isDefined(ir.weightCol))
    assert(ir.getIsotonic)
    assert(ir.getFeatureIndex === 0)

    val model = ir.fit(dataset)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)

    model.transform(dataset)
      .select("label", "features", "prediction", "weight")
      .collect()

    assert(model.getLabelCol === "label")
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(!model.isDefined(model.weightCol))
    assert(model.getIsotonic)
    assert(model.getFeatureIndex === 0)
    assert(model.hasParent)
  }

  test("set parameters") {
    val isotonicRegression = new IsotonicRegression()
      .setIsotonic(false)
      .setWeightCol("w")
      .setFeaturesCol("f")
      .setLabelCol("l")
      .setPredictionCol("p")

    assert(!isotonicRegression.getIsotonic)
    assert(isotonicRegression.getWeightCol === "w")
    assert(isotonicRegression.getFeaturesCol === "f")
    assert(isotonicRegression.getLabelCol === "l")
    assert(isotonicRegression.getPredictionCol === "p")
  }

  test("missing column") {
    val dataset = generateIsotonicInput(Seq(1, 2, 3))

    intercept[IllegalArgumentException] {
      new IsotonicRegression().setWeightCol("w").fit(dataset)
    }

    intercept[IllegalArgumentException] {
      new IsotonicRegression().setFeaturesCol("f").fit(dataset)
    }

    intercept[IllegalArgumentException] {
      new IsotonicRegression().setLabelCol("l").fit(dataset)
    }

    intercept[IllegalArgumentException] {
      new IsotonicRegression().fit(dataset).setFeaturesCol("f").transform(dataset)
    }
  }

  test("vector features column with feature index") {
    val dataset = Seq(
      (4.0, Vectors.dense(0.0, 1.0)),
      (3.0, Vectors.dense(0.0, 2.0)),
      (5.0, Vectors.sparse(2, Array(1), Array(3.0)))
    ).toDF("label", "features")

    val ir = new IsotonicRegression()
      .setFeatureIndex(1)

    val model = ir.fit(dataset)

    val features = generatePredictionInput(Seq(2.0, 3.0, 4.0, 5.0))

    val predictions = model
      .transform(features)
      .select("prediction").rdd.map {
      case Row(pred) => pred
    }.collect()

    assert(predictions === Array(3.5, 5.0, 5.0, 5.0))
  }

  test("read/write") {
    val dataset = generateIsotonicInput(Seq(1, 2, 3, 1, 6, 17, 16, 17, 18))

    def checkModelData(model: IsotonicRegressionModel, model2: IsotonicRegressionModel): Unit = {
      assert(model.boundaries === model2.boundaries)
      assert(model.predictions === model2.predictions)
      assert(model.isotonic === model2.isotonic)
    }

    val ir = new IsotonicRegression()
    testEstimatorAndModelReadWrite(ir, dataset, IsotonicRegressionSuite.allParamSettings,
      checkModelData)
  }

  test("should support all NumericType labels and not support other types") {
    val ir = new IsotonicRegression()
    MLTestingUtils.checkNumericTypes[IsotonicRegressionModel, IsotonicRegression](
      ir, spark, isClassification = false) { (expected, actual) =>
        assert(expected.boundaries === actual.boundaries)
        assert(expected.predictions === actual.predictions)
      }
  }
}

object IsotonicRegressionSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "isotonic" -> true,
    "featureIndex" -> 0
  )
}
