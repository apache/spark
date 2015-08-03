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
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class IsotonicRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {
  private val schema = StructType(
    Array(
      StructField("label", DoubleType),
      StructField("features", DoubleType),
      StructField("weight", DoubleType)))

  private val predictionSchema = StructType(Array(StructField("features", DoubleType)))

  private def generateIsotonicInput(labels: Seq[Double]): DataFrame = {
    val data = Seq.tabulate(labels.size)(i => Row(labels(i), i.toDouble, 1d))
    val parallelData = sc.parallelize(data)

    sqlContext.createDataFrame(parallelData, schema)
  }

  private def generatePredictionInput(features: Seq[Double]): DataFrame = {
    val data = Seq.tabulate(features.size)(i => Row(features(i)))

    val parallelData = sc.parallelize(data)
    sqlContext.createDataFrame(parallelData, predictionSchema)
  }

  test("isotonic regression predictions") {
    val dataset = generateIsotonicInput(Seq(1, 2, 3, 1, 6, 17, 16, 17, 18))
    val trainer = new IsotonicRegression().setIsotonicParam(true)

    val model = trainer.fit(dataset)

    val predictions = model
      .transform(dataset)
      .select("prediction").map {
        case Row(pred) => pred
      }.collect()

    assert(predictions === Array(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18))

    assert(model.parentModel.boundaries === Array(0, 1, 3, 4, 5, 6, 7, 8))
    assert(model.parentModel.predictions === Array(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
    assert(model.parentModel.isotonic)
  }

  test("antitonic regression predictions") {
    val dataset = generateIsotonicInput(Seq(7, 5, 3, 5, 1))
    val trainer = new IsotonicRegression().setIsotonicParam(false)

    val model = trainer.fit(dataset)
    val features = generatePredictionInput(Seq(-2.0, -1.0, 0.5, 0.75, 1.0, 2.0, 9.0))

    val predictions = model
      .transform(features)
      .select("prediction").map {
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
    assert(ir.getWeightCol === "weight")
    assert(ir.getPredictionCol === "prediction")
    assert(ir.getIsotonicParam === true)

    val model = ir.fit(dataset)
    model.transform(dataset)
      .select("label", "features", "prediction", "weight")
      .collect()

    assert(model.getLabelCol === "label")
    assert(model.getFeaturesCol === "features")
    assert(model.getWeightCol === "weight")
    assert(model.getPredictionCol === "prediction")
    assert(model.getIsotonicParam === true)
    assert(model.hasParent)
  }

  test("set parameters") {
    val isotonicRegression = new IsotonicRegression()
      .setIsotonicParam(false)
      .setWeightParam("w")
      .setFeaturesCol("f")
      .setLabelCol("l")
      .setPredictionCol("p")

    assert(isotonicRegression.getIsotonicParam === false)
    assert(isotonicRegression.getWeightCol === "w")
    assert(isotonicRegression.getFeaturesCol === "f")
    assert(isotonicRegression.getLabelCol === "l")
    assert(isotonicRegression.getPredictionCol === "p")
  }

  test("missing column") {
    val dataset = generateIsotonicInput(Seq(1, 2, 3))

    intercept[IllegalArgumentException] {
      new IsotonicRegression().setWeightParam("w").fit(dataset)
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
}
