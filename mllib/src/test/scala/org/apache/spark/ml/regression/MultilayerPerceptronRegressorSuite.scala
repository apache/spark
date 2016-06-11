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
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vectors}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class MultilayerPerceptronRegressorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("MLPRegressor behaves reasonably on toy data") {
    val df = spark.createDataFrame(Seq(
      LabeledPoint(30, Vectors.dense(1, 2, 3, 4)),
      LabeledPoint(-15, Vectors.dense(6, 3, 2, 1)),
      LabeledPoint(33, Vectors.dense(2, 2, 3, 4)),
      LabeledPoint(-18, Vectors.dense(6, 4, 2, 1)),
      LabeledPoint(27, Vectors.dense(1, 2, 6, 4)),
      LabeledPoint(-12, Vectors.dense(6, 3, 2, 2))
    ))
    val mlpr = new MultilayerPerceptronRegressor().setLayers(Array[Int](4, 10, 10, 1))
    val model = mlpr.fit(df)
    val results = model.transform(df)
    val predictions = results.select("prediction").rdd.map(_.getDouble(0))
    assert(predictions.max() > 2)
    assert(predictions.min() < -1)
  }

  test("MLPRegressor works with gradient descent") {
    val df = spark.createDataFrame(Seq(
      LabeledPoint(30, Vectors.dense(1, 2, 3, 4)),
      LabeledPoint(-15, Vectors.dense(6, 3, 2, 1)),
      LabeledPoint(33, Vectors.dense(2, 2, 3, 4)),
      LabeledPoint(-18, Vectors.dense(6, 4, 2, 1)),
      LabeledPoint(27, Vectors.dense(1, 2, 6, 4)),
      LabeledPoint(-12, Vectors.dense(6, 3, 2, 2))
    ))
    val layers = Array[Int](4, 5, 8, 1)
    val mlpr = new MultilayerPerceptronRegressor().setLayers(layers).setSolver("gd")
    val model = mlpr.fit(df)
    val results = model.transform(df)
    val predictions = results.select("prediction").rdd.map(_.getDouble(0))
    assert(predictions.max() > 2)
    assert(predictions.min() < -1)
  }

  test("Input Validation") {
    val mlpr = new MultilayerPerceptronRegressor()
    intercept[IllegalArgumentException] {
      mlpr.setLayers(Array[Int]())
    }
    intercept[IllegalArgumentException] {
      mlpr.setLayers(Array[Int](1))
    }
    intercept[IllegalArgumentException] {
      mlpr.setLayers(Array[Int](0, 1))
    }
    intercept[IllegalArgumentException] {
      mlpr.setLayers(Array[Int](1, 0))
    }
    mlpr.setLayers(Array[Int](1, 1))
  }

  test("Test setWeights by training restart") {
    val dataFrame = spark.createDataFrame(Seq(
      LabeledPoint(30, Vectors.dense(1, 2, 3, 4)),
      LabeledPoint(-15, Vectors.dense(6, 3, 2, 1)),
      LabeledPoint(33, Vectors.dense(2, 2, 3, 4)),
      LabeledPoint(-18, Vectors.dense(6, 4, 2, 1)),
      LabeledPoint(27, Vectors.dense(1, 2, 6, 4)),
      LabeledPoint(-12, Vectors.dense(6, 3, 2, 2))
    ))
    val layers = Array[Int](4, 5, 1)
    val trainer = new MultilayerPerceptronRegressor()
      .setLayers(layers)
      .setBlockSize(1)
      .setSeed(12L)
      .setMaxIter(1)
      .setTol(1e-6)
    // Compute weights to initialize network with.
    val initialWeights = trainer.fit(dataFrame).weights
    // Set trainer weights to the initialization for this test.
    trainer.setInitialWeights(initialWeights.copy)
    // Compute new weights with our initialization.
    val weights1 = trainer.fit(dataFrame).weights
    // Reset weights back to our initialization.
    trainer.setInitialWeights(initialWeights.copy)
    // Compute another set of weights with our initialization.
    val weights2 = trainer.fit(dataFrame).weights
    assert(weights1 ~== weights2 absTol 10e-5,
      "Training should produce the same weights given equal initial weights and number of steps")
  }

  test("read/write: MultilayerPerceptronRegressor") {
    val mlpr = new MultilayerPerceptronRegressor()
      .setLayers(Array(4, 3, 1))
      .setMaxIter(5)
      .setBlockSize(2)
      .setSeed(42)
      .setTol(0.1)
      .setFeaturesCol("myFeatures")
      .setLabelCol("myLabel")
      .setPredictionCol("myPrediction")
    testDefaultReadWrite(mlpr, testParams = true)
  }

  test("read/write: MultilayerPerceptronRegressorModel") {
    val df = spark.createDataFrame(Seq(
      LabeledPoint(10, Vectors.dense(1, 2, 3, 4)),
      LabeledPoint(-5, Vectors.dense(6, 3, 2, 1)),
      LabeledPoint(11, Vectors.dense(2, 2, 3, 4)),
      LabeledPoint(-6, Vectors.dense(6, 4, 2, 1)),
      LabeledPoint(9, Vectors.dense(1, 2, 6, 4)),
      LabeledPoint(-4, Vectors.dense(6, 3, 2, 2))
    ))
    val mlp = new MultilayerPerceptronRegressor().setLayers(Array(4, 3, 1)).setMaxIter(5)
    val mlpModel = mlp.fit(df)
    val newMlpModel = testDefaultReadWrite(mlpModel, testParams = true)
    assert(newMlpModel.layers === mlpModel.layers)
    assert(newMlpModel.weights === mlpModel.weights)
  }

  test("should support all NumericType labels and not support other types") {
    val layers = Array(1, 1)
    val mpc = new MultilayerPerceptronRegressor().setLayers(layers).setMaxIter(1)
    MLTestingUtils.checkNumericTypes[
      MultilayerPerceptronRegressorModel, MultilayerPerceptronRegressor](
      mpc, spark) { (expected, actual) =>
      assert(expected.layers === actual.layers)
      assert(expected.weights === actual.weights)
    }
  }
}
