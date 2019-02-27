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

import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.sql.{Dataset, Row}

class MultilayerPerceptronClassifierSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = Seq(
      (Vectors.dense(0.0, 0.0), 0.0),
      (Vectors.dense(0.0, 1.0), 1.0),
      (Vectors.dense(1.0, 0.0), 1.0),
      (Vectors.dense(1.0, 1.0), 0.0)
    ).toDF("features", "label")
  }

  test("Input Validation") {
    val mlpc = new MultilayerPerceptronClassifier()
    intercept[IllegalArgumentException] {
      mlpc.setLayers(Array.empty[Int])
    }
    intercept[IllegalArgumentException] {
      mlpc.setLayers(Array[Int](1))
    }
    intercept[IllegalArgumentException] {
      mlpc.setLayers(Array[Int](0, 1))
    }
    intercept[IllegalArgumentException] {
      mlpc.setLayers(Array[Int](1, 0))
    }
    mlpc.setLayers(Array[Int](1, 1))
  }

  test("XOR function learning as binary classification problem with two outputs.") {
    val layers = Array[Int](2, 5, 2)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setSeed(123L)
      .setMaxIter(100)
      .setSolver("l-bfgs")
    val model = trainer.fit(dataset)
    MLTestingUtils.checkCopyAndUids(trainer, model)
    testTransformer[(Vector, Double)](dataset.toDF(), model, "prediction", "label") {
      case Row(p: Double, l: Double) => assert(p == l)
    }
  }

  test("prediction on single instance") {
    val layers = Array[Int](2, 5, 2)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setSeed(123L)
      .setMaxIter(100)
      .setSolver("l-bfgs")
    val model = trainer.fit(dataset)
    testPredictionModelSinglePrediction(model, dataset)
  }

  test("Predicted class probabilities: calibration on toy dataset") {
    val layers = Array[Int](4, 5, 2)

    val strongDataset = Seq(
      (Vectors.dense(1, 2, 3, 4), 0d, Vectors.dense(1d, 0d)),
      (Vectors.dense(4, 3, 2, 1), 1d, Vectors.dense(0d, 1d)),
      (Vectors.dense(1, 1, 1, 1), 0d, Vectors.dense(.5, .5)),
      (Vectors.dense(1, 1, 1, 1), 1d, Vectors.dense(.5, .5))
    ).toDF("features", "label", "expectedProbability")
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setSeed(123L)
      .setMaxIter(100)
      .setSolver("l-bfgs")
    val model = trainer.fit(strongDataset)
    testTransformer[(Vector, Double, Vector)](strongDataset.toDF(), model,
      "probability", "expectedProbability") {
      case Row(p: Vector, e: Vector) => assert(p ~== e absTol 1e-3)
    }
    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, MultilayerPerceptronClassificationModel](this, model, strongDataset)
  }

  test("test model probability") {
    val layers = Array[Int](2, 5, 2)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setSeed(123L)
      .setMaxIter(100)
      .setSolver("l-bfgs")
    val model = trainer.fit(dataset)
    model.setProbabilityCol("probability")
    testTransformer[(Vector, Double)](dataset.toDF(), model, "features", "probability") {
      case Row(features: Vector, prob: Vector) =>
        val prob2 = model.mlpModel.predict(features)
        assert(prob ~== prob2 absTol 1e-3)
    }
  }

  test("Test setWeights by training restart") {
    val dataFrame = Seq(
      (Vectors.dense(0.0, 0.0), 0.0),
      (Vectors.dense(0.0, 1.0), 1.0),
      (Vectors.dense(1.0, 0.0), 1.0),
      (Vectors.dense(1.0, 1.0), 0.0)
    ).toDF("features", "label")
    val layers = Array[Int](2, 5, 2)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setSeed(12L)
      .setMaxIter(1)
      .setTol(1e-6)
    val initialWeights = trainer.fit(dataFrame).weights
    trainer.setInitialWeights(initialWeights.copy)
    val weights1 = trainer.fit(dataFrame).weights
    trainer.setInitialWeights(initialWeights.copy)
    val weights2 = trainer.fit(dataFrame).weights
    assert(weights1 ~== weights2 absTol 10e-5,
      "Training should produce the same weights given equal initial weights and number of steps")
  }

  test("3 class classification with 2 hidden layers") {
    val nPoints = 1000

    // The following coefficients are taken from OneVsRestSuite.scala
    // they represent 3-class iris dataset
    val coefficients = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)
    // the input seed is somewhat magic, to make this test pass
    val data = generateMultinomialLogisticInput(
      coefficients, xMean, xVariance, true, nPoints, 1).toDS()
    val dataFrame = data.toDF("label", "features")
    val numClasses = 3
    val numIterations = 100
    val layers = Array[Int](4, 5, 4, numClasses)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setSeed(11L) // currently this seed is ignored
      .setMaxIter(numIterations)
    val model = trainer.fit(dataFrame)
    val numFeatures = dataFrame.select("features").first().getAs[Vector](0).size
    assert(model.numFeatures === numFeatures)
    // train multinomial logistic regression
    val lr = new LogisticRegressionWithLBFGS()
      .setIntercept(true)
      .setNumClasses(numClasses)
    lr.optimizer.setRegParam(0.0)
      .setNumIterations(numIterations)
    val lrModel = lr.run(data.rdd.map(OldLabeledPoint.fromML))
    val lrPredictionAndLabels =
      lrModel.predict(data.rdd.map(p => OldVectors.fromML(p.features))).zip(data.rdd.map(_.label))
    // MLP's predictions should not differ a lot from LR's.
    val lrMetrics = new MulticlassMetrics(lrPredictionAndLabels)
    testTransformerByGlobalCheckFunc[(Double, Vector)](dataFrame, model, "prediction", "label") {
      rows: Seq[Row] =>
        val mlpPredictionAndLabels = rows.map(x => (x.getDouble(0), x.getDouble(1)))
        val mlpMetrics = new MulticlassMetrics(sc.makeRDD(mlpPredictionAndLabels))
        assert(mlpMetrics.confusionMatrix.asML ~== lrMetrics.confusionMatrix.asML absTol 100)
    }
  }

  test("read/write: MultilayerPerceptronClassifier") {
    val mlp = new MultilayerPerceptronClassifier()
      .setLayers(Array(2, 3, 2))
      .setMaxIter(5)
      .setBlockSize(2)
      .setSeed(42)
      .setTol(0.1)
      .setFeaturesCol("myFeatures")
      .setLabelCol("myLabel")
      .setPredictionCol("myPrediction")

    testDefaultReadWrite(mlp, testParams = true)
  }

  test("read/write: MultilayerPerceptronClassificationModel") {
    val mlp = new MultilayerPerceptronClassifier().setLayers(Array(2, 3, 2)).setMaxIter(5)
    val mlpModel = mlp.fit(dataset)
    val newMlpModel = testDefaultReadWrite(mlpModel, testParams = true)
    assert(newMlpModel.layers === mlpModel.layers)
    assert(newMlpModel.weights === mlpModel.weights)
  }

  test("should support all NumericType labels and not support other types") {
    val layers = Array(3, 2)
    val mpc = new MultilayerPerceptronClassifier().setLayers(layers).setMaxIter(1)
    MLTestingUtils.checkNumericTypes[
        MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier](
      mpc, spark) { (expected, actual) =>
        assert(expected.layers === actual.layers)
        assert(expected.weights === actual.weights)
      }
  }
}
