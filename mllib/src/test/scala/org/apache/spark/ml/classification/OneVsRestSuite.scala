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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MetadataUtils, MLTestingUtils}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.Metadata

class OneVsRestSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _
  @transient var rdd: RDD[LabeledPoint] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val nPoints = 1000

    // The following coefficients and xMean/xVariance are computed from iris dataset with lambda=0.2
    // As a result, we are drawing samples from probability distribution of an actual model.
    val coefficients = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)
    rdd = sc.parallelize(generateMultinomialLogisticInput(
      coefficients, xMean, xVariance, true, nPoints, 42), 2)
    dataset = spark.createDataFrame(rdd)
  }

  test("params") {
    ParamsSuite.checkParams(new OneVsRest)
    val lrModel = new LogisticRegressionModel("lr", Vectors.dense(0.0), 0.0)
    val model = new OneVsRestModel("ovr", Metadata.empty, Array(lrModel))
    ParamsSuite.checkParams(model)
  }

  test("one-vs-rest: default params") {
    val numClasses = 3
    val ova = new OneVsRest()
      .setClassifier(new LogisticRegression)
    assert(ova.getLabelCol === "label")
    assert(ova.getPredictionCol === "prediction")
    val ovaModel = ova.fit(dataset)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(ovaModel)

    assert(ovaModel.models.length === numClasses)
    val transformedDataset = ovaModel.transform(dataset)

    // check for label metadata in prediction col
    val predictionColSchema = transformedDataset.schema(ovaModel.getPredictionCol)
    assert(MetadataUtils.getNumClasses(predictionColSchema) === Some(3))

    val ovaResults = transformedDataset.select("prediction", "label").rdd.map {
      row => (row.getDouble(0), row.getDouble(1))
    }

    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(numClasses)
    lr.optimizer.setRegParam(0.1).setNumIterations(100)

    val model = lr.run(rdd.map(OldLabeledPoint.fromML))
    val results = model.predict(rdd.map(p => OldVectors.fromML(p.features))).zip(rdd.map(_.label))
    // determine the #confusion matrix in each class.
    // bound how much error we allow compared to multinomial logistic regression.
    val expectedMetrics = new MulticlassMetrics(results)
    val ovaMetrics = new MulticlassMetrics(ovaResults)
    assert(expectedMetrics.confusionMatrix ~== ovaMetrics.confusionMatrix absTol 400)
  }

  test("one-vs-rest: pass label metadata correctly during train") {
    val numClasses = 3
    val ova = new OneVsRest()
    ova.setClassifier(new MockLogisticRegression)

    val labelMetadata = NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
    val labelWithMetadata = dataset("label").as("label", labelMetadata.toMetadata())
    val features = dataset("features").as("features")
    val datasetWithLabelMetadata = dataset.select(labelWithMetadata, features)
    ova.fit(datasetWithLabelMetadata)
  }

  test("SPARK-8092: ensure label features and prediction cols are configurable") {
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexed")

    val indexedDataset = labelIndexer
      .fit(dataset)
      .transform(dataset)
      .drop("label")
      .withColumnRenamed("features", "f")

    val ova = new OneVsRest()
    ova.setClassifier(new LogisticRegression())
      .setLabelCol(labelIndexer.getOutputCol)
      .setFeaturesCol("f")
      .setPredictionCol("p")

    val ovaModel = ova.fit(indexedDataset)
    val transformedDataset = ovaModel.transform(indexedDataset)
    val outputFields = transformedDataset.schema.fieldNames.toSet
    assert(outputFields.contains("p"))
  }

  test("SPARK-8049: OneVsRest shouldn't output temp columns") {
    val logReg = new LogisticRegression()
      .setMaxIter(1)
    val ovr = new OneVsRest()
      .setClassifier(logReg)
    val output = ovr.fit(dataset).transform(dataset)
    assert(output.schema.fieldNames.toSet === Set("label", "features", "prediction"))
  }

  test("OneVsRest.copy and OneVsRestModel.copy") {
    val lr = new LogisticRegression()
      .setMaxIter(1)

    val ovr = new OneVsRest()
    withClue("copy with classifier unset should work") {
      ovr.copy(ParamMap(lr.maxIter -> 10))
    }
    ovr.setClassifier(lr)
    val ovr1 = ovr.copy(ParamMap(lr.maxIter -> 10))
    require(ovr.getClassifier.getOrDefault(lr.maxIter) === 1, "copy should have no side-effects")
    require(ovr1.getClassifier.getOrDefault(lr.maxIter) === 10,
      "copy should handle extra classifier params")

    val ovrModel = ovr1.fit(dataset).copy(ParamMap(lr.thresholds -> Array(0.9, 0.1)))
    ovrModel.models.foreach { case m: LogisticRegressionModel =>
      require(m.getThreshold === 0.1, "copy should handle extra model params")
    }
  }

  test("read/write: OneVsRest") {
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

    val ova = new OneVsRest()
      .setClassifier(lr)
      .setLabelCol("myLabel")
      .setFeaturesCol("myFeature")
      .setPredictionCol("myPrediction")

    val ova2 = testDefaultReadWrite(ova, testParams = false)
    assert(ova.uid === ova2.uid)
    assert(ova.getFeaturesCol === ova2.getFeaturesCol)
    assert(ova.getLabelCol === ova2.getLabelCol)
    assert(ova.getPredictionCol === ova2.getPredictionCol)

    ova2.getClassifier match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getMaxIter === lr2.getMaxIter)
        assert(lr.getRegParam === lr2.getRegParam)
      case other =>
        throw new AssertionError(s"Loaded OneVsRest expected classifier of type" +
          s" LogisticRegression but found ${other.getClass.getName}")
    }
  }

  test("read/write: OneVsRestModel") {
    def checkModelData(model: OneVsRestModel, model2: OneVsRestModel): Unit = {
      assert(model.uid === model2.uid)
      assert(model.getFeaturesCol === model2.getFeaturesCol)
      assert(model.getLabelCol === model2.getLabelCol)
      assert(model.getPredictionCol === model2.getPredictionCol)

      val classifier = model.getClassifier.asInstanceOf[LogisticRegression]

      model2.getClassifier match {
        case lr2: LogisticRegression =>
          assert(classifier.uid === lr2.uid)
          assert(classifier.getMaxIter === lr2.getMaxIter)
          assert(classifier.getRegParam === lr2.getRegParam)
        case other =>
          throw new AssertionError(s"Loaded OneVsRestModel expected classifier of type" +
            s" LogisticRegression but found ${other.getClass.getName}")
      }

      assert(model.labelMetadata === model2.labelMetadata)
      model.models.zip(model2.models).foreach {
        case (lrModel1: LogisticRegressionModel, lrModel2: LogisticRegressionModel) =>
          assert(lrModel1.uid === lrModel2.uid)
          assert(lrModel1.coefficients === lrModel2.coefficients)
          assert(lrModel1.intercept === lrModel2.intercept)
        case other =>
          throw new AssertionError(s"Loaded OneVsRestModel expected model of type" +
            s" LogisticRegressionModel but found ${other.getClass.getName}")
      }
    }

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
    val ova = new OneVsRest().setClassifier(lr)
    val ovaModel = ova.fit(dataset)
    val newOvaModel = testDefaultReadWrite(ovaModel, testParams = false)
    checkModelData(ovaModel, newOvaModel)
  }

  test("should support all NumericType labels and not support other types") {
    val ovr = new OneVsRest().setClassifier(new LogisticRegression().setMaxIter(1))
    MLTestingUtils.checkNumericTypes[OneVsRestModel, OneVsRest](
      ovr, spark) { (expected, actual) =>
        val expectedModels = expected.models.map(m => m.asInstanceOf[LogisticRegressionModel])
        val actualModels = actual.models.map(m => m.asInstanceOf[LogisticRegressionModel])
        assert(expectedModels.length === actualModels.length)
        expectedModels.zip(actualModels).foreach { case (e, a) =>
          assert(e.intercept === a.intercept)
          assert(e.coefficients.toArray === a.coefficients.toArray)
        }
      }
  }
}

private class MockLogisticRegression(uid: String) extends LogisticRegression(uid) {

  def this() = this("mockLogReg")

  setMaxIter(1)

  override protected[spark] def train(dataset: Dataset[_]): LogisticRegressionModel = {
    val labelSchema = dataset.schema($(labelCol))
    // check for label attribute propagation.
    assert(MetadataUtils.getNumClasses(labelSchema).forall(_ == 2))
    super.train(dataset)
  }
}
