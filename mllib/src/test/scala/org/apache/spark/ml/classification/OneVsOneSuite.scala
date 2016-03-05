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
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.{MetadataUtils, MLTestingUtils}
import org.apache.spark.mllib.classification.LogisticRegressionSuite._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.Metadata

class OneVsOneSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var dataset: DataFrame = _
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
    dataset = sqlContext.createDataFrame(rdd)
  }

  test("params") {
    ParamsSuite.checkParams(new OneVsOne)
    val lrModel = new LogisticRegressionModel("lr", Vectors.dense(0.0), 0.0)
    val model = new OneVsOneModel("ovo", Metadata.empty, Array(lrModel), Array((0, 1)))
    ParamsSuite.checkParams(model)
  }

  test("one-vs-one: default params") {
    val numClasses = 3
    val ovo = new OneVsOne()
      .setClassifier(new LogisticRegression)
    assert(ovo.getLabelCol === "label")
    assert(ovo.getPredictionCol === "prediction")
    val ovoModel = ovo.fit(dataset)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(ovoModel)

    assert(ovoModel.models.size === numClasses)
    val transformedDataset = ovoModel.transform(dataset)

    // check for label metadata in prediction col
    val predictionColSchema = transformedDataset.schema(ovoModel.getPredictionCol)
    assert(MetadataUtils.getNumClasses(predictionColSchema) === Some(3))

    val ovaResults = transformedDataset.select("prediction", "label").rdd.map {
      row => (row.getDouble(0), row.getDouble(1))
    }

    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(numClasses)
    lr.optimizer.setRegParam(0.1).setNumIterations(100)

    val model = lr.run(rdd)
    val results = model.predict(rdd.map(_.features)).zip(rdd.map(_.label))
    // determine the #confusion matrix in each class.
    // bound how much error we allow compared to multinomial logistic regression.
    val expectedMetrics = new MulticlassMetrics(results)
    val ovoMetrics = new MulticlassMetrics(ovaResults)
    assert(expectedMetrics.confusionMatrix ~== ovoMetrics.confusionMatrix absTol 400)
  }

  test("one-vs-one: pass label metadata correctly during train") {
    val numClasses = 3
    val ovo = new OneVsOne()
    ovo.setClassifier(new MockLogisticRegression)

    val labelMetadata = NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
    val labelWithMetadata = dataset("label").as("label", labelMetadata.toMetadata())
    val features = dataset("features").as("features")
    val datasetWithLabelMetadata = dataset.select(labelWithMetadata, features)
    ovo.fit(datasetWithLabelMetadata)
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

    val ovo = new OneVsOne()
    ovo.setClassifier(new LogisticRegression())
      .setLabelCol(labelIndexer.getOutputCol)
      .setFeaturesCol("f")
      .setPredictionCol("p")

    val ovoModel = ovo.fit(indexedDataset)
    val transformedDataset = ovoModel.transform(indexedDataset)
    val outputFields = transformedDataset.schema.fieldNames.toSet
    assert(outputFields.contains("p"))
  }

  test("SPARK-8049: OneVsOne shouldn't output temp columns") {
    val logReg = new LogisticRegression()
      .setMaxIter(1)
    val ovo = new OneVsOne()
      .setClassifier(logReg)
    val output = ovo.fit(dataset).transform(dataset)
    assert(output.schema.fieldNames.toSet === Set("label", "features", "prediction"))
  }

  test("OneVsOne.copy and OneVsOneModel.copy") {
    val lr = new LogisticRegression()
      .setMaxIter(1)

    val ovo = new OneVsOne()
    withClue("copy with classifier unset should work") {
      ovo.copy(ParamMap(lr.maxIter -> 10))
    }
    ovo.setClassifier(lr)
    val ovo1 = ovo.copy(ParamMap(lr.maxIter -> 10))
    assert(ovo.getClassifier.getOrDefault(lr.maxIter) === 1, "copy should have no side-effects")
    assert(ovo1.getClassifier.getOrDefault(lr.maxIter) === 10,
      "copy should handle extra classifier params")

    val ovoModel = ovo1.fit(dataset).copy(ParamMap(lr.thresholds -> Array(0.9, 0.1)))
    ovoModel.models.foreach { case m: LogisticRegressionModel =>
      assert(m.getThreshold === 0.1, "copy should handle extra model params")
    }
  }
}
