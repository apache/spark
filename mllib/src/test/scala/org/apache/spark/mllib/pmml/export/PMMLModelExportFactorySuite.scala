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

package org.apache.spark.mllib.pmml.export

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes, LogisticRegressionModel, SVMModel}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LassoModel, LinearRegressionModel, RidgeRegressionModel}
import org.apache.spark.mllib.util.LinearDataGenerator

class PMMLModelExportFactorySuite extends SparkFunSuite {

  test("PMMLModelExportFactory create KMeansPMMLModelExport when passing a KMeansModel") {
    val clusterCenters = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0))
    val kmeansModel = new KMeansModel(clusterCenters)

    val modelExport = PMMLModelExportFactory.createPMMLModelExport(kmeansModel)

    assert(modelExport.isInstanceOf[KMeansPMMLModelExport])
  }

  test("PMMLModelExportFactory create NaiveBayesPMMLModelExport when passing a NaiveBayesModel") {
    val label = Array(0.0, 1.0, 2.0)
    val pi = Array(0.5, 0.1, 0.4).map(math.log)
    val theta = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))

    val nbModel = new NaiveBayesModel(label, pi, theta, NaiveBayes.Bernoulli)
    val modelExport = PMMLModelExportFactory.createPMMLModelExport(nbModel)

    assert(modelExport.isInstanceOf[NaiveBayesPMMLModelExport])
  }

  test("PMMLModelExportFactory create GeneralizedLinearPMMLModelExport when passing a "
    + "LinearRegressionModel, RidgeRegressionModel or LassoModel") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)

    val linearRegressionModel =
      new LinearRegressionModel(linearInput(0).features, linearInput(0).label)
    val linearModelExport = PMMLModelExportFactory.createPMMLModelExport(linearRegressionModel)
    assert(linearModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])

    val ridgeRegressionModel =
      new RidgeRegressionModel(linearInput(0).features, linearInput(0).label)
    val ridgeModelExport = PMMLModelExportFactory.createPMMLModelExport(ridgeRegressionModel)
    assert(ridgeModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])

    val lassoModel = new LassoModel(linearInput(0).features, linearInput(0).label)
    val lassoModelExport = PMMLModelExportFactory.createPMMLModelExport(lassoModel)
    assert(lassoModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])
  }

  test("PMMLModelExportFactory create BinaryClassificationPMMLModelExport "
    + "when passing a LogisticRegressionModel or SVMModel") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)

    val logisticRegressionModel =
      new LogisticRegressionModel(linearInput(0).features, linearInput(0).label)
    val logisticRegressionModelExport =
      PMMLModelExportFactory.createPMMLModelExport(logisticRegressionModel)
    assert(logisticRegressionModelExport.isInstanceOf[BinaryClassificationPMMLModelExport])

    val svmModel = new SVMModel(linearInput(0).features, linearInput(0).label)
    val svmModelExport = PMMLModelExportFactory.createPMMLModelExport(svmModel)
    assert(svmModelExport.isInstanceOf[BinaryClassificationPMMLModelExport])
  }

  test("PMMLModelExportFactory throw IllegalArgumentException "
    + "when passing a Multinomial Logistic Regression") {
    /** 3 classes, 2 features */
    val multiclassLogisticRegressionModel = new LogisticRegressionModel(
      weights = Vectors.dense(0.1, 0.2, 0.3, 0.4), intercept = 1.0,
      numFeatures = 2, numClasses = 3)

    intercept[IllegalArgumentException] {
      PMMLModelExportFactory.createPMMLModelExport(multiclassLogisticRegressionModel)
    }
  }

  test("PMMLModelExportFactory throw IllegalArgumentException when passing an unsupported model") {
    val invalidModel = new Object

    intercept[IllegalArgumentException] {
      PMMLModelExportFactory.createPMMLModelExport(invalidModel)
    }
  }

  test("PMMLModelExportFactory throw IllegalArgumentException "
    + "when passing a Multinomial Naive Bayes") {
    val label = Array(0.0, 1.0, 2.0)
    val pi = Array(0.5, 0.1, 0.4).map(math.log)
    val theta = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))

    val nbModel = new NaiveBayesModel(label, pi, theta, NaiveBayes.Multinomial)

    intercept[IllegalArgumentException] {
      PMMLModelExportFactory.createPMMLModelExport(nbModel)
    }
  }
}
