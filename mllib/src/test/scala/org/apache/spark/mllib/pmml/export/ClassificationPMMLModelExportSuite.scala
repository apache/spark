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

import org.dmg.pmml.RegressionModel
import org.dmg.pmml.RegressionNormalizationMethodType

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LinearDataGenerator

class ClassificationPMMLModelExportSuite extends SparkFunSuite {

  test("binary logistic regression PMML export") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)
    val logisticRegressionModel =
      new LogisticRegressionModel(linearInput(0).features, linearInput(0).label)

    val logisticModelExport = PMMLModelExportFactory.createPMMLModelExport(logisticRegressionModel)

    // assert that the PMML format is as expected
    assert(logisticModelExport.isInstanceOf[PMMLModelExport])
    val pmml = logisticModelExport.asInstanceOf[PMMLModelExport].getPmml
    assert(pmml.getHeader.getDescription === "logistic regression")
    // check that the number of fields match the weights size
    assert(pmml.getDataDictionary.getNumberOfFields === logisticRegressionModel.weights.size + 1)
    // This verify that there is a model attached to the pmml object and the model is a regression
    // one.  It also verifies that the pmml model has a regression table (for target category 1)
    // with the same number of predictors of the model weights.
    val pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(1).getTargetCategory === "1")
    assert(pmmlRegressionModel.getRegressionTables.get(1).getNumericPredictors.size
      === logisticRegressionModel.weights.size)
    // verify if there is a second table with target category 0 and no predictors
    assert(pmmlRegressionModel.getRegressionTables.get(0).getTargetCategory === "0")
    assert(pmmlRegressionModel.getRegressionTables.get(0).getNumericPredictors.size === 0)
    // ensure logistic regression has normalization method set to LOGIT
    assert(pmmlRegressionModel.getNormalizationMethod() == RegressionNormalizationMethodType.LOGIT)
  }

  test("linear SVM PMML export") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)
    val svmModel = new SVMModel(linearInput(0).features, linearInput(0).label)

    val svmModelExport = PMMLModelExportFactory.createPMMLModelExport(svmModel)

    // assert that the PMML format is as expected
    assert(svmModelExport.isInstanceOf[PMMLModelExport])
    val pmml = svmModelExport.getPmml
    assert(pmml.getHeader.getDescription
      === "linear SVM")
    // check that the number of fields match the weights size
    assert(pmml.getDataDictionary.getNumberOfFields === svmModel.weights.size + 1)
    // This verify that there is a model attached to the pmml object and the model is a regression
    // one.  It also verifies that the pmml model has a regression table (for target category 1)
    // with the same number of predictors of the model weights.
    val pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(1).getTargetCategory === "1")
    assert(pmmlRegressionModel.getRegressionTables.get(1).getNumericPredictors.size
      === svmModel.weights.size)
    // verify if there is a second table with target category 0 and no predictors
    assert(pmmlRegressionModel.getRegressionTables.get(0).getTargetCategory === "0")
    assert(pmmlRegressionModel.getRegressionTables.get(0).getNumericPredictors.size === 0)
    // ensure linear SVM has normalization method set to NONE
    assert(pmmlRegressionModel.getNormalizationMethod() == RegressionNormalizationMethodType.NONE)
  }
  
  test("multiclass logistic regression PMML export (wihtout intercept)") {
    /** 3 classes, 2 features */
    val logisticRegressionModel = new LogisticRegressionModel(
        weights = Vectors.dense(0.1, 0.2, 0.3, 0.4), intercept = 0.0, 
        numFeatures = 2, numClasses = 3)

    val logisticModelExport = PMMLModelExportFactory.createPMMLModelExport(logisticRegressionModel)

    // assert that the PMML format is as expected
    assert(logisticModelExport.isInstanceOf[PMMLModelExport])
    val pmml = logisticModelExport.asInstanceOf[PMMLModelExport].getPmml
    assert(pmml.getHeader.getDescription === "logistic regression")
    // check that the number of fields match the weights size
    assert(pmml.getDataDictionary.getNumberOfFields === logisticRegressionModel.numFeatures + 1)
    // This verify that there is a model attached to the pmml object and the model is a regression
    // one.  It also verifies that the pmml model has a regression table (for target category 1)
    // with the same number of predictors of the model weights / numFeatures.
    var pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(1).getTargetCategory === "1")
    assert(pmmlRegressionModel.getRegressionTables.get(1).getNumericPredictors.size
      === logisticRegressionModel.weights.size / logisticRegressionModel.numFeatures)
    // verify there is a category 2 as there are 3 classes
    pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(2).getTargetCategory === "2")
    assert(pmmlRegressionModel.getRegressionTables.get(2).getNumericPredictors.size
      === logisticRegressionModel.weights.size / logisticRegressionModel.numFeatures)
    // verify if there is a third table with target category 0 and no predictors
    assert(pmmlRegressionModel.getRegressionTables.get(0).getTargetCategory === "0")
    assert(pmmlRegressionModel.getRegressionTables.get(0).getNumericPredictors.size === 0)
    // ensure logistic regression has normalization method set to LOGIT
    assert(pmmlRegressionModel.getNormalizationMethod() == RegressionNormalizationMethodType.LOGIT)
    // ensure the category 1 and 2 tables have intercept 0
    assert(pmmlRegressionModel.getRegressionTables.get(1).getIntercept() === 0)
    assert(pmmlRegressionModel.getRegressionTables.get(2).getIntercept() === 0)
  }

  test("multiclass logistic regression PMML export (with intercept)") {
    /** 3 classes, 2 features */
    val logisticRegressionModel = new LogisticRegressionModel(
        weights = Vectors.dense(0.1, 0.2, 0.01, 0.3, 0.4, 0.02), intercept = 0.0,
        numFeatures = 2, numClasses = 3)

    val logisticModelExport = PMMLModelExportFactory.createPMMLModelExport(logisticRegressionModel)

    // assert that the PMML format is as expected
    assert(logisticModelExport.isInstanceOf[PMMLModelExport])
    val pmml = logisticModelExport.asInstanceOf[PMMLModelExport].getPmml
    assert(pmml.getHeader.getDescription === "logistic regression")
    // check that the number of fields match the weights size
    assert(pmml.getDataDictionary.getNumberOfFields === logisticRegressionModel.numFeatures + 1)
    // This verify that there is a model attached to the pmml object and the model is a regression
    // one.  It also verifies that the pmml model has a regression table (for target category 1)
    // with the same number of predictors of the model weights / numFeatures + 1.
    var pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(1).getTargetCategory === "1")
    assert(pmmlRegressionModel.getRegressionTables.get(1).getNumericPredictors.size
      === logisticRegressionModel.weights.size / (logisticRegressionModel.numFeatures + 1))
    // verify there is a category 2 as there are 3 classes
    pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(2).getTargetCategory === "2")
    assert(pmmlRegressionModel.getRegressionTables.get(2).getNumericPredictors.size
      === logisticRegressionModel.weights.size / (logisticRegressionModel.numFeatures + 1))
    // verify if there is a third table with target category 0 and no predictors
    assert(pmmlRegressionModel.getRegressionTables.get(0).getTargetCategory === "0")
    assert(pmmlRegressionModel.getRegressionTables.get(0).getNumericPredictors.size === 0)
    // ensure logistic regression has normalization method set to LOGIT
    assert(pmmlRegressionModel.getNormalizationMethod() == RegressionNormalizationMethodType.LOGIT)
    // ensure the category 1 and 2 tables have intercept 0
    assert(pmmlRegressionModel.getRegressionTables.get(1).getIntercept() === 0.01)
    assert(pmmlRegressionModel.getRegressionTables.get(2).getIntercept() === 0.02)
  }

}
