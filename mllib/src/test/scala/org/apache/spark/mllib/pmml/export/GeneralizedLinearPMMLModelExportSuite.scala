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
import org.scalatest.FunSuite

import org.apache.spark.mllib.regression.{LassoModel, LinearRegressionModel, RidgeRegressionModel}
import org.apache.spark.mllib.util.LinearDataGenerator

class GeneralizedLinearPMMLModelExportSuite extends FunSuite {

  test("linear regression PMML export") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)
    val linearRegressionModel =
      new LinearRegressionModel(linearInput(0).features, linearInput(0).label)
    val linearModelExport = PMMLModelExportFactory.createPMMLModelExport(linearRegressionModel)
    // assert that the PMML format is as expected
    assert(linearModelExport.isInstanceOf[PMMLModelExport])
    val pmml = linearModelExport.getPmml
    assert(pmml.getHeader.getDescription === "linear regression")
    // check that the number of fields match the weights size
    assert(pmml.getDataDictionary.getNumberOfFields === linearRegressionModel.weights.size + 1)
    // This verifies that there is a model attached to the pmml object and the model is a regression
    // one.  It also verifies that the pmml model has a regression table with the same number of
    // predictors of the model weights.
    val pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(0).getNumericPredictors.size
      === linearRegressionModel.weights.size)
  }

  test("ridge regression PMML export") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)
    val ridgeRegressionModel =
      new RidgeRegressionModel(linearInput(0).features, linearInput(0).label)
    val ridgeModelExport = PMMLModelExportFactory.createPMMLModelExport(ridgeRegressionModel)
    // assert that the PMML format is as expected
    assert(ridgeModelExport.isInstanceOf[PMMLModelExport])
    val pmml = ridgeModelExport.getPmml
    assert(pmml.getHeader.getDescription === "ridge regression")
    // check that the number of fields match the weights size
    assert(pmml.getDataDictionary.getNumberOfFields === ridgeRegressionModel.weights.size + 1)
    // This verify that there is a model attached to the pmml object and the model is a regression
    // one.  It also verifies that the pmml model has a regression table with the same number of
    // predictors of the model weights.
    val pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(0).getNumericPredictors.size
      === ridgeRegressionModel.weights.size)
  }

  test("lasso PMML export") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)
    val lassoModel = new LassoModel(linearInput(0).features, linearInput(0).label)
    val lassoModelExport = PMMLModelExportFactory.createPMMLModelExport(lassoModel)
    // assert that the PMML format is as expected
    assert(lassoModelExport.isInstanceOf[PMMLModelExport])
    val pmml = lassoModelExport.getPmml
    assert(pmml.getHeader.getDescription === "lasso regression")
    // check that the number of fields match the weights size
    assert(pmml.getDataDictionary.getNumberOfFields === lassoModel.weights.size + 1)
    // This verify that there is a model attached to the pmml object and the model is a regression
    // one. It also verifies that the pmml model has a regression table with the same number of
    // predictors of the model weights.
    val pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[RegressionModel]
    assert(pmmlRegressionModel.getRegressionTables.get(0).getNumericPredictors.size
      === lassoModel.weights.size)
  }

}
