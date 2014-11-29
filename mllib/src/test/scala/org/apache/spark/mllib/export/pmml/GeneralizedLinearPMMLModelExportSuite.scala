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

package org.apache.spark.mllib.export.pmml

import org.dmg.pmml.RegressionModel
import org.scalatest.FunSuite

import org.apache.spark.mllib.export.ModelExportFactory
import org.apache.spark.mllib.export.ModelExportType
import org.apache.spark.mllib.regression.LassoModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.RidgeRegressionModel
import org.apache.spark.mllib.util.LinearDataGenerator

class GeneralizedLinearPMMLModelExportSuite extends FunSuite{

   test("GeneralizedLinearPMMLModelExport generate PMML format") {

    //arrange models to test
    val linearInput = LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 1, 17)
    val linearRegressionModel = new LinearRegressionModel(linearInput(0).features, linearInput(0).label);
    val ridgeRegressionModel = new RidgeRegressionModel(linearInput(0).features, linearInput(0).label);
    val lassoModel = new LassoModel(linearInput(0).features, linearInput(0).label);
    
    //act by exporting the model to the PMML format
    val linearModelExport = ModelExportFactory.createModelExport(linearRegressionModel, ModelExportType.PMML)         
    //assert that the PMML format is as expected
    assert(linearModelExport.isInstanceOf[PMMLModelExport])
    var pmml = linearModelExport.asInstanceOf[PMMLModelExport].getPmml()
    assert(pmml.getHeader().getDescription() === "linear regression")
    //check that the number of fields match the weights size
    assert(pmml.getDataDictionary().getNumberOfFields() === linearRegressionModel.weights.size + 1)
    //this verify that there is a model attached to the pmml object and the model is a regression one
    //it also verifies that the pmml model has a regression table with the same number of predictors of the model weights
    assert(pmml.getModels().get(0).asInstanceOf[RegressionModel]
     .getRegressionTables().get(0).getNumericPredictors().size() === linearRegressionModel.weights.size)
  
    //act
    val ridgeModelExport = ModelExportFactory.createModelExport(ridgeRegressionModel, ModelExportType.PMML)         
    //assert that the PMML format is as expected
    assert(ridgeModelExport.isInstanceOf[PMMLModelExport])
    pmml = ridgeModelExport.asInstanceOf[PMMLModelExport].getPmml()
    assert(pmml.getHeader().getDescription() === "ridge regression")
    //check that the number of fields match the weights size
    assert(pmml.getDataDictionary().getNumberOfFields() === ridgeRegressionModel.weights.size + 1)
    //this verify that there is a model attached to the pmml object and the model is a regression one
    //it also verifies that the pmml model has a regression table with the same number of predictors of the model weights
    assert(pmml.getModels().get(0).asInstanceOf[RegressionModel]
     .getRegressionTables().get(0).getNumericPredictors().size() === ridgeRegressionModel.weights.size)
    
    //act
    val lassoModelExport = ModelExportFactory.createModelExport(lassoModel, ModelExportType.PMML)         
    //assert that the PMML format is as expected
    assert(lassoModelExport.isInstanceOf[PMMLModelExport])
    pmml = lassoModelExport.asInstanceOf[PMMLModelExport].getPmml()
    assert(pmml.getHeader().getDescription() === "lasso regression")
    //check that the number of fields match the weights size
    assert(pmml.getDataDictionary().getNumberOfFields() === lassoModel.weights.size + 1)
    //this verify that there is a model attached to the pmml object and the model is a regression one
    //it also verifies that the pmml model has a regression table with the same number of predictors of the model weights
    assert(pmml.getModels().get(0).asInstanceOf[RegressionModel]
     .getRegressionTables().get(0).getNumericPredictors().size() === lassoModel.weights.size)
   
    //manual checking
    //ModelExporter.toPMML(linearRegressionModel,"/tmp/linearregression.xml")
    //ModelExporter.toPMML(ridgeRegressionModel,"/tmp/ridgeregression.xml")
    //ModelExporter.toPMML(lassoModel,"/tmp/lassoregression.xml")
    
   }
  
}
