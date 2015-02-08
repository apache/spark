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
import org.apache.spark.mllib.classification.SVMModel
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
    val svmModel = new SVMModel(linearInput(0).features, linearInput(0).label);
    
    //act by exporting the model to the PMML format
    val linearModelExport = PMMLModelExportFactory.createPMMLModelExport(linearRegressionModel)         
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
    val ridgeModelExport = PMMLModelExportFactory.createPMMLModelExport(ridgeRegressionModel)         
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
    val lassoModelExport = PMMLModelExportFactory.createPMMLModelExport(lassoModel)         
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
     
    //act
    val svmModelExport = PMMLModelExportFactory.createPMMLModelExport(svmModel)         
    //assert that the PMML format is as expected
    assert(svmModelExport.isInstanceOf[PMMLModelExport])
    pmml = svmModelExport.asInstanceOf[PMMLModelExport].getPmml()
    assert(pmml.getHeader().getDescription() === "linear SVM: if predicted value > 0, the outcome is positive, or negative otherwise")
    //check that the number of fields match the weights size
    assert(pmml.getDataDictionary().getNumberOfFields() === svmModel.weights.size + 1)
    //this verify that there is a model attached to the pmml object and the model is a regression one
    //it also verifies that the pmml model has a regression table with the same number of predictors of the model weights
    assert(pmml.getModels().get(0).asInstanceOf[RegressionModel]
     .getRegressionTables().get(0).getNumericPredictors().size() === svmModel.weights.size)
   
    //manual checking
    //linearRegressionModel.toPMML("/tmp/linearregression.xml")
    //ridgeRegressionModel.toPMML("/tmp/ridgeregression.xml")
    //lassoModel.toPMML("/tmp/lassoregression.xml")
    //svmModel.toPMML("/tmp/linearsvm.xml")
    
   }
  
}
