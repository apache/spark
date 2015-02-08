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
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.LinearDataGenerator

class LogisticRegressionPMMLModelExportSuite extends FunSuite{

   test("LogisticRegressionPMMLModelExport generate PMML format") {

    //arrange models to test
    val linearInput = LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 1, 17)
    val logisticRegressionModel = new LogisticRegressionModel(linearInput(0).features, linearInput(0).label);
    
    //act by exporting the model to the PMML format
    val logisticModelExport = PMMLModelExportFactory.createPMMLModelExport(logisticRegressionModel)         
    //assert that the PMML format is as expected
    assert(logisticModelExport.isInstanceOf[PMMLModelExport])
    var pmml = logisticModelExport.asInstanceOf[PMMLModelExport].getPmml()
    assert(pmml.getHeader().getDescription() === "logistic regression")
    //check that the number of fields match the weights size
    assert(pmml.getDataDictionary().getNumberOfFields() === logisticRegressionModel.weights.size + 1)
    //this verify that there is a model attached to the pmml object and the model is a regression one
    //it also verifies that the pmml model has a regression table (for target category 1) with the same number of predictors of the model weights
    assert(pmml.getModels().get(0).asInstanceOf[RegressionModel]
     .getRegressionTables().get(0).getTargetCategory() === "1")
    assert(pmml.getModels().get(0).asInstanceOf[RegressionModel]
     .getRegressionTables().get(0).getNumericPredictors().size() === logisticRegressionModel.weights.size)
    //verify if there is a second table with target category 0 and no predictors
    assert(pmml.getModels().get(0).asInstanceOf[RegressionModel]
     .getRegressionTables().get(1).getTargetCategory() === "0")
    assert(pmml.getModels().get(0).asInstanceOf[RegressionModel]
     .getRegressionTables().get(1).getNumericPredictors().size() === 0)
   
    //manual checking
    //logisticRegressionModel.toPMML("/tmp/logisticregression.xml")
    
   }
  
}
