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

import org.dmg.pmml.DataDictionary
import org.dmg.pmml.DataField
import org.dmg.pmml.DataType
import org.dmg.pmml.FieldName
import org.dmg.pmml.FieldUsageType
import org.dmg.pmml.MiningField
import org.dmg.pmml.MiningFunctionType
import org.dmg.pmml.MiningSchema
import org.dmg.pmml.NumericPredictor
import org.dmg.pmml.OpType
import org.dmg.pmml.RegressionModel
import org.dmg.pmml.RegressionTable
import org.dmg.pmml.RegressionNormalizationMethodType
import org.apache.spark.mllib.classification.LogisticRegressionModel

/**
 * PMML Model Export for LogisticRegressionModel class
 */
private[mllib] class LogisticRegressionPMMLModelExport(
    model : LogisticRegressionModel, 
    description : String) 
  extends PMMLModelExport{

  /**
   * Export the input LogisticRegressionModel model to PMML format
   */
  populateLogisticRegressionPMML(model)
  
  private def populateLogisticRegressionPMML(model : LogisticRegressionModel): Unit = {

     pmml.getHeader().setDescription(description) 
     
     if(model.weights.size > 0){
       
       val fields = new Array[FieldName](model.weights.size)
       
       val dataDictionary = new DataDictionary()
       
       val miningSchema = new MiningSchema()
       
       val regressionTableYES = new RegressionTable(model.intercept)
       .withTargetCategory("1")
       
       val regressionTableNO = new RegressionTable(0.0)
       .withTargetCategory("0")
       
       val regressionModel = new RegressionModel(miningSchema,MiningFunctionType.CLASSIFICATION)
        .withModelName(description)
        .withNormalizationMethod(RegressionNormalizationMethodType.LOGIT)
        .withRegressionTables(regressionTableYES, regressionTableNO)
        
       for ( i <- 0 until model.weights.size) {
         fields(i) = FieldName.create("field_" + i)
         dataDictionary
            .withDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
         miningSchema
            .withMiningFields(new MiningField(fields(i))
            .withUsageType(FieldUsageType.ACTIVE))
         regressionTableYES
            .withNumericPredictors(new NumericPredictor(fields(i), model.weights(i)))   
       }
       
       // add target field
       val targetField = FieldName.create("target");
       dataDictionary
        .withDataFields(
            new DataField(targetField, OpType.CATEGORICAL, DataType.STRING)
        )
        miningSchema
         .withMiningFields(new MiningField(targetField)
         .withUsageType(FieldUsageType.TARGET))
       
       dataDictionary.withNumberOfFields((dataDictionary.getDataFields()).size())
       
       pmml.setDataDictionary(dataDictionary)
       pmml.withModels(regressionModel)
       
     }
 
  }
  
}
