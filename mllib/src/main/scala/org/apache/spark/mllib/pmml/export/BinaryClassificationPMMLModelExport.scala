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

package org.apache.spark.mllib.pmml.`export`

import scala.{Array => SArray}

import org.dmg.pmml.{DataDictionary, DataField, DataType, FieldName, MiningField,
  MiningFunction, MiningSchema, OpType}
import org.dmg.pmml.regression.{NumericPredictor, RegressionModel, RegressionTable}

import org.apache.spark.mllib.regression.GeneralizedLinearModel

/**
 * PMML Model Export for GeneralizedLinearModel class with binary ClassificationModel
 */
private[mllib] class BinaryClassificationPMMLModelExport(
    model: GeneralizedLinearModel,
    description: String,
    normalizationMethod: RegressionModel.NormalizationMethod,
    threshold: Double)
  extends PMMLModelExport {

  populateBinaryClassificationPMML()

  /**
   * Export the input LogisticRegressionModel or SVMModel to PMML format.
   */
  private def populateBinaryClassificationPMML(): Unit = {
     pmml.getHeader.setDescription(description)

     if (model.weights.size > 0) {
       val fields = new SArray[FieldName](model.weights.size)
       val dataDictionary = new DataDictionary
       val miningSchema = new MiningSchema
       val regressionTableYES = new RegressionTable(model.intercept).setTargetCategory("1")
       var interceptNO = threshold
       if (RegressionModel.NormalizationMethod.LOGIT == normalizationMethod) {
         if (threshold <= 0) {
           interceptNO = Double.MinValue
         } else if (threshold >= 1) {
           interceptNO = Double.MaxValue
         } else {
           interceptNO = -math.log(1 / threshold - 1)
         }
       }
       val regressionTableNO = new RegressionTable(interceptNO).setTargetCategory("0")
       val regressionModel = new RegressionModel()
         .setMiningFunction(MiningFunction.CLASSIFICATION)
         .setMiningSchema(miningSchema)
         .setModelName(description)
         .setNormalizationMethod(normalizationMethod)
         .addRegressionTables(regressionTableYES, regressionTableNO)

       for (i <- 0 until model.weights.size) {
         fields(i) = FieldName.create("field_" + i)
         dataDictionary.addDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
         miningSchema
           .addMiningFields(new MiningField(fields(i))
           .setUsageType(MiningField.UsageType.ACTIVE))
         regressionTableYES.addNumericPredictors(new NumericPredictor(fields(i), model.weights(i)))
       }

       // add target field
       val targetField = FieldName.create("target")
       dataDictionary
         .addDataFields(new DataField(targetField, OpType.CATEGORICAL, DataType.STRING))
       miningSchema
         .addMiningFields(new MiningField(targetField)
         .setUsageType(MiningField.UsageType.TARGET))

       dataDictionary.setNumberOfFields(dataDictionary.getDataFields.size)

       pmml.setDataDictionary(dataDictionary)
       pmml.addModels(regressionModel)
     }
  }
}
