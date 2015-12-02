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

import scala.{Array => SArray}

import org.dmg.pmml._

import org.apache.spark.mllib.regression.GeneralizedLinearModel

/**
 * PMML Model Export for GeneralizedLinearModel class with ClassificationModel
 */
private[mllib] class ClassificationPMMLModelExport(
    model : GeneralizedLinearModel,
    numClasses: Int,
    numFeatures: Int,
    description : String,
    normalizationMethod : RegressionNormalizationMethodType,
    threshold: Double)
  extends PMMLModelExport {

  populateClassificationPMML()

  /**
   * Export the input LogisticRegressionModel or SVMModel to PMML format.
   */
  private def populateClassificationPMML(): Unit = {
     pmml.getHeader.setDescription(description)

     if (model.weights.size > 0) {

       val fields = new SArray[FieldName](numFeatures)
       val dataDictionary = new DataDictionary
       val miningSchema = new MiningSchema

       for (i <- 0 until numFeatures) {
         fields(i) = FieldName.create("field_" + i)
         dataDictionary
           .withDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
         miningSchema
           .withMiningFields(new MiningField(fields(i))
           .withUsageType(FieldUsageType.ACTIVE))
       }

       val regressionModel = new RegressionModel()
         .withFunctionName(MiningFunctionType.CLASSIFICATION)
         .withMiningSchema(miningSchema)
         .withModelName(description)
         .withNormalizationMethod(normalizationMethod)

       var interceptCategoryZero = threshold
       if (RegressionNormalizationMethodType.LOGIT == normalizationMethod) {
         if (threshold <= 0) {
           interceptCategoryZero = Double.MinValue
         } else if (threshold >= 1) {
           interceptCategoryZero = Double.MaxValue
         } else {
           interceptCategoryZero = -math.log(1 / threshold - 1)
         }
       }
       val regressionTableCategoryZero = new RegressionTable(interceptCategoryZero)
         .withTargetCategory("0")
       regressionModel.withRegressionTables(regressionTableCategoryZero)

       // build binary classification
       if (numClasses == 2) {
         // intercept is stored in model.intercept
         val regressionTableCategoryOne = new RegressionTable(model.intercept)
           .withTargetCategory("1")
         for (i <- 0 until numFeatures) {
           regressionTableCategoryOne
             .withNumericPredictors(new NumericPredictor(fields(i), model.weights(i)))
         }
         regressionModel.withRegressionTables(regressionTableCategoryOne)
       } else {
         // build multiclass classification
         for (i <- 0 until numClasses - 1) {
           if (model.weights.size == (numClasses - 1) * (numFeatures + 1)) {
             // intercept is stored in weights (last element)
             val regressionTableCategory = new RegressionTable(
               model.weights(i * (numFeatures + 1) + numFeatures))
               .withTargetCategory((i + 1).toString)
             for (j <- 0 until numFeatures) {
               regressionTableCategory.withNumericPredictors(new NumericPredictor(fields(j),
                   model.weights(i * (numFeatures + 1) + j)))
             }
             regressionModel.withRegressionTables(regressionTableCategory)
            } else {
             // intercept is zero
             val regressionTableCategory = new RegressionTable(0)
               .withTargetCategory((i + 1).toString)
             for (j <- 0 until numFeatures) {
               regressionTableCategory.withNumericPredictors(new NumericPredictor(fields(j),
                   model.weights(i*numFeatures + j)))
             }
             regressionModel.withRegressionTables(regressionTableCategory)
            }
         }
       }

       // add target field
       val targetField = FieldName.create("target")
       dataDictionary
         .withDataFields(new DataField(targetField, OpType.CATEGORICAL, DataType.STRING))
       miningSchema
         .withMiningFields(new MiningField(targetField)
         .withUsageType(FieldUsageType.TARGET))

       dataDictionary.withNumberOfFields(dataDictionary.getDataFields.size)

       pmml.setDataDictionary(dataDictionary)
       pmml.withModels(regressionModel)
     }
  }
}
