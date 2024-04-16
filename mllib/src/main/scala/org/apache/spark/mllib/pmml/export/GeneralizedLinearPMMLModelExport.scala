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
 * PMML Model Export for GeneralizedLinearModel abstract class
 */
private[mllib] class GeneralizedLinearPMMLModelExport(
    model: GeneralizedLinearModel,
    description: String)
  extends PMMLModelExport {

  populateGeneralizedLinearPMML(model)

  /**
   * Export the input GeneralizedLinearModel model to PMML format.
   */
  private def populateGeneralizedLinearPMML(model: GeneralizedLinearModel): Unit = {
    pmml.getHeader.setDescription(description)

    if (model.weights.size > 0) {
      val fields = new SArray[FieldName](model.weights.size)
      val dataDictionary = new DataDictionary
      val miningSchema = new MiningSchema
      val regressionTable = new RegressionTable(model.intercept)
      val regressionModel = new RegressionModel()
        .setMiningFunction(MiningFunction.REGRESSION)
        .setMiningSchema(miningSchema)
        .setModelName(description)
        .addRegressionTables(regressionTable)

      for (i <- 0 until model.weights.size) {
        fields(i) = FieldName.create("field_" + i)
        dataDictionary.addDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
        miningSchema
          .addMiningFields(new MiningField(fields(i))
          .setUsageType(MiningField.UsageType.ACTIVE))
        regressionTable.addNumericPredictors(new NumericPredictor(fields(i), model.weights(i)))
      }

      // for completeness add target field
      val targetField = FieldName.create("target")
      dataDictionary.addDataFields(new DataField(targetField, OpType.CONTINUOUS, DataType.DOUBLE))
      miningSchema
        .addMiningFields(new MiningField(targetField)
        .setUsageType(MiningField.UsageType.TARGET))

      dataDictionary.setNumberOfFields(dataDictionary.getDataFields.size)

      pmml.setDataDictionary(dataDictionary)
      pmml.addModels(regressionModel)
    }
  }
}
