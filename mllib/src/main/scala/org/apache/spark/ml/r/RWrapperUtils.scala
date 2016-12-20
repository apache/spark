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

package org.apache.spark.ml.r

import org.apache.spark.internal.Logging
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.feature.{RFormula, RFormulaModel}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset

private[r] object RWrapperUtils extends Logging {

  /**
   * DataFrame column check.
   * When loading libsvm data, default columns "features" and "label" will be added.
   * And "features" would conflict with RFormula default feature column names.
   * Here is to change the column name to avoid "column already exists" error.
   *
   * @param rFormula RFormula instance
   * @param data Input dataset
   */
  def checkDataColumns(rFormula: RFormula, data: Dataset[_]): Unit = {
    if (data.schema.fieldNames.contains(rFormula.getFeaturesCol)) {
      val newFeaturesName = s"${Identifiable.randomUID(rFormula.getFeaturesCol)}"
      logInfo(s"data containing ${rFormula.getFeaturesCol} column, " +
        s"using new name $newFeaturesName instead")
      rFormula.setFeaturesCol(newFeaturesName)
    }

    if (rFormula.getForceIndexLabel && data.schema.fieldNames.contains(rFormula.getLabelCol)) {
      val newLabelName = s"${Identifiable.randomUID(rFormula.getLabelCol)}"
      logInfo(s"data containing ${rFormula.getLabelCol} column and we force to index label, " +
        s"using new name $newLabelName instead")
      rFormula.setLabelCol(newLabelName)
    }
  }

  /**
   * Get the feature names and original labels from the schema
   * of DataFrame transformed by RFormulaModel.
   *
   * @param rFormulaModel The RFormulaModel instance.
   * @param data Input dataset.
   * @return The feature names and original labels.
   */
  def getFeaturesAndLabels(
      rFormulaModel: RFormulaModel,
      data: Dataset[_]): (Array[String], Array[String]) = {
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)
    val labelAttr = Attribute.fromStructField(schema(rFormulaModel.getLabelCol))
      .asInstanceOf[NominalAttribute]
    val labels = labelAttr.values.get
    (features, labels)
  }
}
