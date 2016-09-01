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
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.Dataset

object RWrapperUtils extends Logging {

  /**
   * DataFrame column check.
   * When loading data, default columns "features" and "label" will be added. And these two names
   * would conflict with RFormula default feature and label column names.
   * Here is to change the column name to avoid "column already exists" error.
   *
   * @param rFormula RFormula instance
   * @param data Input dataset
   * @return Unit
   */
  def checkDataColumns(rFormula: RFormula, data: Dataset[_]): Unit = {
    if (data.schema.fieldNames.contains(rFormula.getLabelCol)) {
      val newLabelName = convertToUniqueName(rFormula.getLabelCol, data.schema.fieldNames)
      logWarning(
        s"data containing ${rFormula.getLabelCol} column, changing its name to $newLabelName")
      rFormula.setLabelCol(newLabelName)
    }

    if (data.schema.fieldNames.contains(rFormula.getFeaturesCol)) {
      val newFeaturesName = convertToUniqueName(rFormula.getFeaturesCol, data.schema.fieldNames)
      logWarning(
        s"data containing ${rFormula.getFeaturesCol} column, changing its name to $newFeaturesName")
      rFormula.setFeaturesCol(newFeaturesName)
    }
  }

  /**
   * Convert conflicting name to be an unique name.
   * Appending a sequence number, like originalName_output1
   * and incrementing until it is not already there
   *
   * @param originalName Original name
   * @param fieldNames Array of field names in existing schema
   * @return String
   */
  def convertToUniqueName(originalName: String, fieldNames: Array[String]): String = {
    var counter = 1
    var newName = originalName + "_output"

    while (fieldNames.contains(newName)) {
      newName = originalName + "_output" + counter
      counter += 1
    }
    newName
  }
}
