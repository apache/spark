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

package org.apache.spark.ml.util

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * :: DeveloperApi ::
 * Utils for handling schemas.
 */
@DeveloperApi
object SchemaUtils {

  // TODO: Move the utility methods to SQL.

  /**
   * Check whether the given schema contains a column of the required data type.
   * @param colName  column name
   * @param dataType  required column data type
   */
  def checkColumnType(schema: StructType, colName: String, dataType: DataType): Unit = {
    val actualDataType = schema(colName).dataType
    require(actualDataType.equals(dataType),
      s"Column $colName must be of type $dataType but was actually $actualDataType.")
  }

  /**
   * Appends a new column to the input schema. This fails if the given output column already exists.
   * @param schema input schema
   * @param colName new column name. If this column name is an empty string "", this method returns
   *                the input schema unchanged. This allows users to disable output columns.
   * @param dataType new column data type
   * @return new schema with the input column appended
   */
  def appendColumn(
      schema: StructType,
      colName: String,
      dataType: DataType): StructType = {
    if (colName.isEmpty) return schema
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(colName), s"Column $colName already exists.")
    val outputFields = schema.fields :+ StructField(colName, dataType, nullable = false)
    StructType(outputFields)
  }
}
