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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.errors.DataTypeErrors
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{ArrayType, CharType, DataType, MapType, StringType, StructType, VarcharType}

trait SparkCharVarcharUtils {

  /**
   * Returns true if the given data type is CharType/VarcharType or has nested
   * CharType/VarcharType.
   */
  def hasCharVarchar(dt: DataType): Boolean = {
    dt.existsRecursively(f => f.isInstanceOf[CharType] || f.isInstanceOf[VarcharType])
  }

  /**
   * Validate the given [[DataType]] to fail if it is char or varchar types or contains nested
   * ones
   */
  def failIfHasCharVarchar(dt: DataType): DataType = {
    if (!SqlApiConf.get.charVarcharAsString && hasCharVarchar(dt)) {
      throw DataTypeErrors.charOrVarcharTypeAsStringUnsupportedError()
    } else {
      replaceCharVarcharWithString(dt)
    }
  }

  /**
   * Replaces CharType/VarcharType with StringType recursively in the given data type.
   */
  def replaceCharVarcharWithString(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharVarcharWithString(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharVarcharWithString(kt), replaceCharVarcharWithString(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceCharVarcharWithString(field.dataType))
      })
    case _: CharType => StringType
    case _: VarcharType => StringType
    case _ => dt
  }
}

object SparkCharVarcharUtils extends SparkCharVarcharUtils
