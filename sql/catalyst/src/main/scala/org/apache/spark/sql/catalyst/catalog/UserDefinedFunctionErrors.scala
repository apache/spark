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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.errors.QueryErrorsBase

/**
 * Errors during registering and executing
 * [[org.apache.spark.sql.expressions.UserDefinedFunction]]s.
 */
object UserDefinedFunctionErrors extends QueryErrorsBase {
  def unsupportedUserDefinedFunction(language: RoutineLanguage): Throwable = {
    unsupportedUserDefinedFunction(language.name)
  }

  def unsupportedUserDefinedFunction(language: String): Throwable = {
    SparkException.internalError(s"Unsupported user defined function type: $language")
  }

  def duplicateParameterNames(routineName: String, names: String): Throwable = {
    new AnalysisException(
      errorClass = "DUPLICATE_ROUTINE_PARAMETER_NAMES",
      messageParameters = Map("routineName" -> routineName, "names" -> names))
  }

  def duplicateReturnsColumns(routineName: String, columns: String): Throwable = {
    new AnalysisException(
      errorClass = "DUPLICATE_ROUTINE_RETURNS_COLUMNS",
      messageParameters = Map("routineName" -> routineName, "columns" -> columns))
  }

  def cannotSpecifyNotNullOnFunctionParameters(input: String): Throwable = {
    new AnalysisException(
      errorClass = "USER_DEFINED_FUNCTIONS.NOT_NULL_ON_FUNCTION_PARAMETERS",
      messageParameters = Map("input" -> input))
  }

  def bodyIsNotAQueryForSqlTableUdf(functionName: String): Throwable = {
    new AnalysisException(
      errorClass = "USER_DEFINED_FUNCTIONS.SQL_TABLE_UDF_BODY_MUST_BE_A_QUERY",
      messageParameters = Map("name" -> functionName))
  }

  def missingColumnNamesForSqlTableUdf(functionName: String): Throwable = {
    new AnalysisException(
      errorClass = "USER_DEFINED_FUNCTIONS.SQL_TABLE_UDF_MISSING_COLUMN_NAMES",
      messageParameters = Map("functionName" -> toSQLId(functionName)))
  }

  def invalidTempViewReference(routineName: Seq[String], tempViewName: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_TEMP_OBJ_REFERENCE",
      messageParameters = Map(
        "obj" -> "FUNCTION",
        "objName" -> toSQLId(routineName),
        "tempObj" -> "VIEW",
        "tempObjName" -> toSQLId(tempViewName)
      )
    )
  }

  def invalidTempFuncReference(routineName: Seq[String], tempFuncName: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_TEMP_OBJ_REFERENCE",
      messageParameters = Map(
        "obj" -> "FUNCTION",
        "objName" -> toSQLId(routineName),
        "tempObj" -> "FUNCTION",
        "tempObjName" -> toSQLId(tempFuncName)
      )
    )
  }

  def invalidTempVarReference(routineName: Seq[String], varName: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_TEMP_OBJ_REFERENCE",
      messageParameters = Map(
        "obj" -> "FUNCTION",
        "objName" -> toSQLId(routineName),
        "tempObj" -> "VARIABLE",
        "tempObjName" -> toSQLId(varName)))
  }
}
