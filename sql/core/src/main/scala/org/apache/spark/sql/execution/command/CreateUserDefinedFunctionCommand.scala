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

package org.apache.spark.sql.execution.command

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.{LanguageSQL, RoutineLanguage, UserDefinedFunctionErrors}
import org.apache.spark.sql.catalyst.catalog.UserDefinedFunction._
import org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * The base class for CreateUserDefinedFunctionCommand
 */
abstract class CreateUserDefinedFunctionCommand
  extends LeafRunnableCommand with IgnoreCachedData


object CreateUserDefinedFunctionCommand {

  /**
   * This factory methods serves as a central place to verify required inputs and
   * returns the CREATE command for the parsed user defined function.
   */
  // scalastyle:off argcount
  def apply(
      name: FunctionIdentifier,
      inputParamText: Option[String],
      returnTypeText: String,
      exprText: Option[String],
      queryText: Option[String],
      comment: Option[String],
      isDeterministic: Option[Boolean],
      containsSQL: Option[Boolean],
      language: RoutineLanguage,
      isTableFunc: Boolean,
      isTemp: Boolean,
      ignoreIfExists: Boolean,
      replace: Boolean
  ): CreateUserDefinedFunctionCommand = {
    // scalastyle:on argcount

    assert(language != null)

    language match {
      case LanguageSQL =>
        CreateSQLFunctionCommand(
          name,
          inputParamText,
          returnTypeText,
          exprText,
          queryText,
          comment,
          isDeterministic,
          containsSQL,
          isTableFunc,
          isTemp,
          ignoreIfExists,
          replace)

      case other =>
        throw UserDefinedFunctionErrors.unsupportedUserDefinedFunction(other)
    }
  }

  /**
   * Convert SQL configs to properties by prefixing all configs with a key.
   * When converting a function to [[org.apache.spark.sql.catalyst.catalog.CatalogFunction]] or
   * [[org.apache.spark.sql.catalyst.expressions.ExpressionInfo]], all SQL configs and other
   * function properties (such as the function parameters and the function return type)
   * are saved together in a property map.
   */
  def sqlConfigsToProps(conf: SQLConf): Map[String, String] = {
    val modifiedConfs = ViewHelper.getModifiedConf(conf)
    modifiedConfs.map { case (key, value) => s"$SQL_CONFIG_PREFIX$key" -> value }
  }

  /**
   * Check whether the function parameters contain duplicated column names.
   * It takes the function input parameter struct as input and verifies that there is no duplicates
   * in the parameter column names.
   * If any duplicates are found, it throws an exception with helpful information for users to
   * fix the wrong function parameters.
   *
   * Perform this check while registering the function to fail early.
   * This check does not need to run the function itself.
   */
  def checkParameterNameDuplication(
      param: StructType,
      conf: SQLConf,
      name: FunctionIdentifier): Unit = {
    val names = if (conf.caseSensitiveAnalysis) {
      param.fields.map(_.name)
    } else {
      param.fields.map(_.name.toLowerCase(Locale.ROOT))
    }
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw UserDefinedFunctionErrors.duplicateParameterNames(
        routineName = name.funcName,
        names = duplicateColumns.toSeq.sorted.mkString(", "))
    }
  }

  /**
   * Check whether the function has duplicate column names in the RETURNS clause.
   */
  def checkReturnsColumnDuplication(
      columns: StructType,
      conf: SQLConf,
      name: FunctionIdentifier): Unit = {
    val names = if (conf.caseSensitiveAnalysis) {
      columns.fields.map(_.name)
    } else {
      columns.fields.map(_.name.toLowerCase(Locale.ROOT))
    }
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw UserDefinedFunctionErrors.duplicateReturnsColumns(
        routineName = name.funcName,
        columns = duplicateColumns.toSeq.sorted.mkString(", "))
    }
  }

  /**
   * Check whether the function parameters contain non trailing defaults.
   * For languages that support default values for input parameters,
   * this check ensures once a default value is given to a parameter,
   * all subsequent parameters must also have a default value. It throws error if otherwise.
   *
   * Perform this check on function input parameters while registering the function to fail early.
   * This check does not need to run the function itself.
   */
  def checkDefaultsTrailing(param: StructType, name: FunctionIdentifier): Unit = {
    var defaultFound = false
    var previousParamName = "";
    param.fields.foreach { field =>
      if (field.getDefault().isEmpty && defaultFound) {
        throw new AnalysisException(
          errorClass = "USER_DEFINED_FUNCTIONS.NOT_A_VALID_DEFAULT_PARAMETER_POSITION",
          messageParameters = Map(
            "functionName" -> name.funcName,
            "parameterName" -> previousParamName,
            "nextParameterName" -> field.name))
      }
      defaultFound |= field.getDefault().isDefined
      previousParamName = field.name
    }
  }

  /**
   * Check whether the function input or return columns (for TABLE Return type) have NOT NULL
   * specified. Throw exception if NOT NULL is found.
   *
   * Perform this check on function input and return parameters while registering the function
   * to fail early. This check does not need to run the function itself.
   */
  def checkParameterNotNull(param: StructType, input: String): Unit = {
    param.fields.foreach { field =>
      if (!field.nullable) {
        throw UserDefinedFunctionErrors.cannotSpecifyNotNullOnFunctionParameters(input)
      }
    }
  }
}
