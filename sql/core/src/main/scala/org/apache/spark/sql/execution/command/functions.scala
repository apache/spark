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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResource}
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExpressionInfo}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
 * The DDL command that creates a function.
 * To create a temporary function, the syntax of using this command in SQL is:
 * {{{
 *    CREATE [OR REPLACE] TEMPORARY FUNCTION functionName
 *    AS className [USING JAR\FILE 'uri' [, JAR|FILE 'uri']]
 * }}}
 *
 * To create a permanent function, the syntax in SQL is:
 * {{{
 *    CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] [databaseName.]functionName
 *    AS className [USING JAR\FILE 'uri' [, JAR|FILE 'uri']]
 * }}}
 *
 * @param ignoreIfExists: When true, ignore if the function with the specified name exists
 *                        in the specified database.
 * @param replace: When true, alter the function with the specified name
 */
case class CreateFunctionCommand(
    identifier: FunctionIdentifier,
    className: String,
    resources: Seq[FunctionResource],
    isTemp: Boolean,
    ignoreIfExists: Boolean,
    replace: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val func = CatalogFunction(identifier, className, resources)
    if (isTemp) {
      if (!replace && catalog.isRegisteredFunction(identifier)) {
        throw QueryCompilationErrors.functionAlreadyExistsError(identifier)
      }
      // We first load resources and then put the builder in the function registry.
      catalog.loadFunctionResources(resources)
      catalog.registerFunction(func, overrideIfExists = replace)
    } else {
      // Handles `CREATE OR REPLACE FUNCTION AS ... USING ...`
      if (replace && catalog.functionExists(identifier)) {
        // alter the function in the metastore
        catalog.alterFunction(func)
      } else {
        // For a permanent, we will store the metadata into underlying external catalog.
        // This function will be loaded into the FunctionRegistry when a query uses it.
        // We do not load it into FunctionRegistry right now, to avoid loading the resource and
        // UDF class immediately, as the Spark application to create the function may not have
        // access to the resource and/or UDF class.
        catalog.createFunction(func, ignoreIfExists)
      }
    }
    Seq.empty[Row]
  }
}


/**
 * A command for users to get the usage of a registered function.
 * The syntax of using this command in SQL is
 * {{{
 *   DESCRIBE FUNCTION [EXTENDED] upper;
 * }}}
 */
case class DescribeFunctionCommand(
    info: ExpressionInfo,
    isExtended: Boolean) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(StructField("function_desc", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val identifier = if (info.getDb != null) {
      sparkSession.sessionState.catalog.qualifyIdentifier(
        FunctionIdentifier(info.getName, Some(info.getDb)))
    } else {
      FunctionIdentifier(info.getName)
    }
    val name = identifier.unquotedString
    val result = if (info.getClassName != null) {
      Row(s"Function: $name") ::
        Row(s"Class: ${info.getClassName}") ::
        Row(s"Usage: ${info.getUsage}") :: Nil
    } else {
      Row(s"Function: $name") :: Row(s"Usage: ${info.getUsage}") :: Nil
    }

    if (isExtended) {
      result :+ Row(s"Extended Usage:${info.getExtended}")
    } else {
      result
    }
  }
}


/**
 * The DDL command that drops a function.
 * ifExists: returns an error if the function doesn't exist, unless this is true.
 * isTemp: indicates if it is a temporary function.
 */
case class DropFunctionCommand(
    identifier: FunctionIdentifier,
    ifExists: Boolean,
    isTemp: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (isTemp) {
      assert(identifier.database.isEmpty)
      if (FunctionRegistry.builtin.functionExists(identifier)) {
        throw QueryCompilationErrors.cannotDropBuiltinFuncError(identifier.funcName)
      }
      catalog.dropTempFunction(identifier.funcName, ifExists)
    } else {
      // We are dropping a permanent function.
      catalog.dropFunction(identifier, ignoreIfNotExists = ifExists)
    }
    Seq.empty[Row]
  }
}


/**
 * A command for users to list all of the registered functions.
 * The syntax of using this command in SQL is:
 * {{{
 *    SHOW FUNCTIONS [LIKE pattern]
 * }}}
 * For the pattern, '*' matches any sequence of characters (including no characters) and
 * '|' is for alternation.
 * For example, "show functions like 'yea*|windo*'" will return "window" and "year".
 */
case class ShowFunctionsCommand(
    db: String,
    pattern: Option[String],
    showUserFunctions: Boolean,
    showSystemFunctions: Boolean,
    override val output: Seq[Attribute]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If pattern is not specified, we use '*', which is used to
    // match any sequence of characters (including no characters).
    val functionNames =
      sparkSession.sessionState.catalog
        .listFunctions(db, pattern.getOrElse("*"))
        .collect {
          case (f, "USER") if showUserFunctions => f.unquotedString
          case (f, "SYSTEM") if showSystemFunctions => f.unquotedString
        }
    // Hard code "<>", "!=", "between", "case", and "||"
    // for now as there is no corresponding functions.
    // "<>", "!=", "between", "case", and "||" is SystemFunctions,
    // only show when showSystemFunctions=true
    if (showSystemFunctions) {
      (functionNames ++
        StringUtils.filterPattern(
          FunctionRegistry.builtinOperators.keys.toSeq, pattern.getOrElse("*")))
        .sorted.map(Row(_))
    } else {
      functionNames.sorted.map(Row(_))
    }

  }
}


/**
 * A command for users to refresh the persistent function.
 * The syntax of using this command in SQL is:
 * {{{
 *    REFRESH FUNCTION functionName
 * }}}
 */
case class RefreshFunctionCommand(
    databaseName: Option[String],
    functionName: String)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (FunctionRegistry.builtin.functionExists(FunctionIdentifier(functionName, databaseName))) {
      throw QueryCompilationErrors.cannotRefreshBuiltInFuncError(functionName)
    }
    if (catalog.isTemporaryFunction(FunctionIdentifier(functionName, databaseName))) {
      throw QueryCompilationErrors.cannotRefreshTempFuncError(functionName)
    }

    val identifier = FunctionIdentifier(
      functionName, Some(databaseName.getOrElse(catalog.getCurrentDatabase)))
    // we only refresh the permanent function.
    if (catalog.isPersistentFunction(identifier)) {
      // register overwrite function.
      val func = catalog.getFunctionMetadata(identifier)
      catalog.registerFunction(func, true)
    } else {
      // clear cached function and throw exception
      catalog.unregisterFunction(identifier)
      throw QueryCompilationErrors.noSuchFunctionError(identifier)
    }

    Seq.empty[Row]
  }
}
