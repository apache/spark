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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchFunctionException}
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResource}
import org.apache.spark.sql.catalyst.expressions.Attribute
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
    databaseName: Option[String],
    functionName: String,
    className: String,
    resources: Seq[FunctionResource],
    isTemp: Boolean,
    ignoreIfExists: Boolean,
    replace: Boolean)
  extends LeafRunnableCommand {

  if (ignoreIfExists && replace) {
    throw QueryCompilationErrors.createFuncWithBothIfNotExistsAndReplaceError()
  }

  // Disallow to define a temporary function with `IF NOT EXISTS`
  if (ignoreIfExists && isTemp) {
    throw QueryCompilationErrors.defineTempFuncWithIfNotExistsError()
  }

  // Temporary function names should not contain database prefix like "database.function"
  if (databaseName.isDefined && isTemp) {
    throw QueryCompilationErrors.specifyingDBInCreateTempFuncError(databaseName.get)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val func = CatalogFunction(FunctionIdentifier(functionName, databaseName), className, resources)
    if (isTemp) {
      if (!replace && catalog.isRegisteredFunction(func.identifier)) {
        throw QueryCompilationErrors.functionAlreadyExistsError(func.identifier)
      }
      // We first load resources and then put the builder in the function registry.
      catalog.loadFunctionResources(resources)
      catalog.registerFunction(func, overrideIfExists = replace)
    } else {
      // Handles `CREATE OR REPLACE FUNCTION AS ... USING ...`
      if (replace && catalog.functionExists(func.identifier)) {
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
    functionName: FunctionIdentifier,
    isExtended: Boolean) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(StructField("function_desc", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Hard code "<>", "!=", "between", "case", and "||"
    // for now as there is no corresponding functions.
    functionName.funcName.toLowerCase(Locale.ROOT) match {
      case "<>" =>
        Row(s"Function: $functionName") ::
          Row("Usage: expr1 <> expr2 - " +
            "Returns true if `expr1` is not equal to `expr2`.") :: Nil
      case "!=" =>
        Row(s"Function: $functionName") ::
          Row("Usage: expr1 != expr2 - " +
            "Returns true if `expr1` is not equal to `expr2`.") :: Nil
      case "between" =>
        Row("Function: between") ::
          Row("Usage: expr1 [NOT] BETWEEN expr2 AND expr3 - " +
            "evaluate if `expr1` is [not] in between `expr2` and `expr3`.") :: Nil
      case "case" =>
        Row("Function: case") ::
          Row("Usage: CASE expr1 WHEN expr2 THEN expr3 " +
            "[WHEN expr4 THEN expr5]* [ELSE expr6] END - " +
            "When `expr1` = `expr2`, returns `expr3`; " +
            "when `expr1` = `expr4`, return `expr5`; else return `expr6`.") :: Nil
      case "||" =>
        Row("Function: ||") ::
          Row("Usage: expr1 || expr2 - Returns the concatenation of `expr1` and `expr2`.") :: Nil
      case _ =>
        try {
          val info = sparkSession.sessionState.catalog.lookupFunctionInfo(functionName)
          val name = if (info.getDb != null) info.getDb + "." + info.getName else info.getName
          val result =
            Row(s"Function: $name") ::
              Row(s"Class: ${info.getClassName}") ::
              Row(s"Usage: ${info.getUsage}") :: Nil

          if (isExtended) {
            result :+
              Row(s"Extended Usage:${info.getExtended}")
          } else {
            result
          }
        } catch {
          case _: NoSuchFunctionException => Seq(Row(s"Function: $functionName not found."))
        }
    }
  }
}


/**
 * The DDL command that drops a function.
 * ifExists: returns an error if the function doesn't exist, unless this is true.
 * isTemp: indicates if it is a temporary function.
 */
case class DropFunctionCommand(
    databaseName: Option[String],
    functionName: String,
    ifExists: Boolean,
    isTemp: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (isTemp) {
      if (databaseName.isDefined) {
        throw QueryCompilationErrors.specifyingDBInDropTempFuncError(databaseName.get)
      }
      if (FunctionRegistry.builtin.functionExists(FunctionIdentifier(functionName))) {
        throw QueryCompilationErrors.cannotDropNativeFuncError(functionName)
      }
      catalog.dropTempFunction(functionName, ifExists)
    } else {
      // We are dropping a permanent function.
      catalog.dropFunction(
        FunctionIdentifier(functionName, databaseName),
        ignoreIfNotExists = ifExists)
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
    db: Option[String],
    pattern: Option[String],
    showUserFunctions: Boolean,
    showSystemFunctions: Boolean,
    override val output: Seq[Attribute]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbName = db.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase)
    // If pattern is not specified, we use '*', which is used to
    // match any sequence of characters (including no characters).
    val functionNames =
      sparkSession.sessionState.catalog
        .listFunctions(dbName, pattern.getOrElse("*"))
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
        StringUtils.filterPattern(FunctionsCommand.virtualOperators, pattern.getOrElse("*")))
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

object FunctionsCommand {
  // operators that do not have corresponding functions.
  // They should be handled `DescribeFunctionCommand`, `ShowFunctionsCommand`
  val virtualOperators = Seq("!=", "<>", "between", "case", "||")
}
