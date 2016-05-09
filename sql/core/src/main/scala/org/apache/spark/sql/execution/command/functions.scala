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

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchFunctionException}
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExpressionInfo}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
 * The DDL command that creates a function.
 * To create a temporary function, the syntax of using this command in SQL is:
 * {{{
 *    CREATE TEMPORARY FUNCTION functionName
 *    AS className [USING JAR\FILE 'uri' [, JAR|FILE 'uri']]
 * }}}
 *
 * To create a permanent function, the syntax in SQL is:
 * {{{
 *    CREATE FUNCTION [databaseName.]functionName
 *    AS className [USING JAR\FILE 'uri' [, JAR|FILE 'uri']]
 * }}}
 */
// TODO: Use Seq[FunctionResource] instead of Seq[(String, String)] for resources.
case class CreateFunction(
    databaseName: Option[String],
    functionName: String,
    className: String,
    resources: Seq[(String, String)],
    isTemp: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (isTemp) {
      if (databaseName.isDefined) {
        throw new AnalysisException(s"Specifying a database in CREATE TEMPORARY FUNCTION " +
          s"is not allowed: '${databaseName.get}'")
      }
      // We first load resources and then put the builder in the function registry.
      // Please note that it is allowed to overwrite an existing temp function.
      catalog.loadFunctionResources(resources)
      val info = new ExpressionInfo(className, functionName)
      val builder = catalog.makeFunctionBuilder(functionName, className)
      catalog.createTempFunction(functionName, info, builder, ignoreIfExists = false)
    } else {
      // For a permanent, we will store the metadata into underlying external catalog.
      // This function will be loaded into the FunctionRegistry when a query uses it.
      // We do not load it into FunctionRegistry right now.
      // TODO: should we also parse "IF NOT EXISTS"?
      catalog.createFunction(
        CatalogFunction(FunctionIdentifier(functionName, databaseName), className, resources),
        ignoreIfExists = false)
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
case class DescribeFunction(
    functionName: FunctionIdentifier,
    isExtended: Boolean) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(StructField("function_desc", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  private def replaceFunctionName(usage: String, functionName: String): String = {
    if (usage == null) {
      "N/A."
    } else {
      usage.replaceAll("_FUNC_", functionName)
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Hard code "<>", "!=", "between", and "case" for now as there is no corresponding functions.
    functionName.funcName.toLowerCase match {
      case "<>" =>
        Row(s"Function: $functionName") ::
          Row(s"Usage: a <> b - Returns TRUE if a is not equal to b") :: Nil
      case "!=" =>
        Row(s"Function: $functionName") ::
          Row(s"Usage: a != b - Returns TRUE if a is not equal to b") :: Nil
      case "between" =>
        Row(s"Function: between") ::
          Row(s"Usage: a [NOT] BETWEEN b AND c - " +
            s"evaluate if a is [not] in between b and c") :: Nil
      case "case" =>
        Row(s"Function: case") ::
          Row(s"Usage: CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END - " +
            s"When a = b, returns c; when a = d, return e; else return f") :: Nil
      case _ =>
        try {
          val info = sparkSession.sessionState.catalog.lookupFunctionInfo(functionName)
          val result =
            Row(s"Function: ${info.getName}") ::
              Row(s"Class: ${info.getClassName}") ::
              Row(s"Usage: ${replaceFunctionName(info.getUsage, info.getName)}") :: Nil

          if (isExtended) {
            result :+
              Row(s"Extended Usage:\n${replaceFunctionName(info.getExtended, info.getName)}")
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
case class DropFunction(
    databaseName: Option[String],
    functionName: String,
    ifExists: Boolean,
    isTemp: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (isTemp) {
      if (databaseName.isDefined) {
        throw new AnalysisException(s"Specifying a database in DROP TEMPORARY FUNCTION " +
          s"is not allowed: '${databaseName.get}'")
      }
      if (FunctionRegistry.builtin.functionExists(functionName)) {
        throw new AnalysisException(s"Cannot drop native function '$functionName'")
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
 *
 * TODO currently we are simply ignore the db
 */
case class ShowFunctions(db: Option[String], pattern: Option[String]) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val schema = StructType(StructField("function", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbName = db.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase)
    // If pattern is not specified, we use '*', which is used to
    // match any sequence of characters (including no characters).
    val functionNames =
      sparkSession.sessionState.catalog
        .listFunctions(dbName, pattern.getOrElse("*"))
        .map(_.unquotedString)
    // The session catalog caches some persistent functions in the FunctionRegistry
    // so there can be duplicates.
    functionNames.distinct.sorted.map(Row(_))
  }
}
