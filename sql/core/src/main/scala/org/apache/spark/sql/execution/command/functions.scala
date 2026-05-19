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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResource, SQLFunction, SqlPathFormat}
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExpressionInfo}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{NullType, StringType, StructField, StructType}


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
    val schema = StructType(Array(StructField("function_desc", StringType, nullable = false)))
    toAttributes(schema)
  }

  private def append(buffer: ArrayBuffer[(String, String)], key: String, value: String): Unit = {
    buffer += (key -> value)
  }

  /**
   * Pad all input strings to the same length using the max string length among all inputs.
   */
  private def tabulate(inputs: Seq[String]): Seq[String] = {
    val maxLen = inputs.map(_.length).max
    inputs.map { input => input.padTo(maxLen, " ").mkString }
  }

  private def formatParameters(params: StructType): Seq[String] = {
    val names = tabulate(params.map(_.name))
    val dataTypes = tabulate(params.map(_.dataType.sql))
    // Only show parameter comments in extended mode.
    val comments = params.map { p =>
      if (isExtended) p.getComment().map(c => s" '$c'").getOrElse("") else ""
    }
    val defaults = params.map { p =>
      if (isExtended) p.getDefault().map(d => s" DEFAULT $d").getOrElse("") else ""
    }
    names zip dataTypes zip defaults zip comments map {
      case (((name, dataType), default), comment) => s"$name $dataType$default$comment"
    }
  }

  private def describeSQLFunction(
      info: ExpressionInfo,
      qualifiedName: FunctionIdentifier,
      parser: ParserInterface): Seq[Row] = {
    val buffer = new ArrayBuffer[(String, String)]
    val f = SQLFunction.fromExpressionInfo(info, parser)
    // Match the legacy DESCRIBE FUNCTION path's qualification depth so
    // `Function:` always renders the catalog-qualified 3-part name (when
    // applicable), regardless of whether the function is a SQL UDF.
    append(buffer, "Function:", qualifiedName.unquotedString)
    append(buffer, "Type:", if (f.isTableFunc) SQLFunction.TABLE else SQLFunction.SCALAR)
    // Function input
    val input = f.inputParam
    if (input.nonEmpty) {
      val params = formatParameters(input.get)
      assert(params.nonEmpty)
      append(buffer, "Input:", params.head)
      params.tail.foreach(s => append(buffer, "", s))
    } else {
      append(buffer, "Input:", "()")
    }
    // Function returns
    if (f.isTableFunc) {
      val returnParams = formatParameters(f.getTableFuncReturnCols)
      assert(returnParams.nonEmpty)
      append(buffer, "Returns:", returnParams.head)
      returnParams.tail.foreach(s => append(buffer, "", s))
    } else {
      f.getScalarFuncReturnType match {
        case _: NullType =>
        case other => append(buffer, "Returns:", other.sql)
      }
    }
    if (isExtended) {
      f.comment.foreach(c => append(buffer, "Comment:", c))
      f.collation.foreach(c => append(buffer, "Collation:", c))
      f.deterministic.foreach(d => append(buffer, "Deterministic:", d.toString))
      f.containsSQL.foreach { c =>
        val dataAccess = if (c) "CONTAINS SQL" else "READS SQL DATA"
        append(buffer, "Data Access:", dataAccess)
      }
      val configs = f.getSQLConfigs
      if (configs.nonEmpty) {
        val sorted = configs.toSeq.sortBy(_._1).map { case (key, value) => s"$key=$value" }
        append(buffer, "Configs:", sorted.head)
        sorted.tail.foreach(s => append(buffer, "", s))
      }
      f.owner.foreach(o => append(buffer, "Owner:", o))
      append(buffer, "Create Time:", new java.util.Date(f.createTimeMs).toString)
      // Put the function body at the end of the description.
      append(buffer, "Body:", f.exprText.orElse(f.queryText).get)
      // Show the frozen SQL PATH if one was persisted at function creation time.
      if (SQLConf.get.pathEnabled) {
        f.functionStoredResolutionPath
          .flatMap(SqlPathFormat.toDescribeJson)
          .flatMap(SqlPathFormat.formatForDisplay)
          .foreach(p => append(buffer, "SQL Path:", p))
      }
    }
    val keys = tabulate(buffer.map(_._1).toSeq)
    val values = buffer.map(_._2)
    keys.zip(values).map { case (key, value) => Row(s"$key $value") }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val identifier = if (info.getDb != null) {
      sparkSession.sessionState.catalog.qualifyIdentifier(
        FunctionIdentifier(info.getName, Some(info.getDb)))
    } else {
      FunctionIdentifier(info.getName)
    }
    if (SQLFunction.isSQLFunction(info.getClassName)) {
      describeSQLFunction(info, identifier, sparkSession.sessionState.sqlParser)
    } else {
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
      val funcName = if (identifier.database.isDefined) {
        val db = identifier.database.get
        if (!db.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)) {
          throw QueryExecutionErrors.invalidNamespaceNameError(
            Array(CatalogManager.SYSTEM_CATALOG_NAME, db))
        }
        if (identifier.catalog.exists(
            !_.equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME))) {
          throw QueryExecutionErrors.invalidNamespaceNameError(
            Array(identifier.catalog.get, db))
        }
        identifier.funcName
      } else {
        identifier.funcName
      }

      // Check if temp function exists first - if it does, allow dropping it even if a builtin
      // with the same name exists (shadowing case)
      if (!catalog.isTemporaryFunction(FunctionIdentifier(funcName)) &&
          catalog.isBuiltinFunction(funcName)) {
        throw QueryCompilationErrors.cannotDropBuiltinFuncError(funcName)
      }
      catalog.dropTempFunction(funcName, ifExists)
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
          case (f, "USER") if showUserFunctions => f.displayNameForShowFunctions
          case (f, "SYSTEM") if showSystemFunctions => f.displayNameForShowFunctions
        }
        .distinct
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
case class RefreshFunctionCommand(identifier: FunctionIdentifier) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
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
