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

import scala.collection.mutable

import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.UserDefinedFunction._
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalarSubquery}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Represent a SQL function.
 *
 * @param name qualified name of the SQL function
 * @param inputParam function input parameters
 * @param returnType function return type
 * @param exprText function body as an expression
 * @param queryText function body as a query
 * @param comment function comment
 * @param deterministic whether the function is deterministic
 * @param containsSQL whether the function has data access routine to be CONTAINS SQL
 * @param isTableFunc whether the function is a table function
 * @param properties additional properties to be serialized for the SQL function
 * @param owner owner of the function
 * @param createTimeMs function creation time in milliseconds
 */
case class SQLFunction(
    name: FunctionIdentifier,
    inputParam: Option[StructType],
    returnType: Either[DataType, StructType],
    exprText: Option[String],
    queryText: Option[String],
    comment: Option[String],
    deterministic: Option[Boolean],
    containsSQL: Option[Boolean],
    isTableFunc: Boolean,
    properties: Map[String, String],
    owner: Option[String] = None,
    createTimeMs: Long = System.currentTimeMillis) extends UserDefinedFunction {

  assert(exprText.nonEmpty || queryText.nonEmpty)
  assert((isTableFunc && returnType.isRight) || (!isTableFunc && returnType.isLeft))

  override val language: RoutineLanguage = LanguageSQL

  /**
   * Optionally get the function body as an expression or query using the given parser.
   */
  def getExpressionAndQuery(
      parser: ParserInterface,
      isTableFunc: Boolean): (Option[Expression], Option[LogicalPlan]) = {
    // The RETURN clause of the CREATE FUNCTION statement looks like this in the parser:
    // RETURN (query | expression)
    // If the 'query' matches and parses as a SELECT clause of one item with no FROM clause, and
    // this is a scalar function, we skip a level of subquery expression wrapping by using the
    // referenced expression directly.
    val parsedExpression = exprText.map(parser.parseExpression)
    val parsedQuery = queryText.map(parser.parsePlan)
    (parsedExpression, parsedQuery) match {
      case (None, Some(Project(expr :: Nil, _: OneRowRelation)))
        if !isTableFunc =>
        (Some(expr), None)
      case (Some(ScalarSubquery(Project(expr :: Nil, _: OneRowRelation), _, _, _, _, _, _)), None)
        if !isTableFunc =>
        (Some(expr), None)
      case (_, _) =>
        (parsedExpression, parsedQuery)
    }
  }
}

object SQLFunction {

  private val SQL_FUNCTION_PREFIX = "sqlFunction."

  private val FUNCTION_CATALOG_AND_NAMESPACE = "catalogAndNamespace.numParts"
  private val FUNCTION_CATALOG_AND_NAMESPACE_PART_PREFIX = "catalogAndNamespace.part."

  private val FUNCTION_REFERRED_TEMP_VIEW_NAMES = "referredTempViewNames"
  private val FUNCTION_REFERRED_TEMP_FUNCTION_NAMES = "referredTempFunctionsNames"
  private val FUNCTION_REFERRED_TEMP_VARIABLE_NAMES = "referredTempVariableNames"

  def parseDefault(text: String, parser: ParserInterface): Expression = {
    parser.parseExpression(text)
  }

  /**
   * This method returns an optional DataType indicating, when present, either the return type for
   * scalar user-defined functions, or a StructType indicating the names and types of the columns in
   * the output schema for table functions. If the optional value is empty, this indicates that the
   * CREATE FUNCTION statement did not have any RETURNS clause at all (for scalar functions), or
   * that it included a RETURNS TABLE clause but without any specified output schema (for table
   * functions), prompting the analyzer to infer these metadata instead.
   */
  def parseReturnTypeText(
      text: String,
      isTableFunc: Boolean,
      parser: ParserInterface): Option[Either[DataType, StructType]] = {
    if (!isTableFunc) {
      // This is a scalar user-defined function.
      if (text.isEmpty) {
        // The CREATE FUNCTION statement did not have any RETURNS clause.
        Option.empty[Either[DataType, StructType]]
      } else {
        // The CREATE FUNCTION statement included a RETURNS clause with an explicit return type.
        Some(Left(parseDataType(text, parser)))
      }
    } else {
      // This is a table function.
      if (text.equalsIgnoreCase("table")) {
        // The CREATE FUNCTION statement had a RETURNS TABLE clause but without any explicit schema.
        Option.empty[Either[DataType, StructType]]
      } else {
        // The CREATE FUNCTION statement included a RETURNS TABLE clause with an explicit schema.
        Some(Right(parseTableSchema(text, parser)))
      }
    }
  }

  def isSQLFunction(className: String): Boolean = className == SQL_FUNCTION_PREFIX

  /**
   * Convert the current catalog and namespace to properties.
   */
  def catalogAndNamespaceToProps(
      currentCatalog: String,
      currentNamespace: Seq[String]): Map[String, String] = {
    val props = new mutable.HashMap[String, String]
    val parts = currentCatalog +: currentNamespace
    if (parts.nonEmpty) {
      props.put(FUNCTION_CATALOG_AND_NAMESPACE, parts.length.toString)
      parts.zipWithIndex.foreach { case (name, index) =>
        props.put(s"$FUNCTION_CATALOG_AND_NAMESPACE_PART_PREFIX$index", name)
      }
    }
    props.toMap
  }

  /**
   * Convert the temporary object names to properties.
   */
  def referredTempNamesToProps(
      viewNames: Seq[Seq[String]],
      functionsNames: Seq[String],
      variableNames: Seq[Seq[String]]): Map[String, String] = {
    val viewNamesJson =
      JArray(viewNames.map(nameParts => JArray(nameParts.map(JString).toList)).toList)
    val functionsNamesJson = JArray(functionsNames.map(JString).toList)
    val variableNamesJson =
      JArray(variableNames.map(nameParts => JArray(nameParts.map(JString).toList)).toList)

    val props = new mutable.HashMap[String, String]
    props.put(FUNCTION_REFERRED_TEMP_VIEW_NAMES, compact(render(viewNamesJson)))
    props.put(FUNCTION_REFERRED_TEMP_FUNCTION_NAMES, compact(render(functionsNamesJson)))
    props.put(FUNCTION_REFERRED_TEMP_VARIABLE_NAMES, compact(render(variableNamesJson)))
    props.toMap
  }
}
