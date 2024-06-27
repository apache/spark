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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.UserDefinedFunction._
import org.apache.spark.sql.catalyst.parser.ParserInterface
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
}

object SQLFunction {

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
}
