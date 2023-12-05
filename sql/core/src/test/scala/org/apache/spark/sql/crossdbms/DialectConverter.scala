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

package org.apache.spark.sql.crossdbms

trait DialectConverter {

  /**
   * Transform the query string, which is compatible with Spark, such that it is compatible with
   * dialect. This transforms a query string itself. Note that this is a best effort conversion and
   * may not always produce a compatible query string.
   */
  def preprocessQuery(query: String): String
}

/**
 * Wrapper around utility functions that help convert from Spark SQL to PostgreSQL.
 */
object PostgresDialectConverter extends DialectConverter {

  private def dataTypeConverter(query: String): String = {
    // Convert {some decimal number}D to CAST({decimal number} AS DOUBLE PRECISION)
    val doublePrecisionPattern = """(\d+(?:\.\d+)?)D""".r
    val doublePrecisionResult = doublePrecisionPattern.replaceAllIn(query,
      m => s"CAST(${m.group(1)} AS DOUBLE PRECISION)")

    // Convert {number}S to CAST({number} AS SMALLINT)
    val smallIntPattern = """(\d+)S""".r
    val smallIntResult = smallIntPattern.replaceAllIn(doublePrecisionResult, m =>
      s"CAST(${m.group(1)} AS SMALLINT)")

    // Convert float({some decimal number}) to CAST({decimal number} AS REAL)
    val floatPattern = """float\((\d+(?:\.\d+)?)\)""".r
    val floatResult = floatPattern.replaceAllIn(smallIntResult, m => s"CAST(${m.group(1)} AS REAL)")

    // Convert {{some number}E{some number}}BD to CAST({number}E{number} AS DECIMAL)
    val decimalPattern = """(\d+E\d+)BD""".r
    val decimalResult = decimalPattern.replaceAllIn(floatResult,
      m => s"CAST(${m.group(1)} AS DECIMAL)")

    // Convert {some number, like 10}L to CAST(10 AS BIGINT)
    val bigIntPattern = """(\d+)L""".r
    bigIntPattern.replaceAllIn(decimalResult, m => s"CAST(${m.group(1)} AS BIGINT)")
  }

  // Replace double quotes with single quotes
  private def replaceDoubleQuotes(query: String): String = query.replaceAll("\"", "'")

  private def convertValuesClause(query: String): String = {
    var valuesClause = ""
    var tableAlias = ""
    if (query.contains("as")) {
      val split = query.split("as")
      valuesClause = split(0).trim
      tableAlias = split(1).trim
    } else if (query.contains("AS")) {
      val split = query.split("AS")
      valuesClause = split(0).trim
      tableAlias = split(1).trim
    } else {
      valuesClause = query
    }

    val valuesClausePattern = """(?i)(SELECT .+ FROM)?\s*VALUES\s*(\((?:.+\,*)+\))+""".r
    valuesClause match {
      case valuesClausePattern(select, allValues) =>
        val selectClause = if (select != null) { select + " " } else { "" }
        val tableAliasClause = if (tableAlias.isEmpty) {
          ""
        } else {
          s" AS $tableAlias"
        }
        s"$selectClause(VALUES $allValues)" + tableAliasClause
      case _ =>
        query
    }
  }

  private def convertCreateViewSyntax(query: String): String = {
    val createViewPattern =
      """(?i)CREATE\s+((?:TEMP(?:ORARY)?)?\s+)?VIEW\s+(IF NOT EXISTS\s+)?(\w+)\s*(?:\(([\w, ]+)\))?
        | AS\s+(.+)""".stripMargin.r

    query match {
      case createViewPattern(temp, ifNotExists, viewName, columnList, viewQuery) =>
        val ifNotExistsClause = if (ifNotExists != null) {ifNotExists} else { "" }
        val columnListClause = if (columnList != null) { s"($columnList)" } else { "" }
        val convertedValuesClause = convertValuesClause(viewQuery)
        s"""
           |CREATE $temp VIEW $ifNotExistsClause $viewName$columnListClause
           |AS $convertedValuesClause
           |""".stripMargin
      case _ =>
        query
    }
  }

  private def removeNewlines(input: String): String = {
    input.replaceAll("\n|\r\n?", "")
  }

  def preprocessQuery(query: String): String = {
    val noNewLines = removeNewlines(query)
    val singleQuotedQuery = replaceDoubleQuotes(noNewLines)
    val newValuesSyntax = convertCreateViewSyntax(singleQuotedQuery)
    dataTypeConverter(newValuesSyntax)
  }
}
