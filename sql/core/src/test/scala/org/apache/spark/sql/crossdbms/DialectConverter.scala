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

sackage org.apache.spark.sql.crossdbms

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
    val doublePrecisionPattern = """(\d+\.\d+)D""".r
    val doublePrecisionResult = doublePrecisionPattern.replaceAllIn(query,
      m => s"CAST(${m.group(1)} AS DOUBLE PRECISION)")

    // Convert {number}S to CAST({number} AS SMALLINT)
    val smallIntPattern = """(\d+)S""".r
    val smallIntResult = smallIntPattern.replaceAllIn(doublePrecisionResult, m =>
      s"CAST(${m.group(1)} AS SMALLINT)")

    // Convert float({some decimal number}) to CAST({decimal number} AS REAL)
    val floatPattern = """float\((\d+\.\d+)\)""".r
    val floatResult = floatPattern.replaceAllIn(smallIntResult, m => s"CAST(${m.group(1)} AS REAL)")

    // Convert {{some number}E{some number}}BD to CAST({number}E{number} AS DECIMAL)
    val decimalPattern = """(\d+E\d+)BD""".r
    decimalPattern.replaceAllIn(floatResult, m => s"CAST(${m.group(1)} AS DECIMAL)")
  }

  // Replace double quotes with single quotes
  private def replaceDoubleQuotes(query: String): String = query.replaceAll("\"", "'")

  private def convertCreateViewSyntax(query: String): String = {
    // (?i):
    //    This is a flag for case-insensitive matching.
    // CREATE\s+:
    //    Matches the literal string "CREATE" followed by one or more whitespace characters.
    // ((?:TEMP(?:ORARY)?)?\s+)?:
    //    This is a capturing group for the optional "TEMP" or "TEMPORARY" part. The (?: ... ) is a
    //    non-capturing group, and the ? after it makes the entire group optional.
    // VIEW\s+:
    //    Matches the literal string "VIEW" followed by one or more whitespace characters.
    // (IF NOT EXISTS\s+)?:
    //    This is a capturing group for the optional "IF NOT EXISTS" part followed by one or more
    //    whitespace characters. The ? after it makes the entire group optional.
    // (\w+):
    //    This is a capturing group that matches one or more word characters (letters, digits, or
    //    underscores), capturing the view name.
    // \s*:
    //    Matches zero or more whitespace characters.
    // (?:\(([\w, ]+)\))?:
    //    This is a non-capturing group that matches an optional group containing a list of word
    //    characters, commas, and spaces within parentheses. The \w allows word characters, allows
    //    for multiple column names separated by commas, and the space allows for spaces within the
    //    parentheses. The ?: makes it a non-capturing group, and the entire group is optional.
    // \s+:
    //    Matches one or more whitespace characters.
    //  AS:
    //    Matches the literal string "AS".
    // \s+:
    //    Matches one or more whitespace characters.
    // (.+):
    //    This is a capturing group that matches one or more of any character (except for a
    //    newline), capturing the view query.

    val createViewPattern =
      """(?i)CREATE\s+((?:TEMP(?:ORARY)?)?\s+)?VIEW\s+(IF NOT EXISTS\s+)?(\w+)\s*(?:\(([\w, ]+)\))? AS\s+(.+)""".r

    query match {
      case createViewPattern(temp, ifNotExists, viewName, columnList, viewQuery) =>
        s"""
          | CREATE $temp VIEW $ifNotExists $viewName($columnList) AS ($viewQuery)
          |""".stripMargin
      case _ => query
    }
  }

  def preprocessQuery(query: String): String = {
    val singleQuotedQuery = replaceDoubleQuotes(query)
    val queryWithReplacedDataTypes = dataTypeConverter(singleQuotedQuery)
    convertCreateViewSyntax(queryWithReplacedDataTypes)
  }
}
