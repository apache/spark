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

package org.apache.spark.sql.errors

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, toPrettySQL}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

/**
 * The trait exposes util methods for preparing error messages such as quoting of error elements.
 * All classes that extent `QueryErrorsBase` shall follow the rules:
 * 1. Any values shall be outputted in the SQL standard style by using `toSQLValue()`.
 *   For example: 'a string value', 1, NULL.
 * 2. SQL types shall be double quoted and outputted in the upper case using `toSQLType()`.
 *   For example: "INT", "DECIMAL(10,0)".
 * 3. Elements of identifiers shall be wrapped by backticks by using `toSQLId()`.
 *   For example: `namespaceA`.`funcB`, `tableC`.
 * 4. SQL statements shall be in the upper case prepared by using `toSQLStmt`.
 *   For example: DESC PARTITION, DROP TEMPORARY FUNCTION.
 * 5. SQL configs and datasource options shall be wrapped by double quotes by using
 *   `toSQLConf()`/`toDSOption()`.
 *   For example: "spark.sql.ansi.enabled".
 * 6. Any values of datasource options or SQL configs shall be double quoted.
 *   For example: "true", "CORRECTED".
 * 7. SQL expressions shall be wrapped by double quotes.
 *   For example: "earnings + 1".
 */
private[sql] trait QueryErrorsBase {
  // Converts an error class parameter to its SQL representation
  def toSQLValue(v: Any, t: DataType): String = Literal.create(v, t) match {
    case Literal(null, _) => "NULL"
    case Literal(v: Float, FloatType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else v.toString
    case l @ Literal(v: Double, DoubleType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else l.sql
    case l => l.sql
  }

  private def quoteByDefault(elem: String): String = {
    "\"" + elem + "\""
  }

  def toSQLStmt(text: String): String = {
    text.toUpperCase(Locale.ROOT)
  }

  def toSQLId(parts: Seq[String]): String = {
    parts.map(quoteIdentifier).mkString(".")
  }

  def toSQLId(parts: String): String = {
    toSQLId(UnresolvedAttribute.parseAttributeName(parts))
  }

  def toSQLType(t: DataType): String = {
    quoteByDefault(t.sql)
  }

  def toSQLType(text: String): String = {
    quoteByDefault(text.toUpperCase(Locale.ROOT))
  }

  def toSQLConf(conf: String): String = {
    quoteByDefault(conf)
  }

  def toDSOption(option: String): String = {
    quoteByDefault(option)
  }

  def toSQLExpr(e: Expression): String = {
    quoteByDefault(toPrettySQL(e))
  }
}
