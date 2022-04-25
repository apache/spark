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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

trait QueryErrorsBase {
  private def litToErrorValue(l: Literal): String = l match {
    case Literal(null, _) => "NULL"
    case Literal(v: Float, FloatType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else v.toString
    case Literal(v: Double, DoubleType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else l.sql
    case l => l.sql
  }

  // Converts an error class parameter to its SQL representation
  def toSQLValue(v: Any): String = {
    litToErrorValue(Literal(v))
  }

  def toSQLValue(v: Any, t: DataType): String = {
    litToErrorValue(Literal.create(v, t))
  }

  private def quoteByDefault(elem: String): String = {
    "\"" + elem + "\""
  }

  // Quote sql statements in error messages.
  def toSQLStmt(text: String): String = {
    quoteByDefault(text.toUpperCase(Locale.ROOT))
  }

  def toSQLId(parts: Seq[String]): String = {
    parts.map(quoteIdentifier).mkString(".")
  }

  def toSQLId(parts: String): String = {
    toSQLId(parts.split("\\."))
  }

  def toSQLType(t: DataType): String = {
    quoteByDefault(t.sql)
  }

  def toSQLConf(conf: String): String = {
    quoteByDefault(conf)
  }
}
