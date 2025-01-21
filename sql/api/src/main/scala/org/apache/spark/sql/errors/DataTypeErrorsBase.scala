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

import org.apache.spark.QueryContext
import org.apache.spark.sql.catalyst.util.{AttributeNameParser, QuotingUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[sql] trait DataTypeErrorsBase {
  def toSQLId(parts: String): String = {
    toSQLId(AttributeNameParser.parseAttributeName(parts))
  }

  def toSQLId(parts: Seq[String]): String = {
    val cleaned = parts match {
      case Seq("__auto_generated_subquery_name", rest @ _*) if rest != Nil => rest
      case other => other
    }
    cleaned.map(QuotingUtils.quoteIdentifier).mkString(".")
  }

  def toSQLStmt(text: String): String = {
    text.toUpperCase(Locale.ROOT)
  }

  def toSQLConf(conf: String): String = {
    QuotingUtils.toSQLConf(conf)
  }

  def toSQLType(text: String): String = {
    quoteByDefault(text.toUpperCase(Locale.ROOT))
  }

  def toSQLType(t: AbstractDataType): String = t match {
    case TypeCollection(types) => types.map(toSQLType).mkString("(", " or ", ")")
    case u: UserDefinedType[_] => s"UDT(${toSQLType(u.sqlType)})"
    case dt: DataType => quoteByDefault(dt.sql)
    case at => quoteByDefault(at.simpleString.toUpperCase(Locale.ROOT))
  }

  def toSQLValue(value: String): String = {
    if (value == null) {
      "NULL"
    } else {
      "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    }
  }

  def toSQLValue(value: UTF8String): String = toSQLValue(value.toString)

  def toSQLValue(value: Short): String = String.valueOf(value) + "S"

  def toSQLValue(value: Int): String = String.valueOf(value)

  def toSQLValue(value: Long): String = String.valueOf(value) + "L"

  def toSQLValue(value: Float): String = {
    if (value.isNaN) "NaN"
    else if (value.isPosInfinity) "Infinity"
    else if (value.isNegInfinity) "-Infinity"
    else value.toString
  }

  def toSQLValue(value: Double): String = {
    if (value.isNaN) "NaN"
    else if (value.isPosInfinity) "Infinity"
    else if (value.isNegInfinity) "-Infinity"
    else value.toString
  }

  protected def quoteByDefault(elem: String): String = {
    "\"" + elem + "\""
  }

  def getSummary(sqlContext: QueryContext): String = {
    if (sqlContext == null) "" else sqlContext.summary
  }

  def getQueryContext(context: QueryContext): Array[QueryContext] = {
    if (context == null) Array.empty else Array(context)
  }

  def toDSOption(option: String): String = {
    quoteByDefault(option)
  }
}
