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

package org.apache.spark.sql.jdbc

import java.sql.{SQLException, Types}
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DecimalType, ShortType, StringType}

private[sql] object H2Dialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:h2")

  private val distinctUnsupportedAggregateFunctions =
    Set("COVAR_POP", "COVAR_SAMP", "CORR", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXY")

  private val supportedAggregateFunctions = Set("MAX", "MIN", "SUM", "COUNT", "AVG",
    "VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP") ++ distinctUnsupportedAggregateFunctions

  private val supportedFunctions = supportedAggregateFunctions ++
    Set("ABS", "COALESCE", "GREATEST", "LEAST", "RAND", "LOG", "LOG10", "LN", "EXP",
      "POWER", "SQRT", "FLOOR", "CEIL", "ROUND", "SIN", "SINH", "COS", "COSH", "TAN",
      "TANH", "COT", "ASIN", "ACOS", "ATAN", "ATAN2", "DEGREES", "RADIANS", "SIGN",
      "PI", "SUBSTRING", "UPPER", "LOWER", "TRANSLATE", "TRIM")

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Option(JdbcType("CLOB", Types.CLOB))
    case BooleanType => Some(JdbcType("BOOLEAN", Types.BOOLEAN))
    case ShortType | ByteType => Some(JdbcType("SMALLINT", Types.SMALLINT))
    case t: DecimalType => Some(
      JdbcType(s"NUMERIC(${t.precision},${t.scale})", Types.NUMERIC))
    case _ => JdbcUtils.getCommonJDBCType(dt)
  }

  private val functionMap: java.util.Map[String, UnboundFunction] =
    new ConcurrentHashMap[String, UnboundFunction]()

  // test only
  def registerFunction(name: String, fn: UnboundFunction): UnboundFunction = {
    functionMap.put(name, fn)
  }

  override def functions: Seq[(String, UnboundFunction)] = functionMap.asScala.toSeq

  // test only
  def clearFunctions(): Unit = {
    functionMap.clear()
  }

  override def classifyException(message: String, e: Throwable): AnalysisException = {
    e match {
      case exception: SQLException =>
        // Error codes are from https://www.h2database.com/javadoc/org/h2/api/ErrorCode.html
        exception.getErrorCode match {
          // TABLE_OR_VIEW_ALREADY_EXISTS_1
          case 42101 =>
            throw new TableAlreadyExistsException(message, cause = Some(e))
          // TABLE_OR_VIEW_NOT_FOUND_1
          case 42102 =>
            throw NoSuchTableException(message, cause = Some(e))
          // SCHEMA_NOT_FOUND_1
          case 90079 =>
            throw NoSuchNamespaceException(message, cause = Some(e))
          case _ => // do nothing
        }
      case _ => // do nothing
    }
    super.classifyException(message, e)
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val h2SQLBuilder = new H2SQLBuilder()
    try {
      Some(h2SQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  class H2SQLBuilder extends JDBCSQLBuilder {
    override def visitAggregateFunction(
        funcName: String, isDistinct: Boolean, inputs: Array[String]): String =
      if (isDistinct && distinctUnsupportedAggregateFunctions.contains(funcName)) {
        throw new UnsupportedOperationException(s"${this.getClass.getSimpleName} does not " +
          s"support aggregate function: $funcName with DISTINCT");
      } else {
        super.visitAggregateFunction(funcName, isDistinct, inputs)
      }

    override def visitExtract(field: String, source: String): String = {
      val newField = field match {
        case "DAY_OF_WEEK" => "ISO_DAY_OF_WEEK"
        case "WEEK" => "ISO_WEEK"
        case "YEAR_OF_WEEK" => "ISO_WEEK_YEAR"
        case _ => field
      }
      s"EXTRACT($newField FROM $source)"
    }
  }
}
