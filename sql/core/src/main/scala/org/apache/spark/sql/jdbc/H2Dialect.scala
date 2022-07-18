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

import java.sql.{Connection, SQLException, Types}
import java.util
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NoSuchIndexException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
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

  // CREATE INDEX syntax
  // https://www.h2database.com/html/commands.html#create_index
  override def createIndex(
      indexName: String,
      tableIdent: Identifier,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String]): String = {
    val columnList = columns.map(col => quoteIdentifier(col.fieldNames.head))
    val (indexType, _) = JdbcUtils.processIndexProperties(properties, "h2")

    s"CREATE INDEX ${quoteIdentifier(indexName)} $indexType ON " +
      s"${tableNameWithSchema(tableIdent)} (${columnList.mkString(", ")})"
  }

  // DROP INDEX syntax
  // https://www.h2database.com/html/commands.html#drop_index
  override def dropIndex(indexName: String, tableIdent: Identifier): String = {
    s"DROP INDEX ${indexNameWithSchema(tableIdent, indexName)}"
  }

  // See https://www.h2database.com/html/systemtables.html?#information_schema_indexes
  override def indexExists(
      conn: Connection,
      indexName: String,
      tableIdent: Identifier,
      options: JDBCOptions): Boolean = {
    val sql = s"SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE " +
      s"TABLE_SCHEMA = '${tableIdent.namespace().last}' AND " +
      s"TABLE_NAME = '${tableIdent.name()}' AND INDEX_NAME = '$indexName'"
    JdbcUtils.checkIfIndexExists(conn, sql, options)
  }

  private def tableNameWithSchema(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(quoteIdentifier).mkString(".")
  }

  private def indexNameWithSchema(ident: Identifier, indexName: String): String = {
    (ident.namespace() :+ indexName).map(quoteIdentifier).mkString(".")
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
          // INDEX_ALREADY_EXISTS_1
          case 42111 =>
            throw new IndexAlreadyExistsException(message, cause = Some(e))
          // INDEX_NOT_FOUND_1
          case 42112 =>
            throw new NoSuchIndexException(message, cause = Some(e))
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
