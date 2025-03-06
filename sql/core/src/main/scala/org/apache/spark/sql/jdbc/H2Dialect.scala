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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

import org.apache.spark.{SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NoSuchIndexException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.index.TableIndex
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, NamedReference}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DecimalType, MetadataBuilder, ShortType, StringType, TimestampType}

private[sql] case class H2Dialect() extends JdbcDialect with NoLegacyJDBCError {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:h2")

  private val distinctUnsupportedAggregateFunctions =
    Set("COVAR_POP", "COVAR_SAMP", "CORR", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXY",
      "MODE", "PERCENTILE_CONT", "PERCENTILE_DISC")

  private val supportedAggregateFunctions = Set("MAX", "MIN", "SUM", "COUNT", "AVG",
    "VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP") ++ distinctUnsupportedAggregateFunctions

  private val supportedFunctions = supportedAggregateFunctions ++
    Set("ABS", "COALESCE", "GREATEST", "LEAST", "RAND", "LOG", "LOG10", "LN", "EXP",
      "POWER", "SQRT", "FLOOR", "CEIL", "ROUND", "SIN", "SINH", "COS", "COSH", "TAN",
      "TANH", "COT", "ASIN", "ACOS", "ATAN", "ATAN2", "DEGREES", "RADIANS", "SIGN",
      "PI", "SUBSTRING", "UPPER", "LOWER", "TRANSLATE", "TRIM", "MD5", "SHA1", "SHA2",
      "BIT_LENGTH", "CHAR_LENGTH", "CONCAT", "RPAD", "LPAD")

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case Types.NUMERIC if size > 38 =>
        // H2 supports very large decimal precision like 100000. The max precision in Spark is only
        // 38. Here we shrink both the precision and scale of H2 decimal to fit Spark, and still
        // keep the ratio between them.
        val scale = if (null != md) md.build().getLong("scale") else 0L
        val selectedScale = (DecimalType.MAX_PRECISION * (scale.toDouble / size.toDouble)).toInt
        Option(DecimalType(DecimalType.MAX_PRECISION, selectedScale))
      case Types.TIMESTAMP_WITH_TIMEZONE | Types.TIME_WITH_TIMEZONE => Some(TimestampType)
      case _ => None
    }
  }

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
    val sql = "SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE " +
      s"TABLE_SCHEMA = '${tableIdent.namespace().last}' AND " +
      s"TABLE_NAME = '${tableIdent.name()}' AND INDEX_NAME = '$indexName'"
    JdbcUtils.checkIfIndexExists(conn, sql, options)
  }

  // See
  // https://www.h2database.com/html/systemtables.html?#information_schema_indexes
  // https://www.h2database.com/html/systemtables.html?#information_schema_index_columns
  override def listIndexes(
      conn: Connection,
      tableIdent: Identifier,
      options: JDBCOptions): Array[TableIndex] = {
    val sql = {
      s"""
         | SELECT
         |   i.INDEX_CATALOG AS INDEX_CATALOG,
         |   i.INDEX_SCHEMA AS INDEX_SCHEMA,
         |   i.INDEX_NAME AS INDEX_NAME,
         |   i.INDEX_TYPE_NAME AS INDEX_TYPE_NAME,
         |   i.REMARKS as REMARKS,
         |   ic.COLUMN_NAME AS COLUMN_NAME
         | FROM INFORMATION_SCHEMA.INDEXES i, INFORMATION_SCHEMA.INDEX_COLUMNS ic
         | WHERE i.TABLE_CATALOG = ic.TABLE_CATALOG
         | AND i.TABLE_SCHEMA = ic.TABLE_SCHEMA
         | AND i.TABLE_NAME = ic.TABLE_NAME
         | AND i.INDEX_CATALOG = ic.INDEX_CATALOG
         | AND i.INDEX_SCHEMA = ic.INDEX_SCHEMA
         | AND i.INDEX_NAME = ic.INDEX_NAME
         | AND i.TABLE_NAME = '${tableIdent.name()}'
         | AND i.INDEX_SCHEMA = '${tableIdent.namespace().last}'
         |""".stripMargin
    }
    var indexMap: Map[String, TableIndex] = Map()
    try {
      JdbcUtils.executeQuery(conn, options, sql) { rs =>
        while (rs.next()) {
          val indexName = rs.getString("INDEX_NAME")
          val colName = rs.getString("COLUMN_NAME")
          val indexType = rs.getString("INDEX_TYPE_NAME")
          val indexComment = rs.getString("REMARKS")
          if (indexMap.contains(indexName)) {
            val index = indexMap(indexName)
            val newIndex = new TableIndex(indexName, indexType,
              index.columns() :+ FieldReference(colName),
              index.columnProperties, index.properties)
            indexMap += (indexName -> newIndex)
          } else {
            val properties = new util.Properties()
            if (StringUtils.isNotEmpty(indexComment)) properties.put("COMMENT", indexComment)
            val index = new TableIndex(indexName, indexType, Array(FieldReference(colName)),
              new util.HashMap[NamedReference, util.Properties](), properties)
            indexMap += (indexName -> index)
          }
        }
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot retrieved index info.")
    }

    indexMap.values.toArray
  }

  private def tableNameWithSchema(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(quoteIdentifier).mkString(".")
  }

  private def indexNameWithSchema(ident: Identifier, indexName: String): String = {
    (ident.namespace() :+ indexName).map(quoteIdentifier).mkString(".")
  }

  override def classifyException(
      e: Throwable,
      condition: String,
      messageParameters: Map[String, String],
      description: String,
      isRuntime: Boolean): Throwable with SparkThrowable = {
    e match {
      case exception: SQLException =>
        // Error codes are from https://www.h2database.com/javadoc/org/h2/api/ErrorCode.html
        exception.getErrorCode match {
          // TABLE_OR_VIEW_ALREADY_EXISTS_1
          case 42101 =>
            // The message is: Table "identifier" already exists
            val regex = """"((?:[^"\\]|\\[\\"ntbrf])+)"""".r
            val name = regex.findFirstMatchIn(e.getMessage).get.group(1)
            val quotedName = org.apache.spark.sql.catalyst.util.quoteIdentifier(name)
            throw new TableAlreadyExistsException(
              errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
              messageParameters = Map("relationName" -> quotedName),
              cause = Some(e))
          // TABLE_OR_VIEW_NOT_FOUND_1
          case 42102 =>
            val relationName = messageParameters.getOrElse("tableName", "")
            throw new NoSuchTableException(
              errorClass = "TABLE_OR_VIEW_NOT_FOUND",
              messageParameters = Map("relationName" -> relationName),
              cause = Some(e))
          // SCHEMA_NOT_FOUND_1
          case 90079 =>
            val regex = """"((?:[^"\\]|\\[\\"ntbrf])+)"""".r
            val name = regex.findFirstMatchIn(e.getMessage).get.group(1)
            val quotedName = org.apache.spark.sql.catalyst.util.quoteIdentifier(name)
            throw new NoSuchNamespaceException(errorClass = "SCHEMA_NOT_FOUND",
              messageParameters = Map("schemaName" -> quotedName))
          // INDEX_ALREADY_EXISTS_1
          case 42111 if condition == "FAILED_JDBC.CREATE_INDEX" =>
            val indexName = messageParameters("indexName")
            val tableName = messageParameters("tableName")
            throw new IndexAlreadyExistsException(
              indexName = indexName, tableName = tableName, cause = Some(e))
          // INDEX_NOT_FOUND_1
          case 42112 if condition == "FAILED_JDBC.DROP_INDEX" =>
            val indexName = messageParameters("indexName")
            val tableName = messageParameters("tableName")
            throw new NoSuchIndexException(indexName, tableName, cause = Some(e))
          case _ => // do nothing
        }
      case _ => // do nothing
    }
    super.classifyException(e, condition, messageParameters, description, isRuntime)
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
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3184",
          messageParameters = Map(
            "class" -> this.getClass.getSimpleName,
            "funcName" -> funcName))
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

    override def visitSQLFunction(funcName: String, inputs: Array[String]): String = {
      funcName match {
        case "MD5" =>
          "RAWTOHEX(HASH('MD5', " + inputs.mkString(",") + "))"
        case "SHA1" =>
          "RAWTOHEX(HASH('SHA-1', " + inputs.mkString(",") + "))"
        case "SHA2" =>
          "RAWTOHEX(HASH('SHA-" + inputs(1) + "'," + inputs(0) + "))"
        case _ => super.visitSQLFunction(funcName, inputs)
      }
    }
  }

  override def supportsLimit: Boolean = true

  override def supportsOffset: Boolean = true
}
