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

package org.apache.spark.sql.hive.thriftserver

import java.util.regex.Pattern

import scala.jdk.CollectionConverters._

import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveOperationType, HivePrivilegeObject}
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.GetColumnsOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.types._

/**
 * Spark's own SparkGetColumnsOperation
 *
 * @param sqlContext SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 * @param catalogName catalog name. NULL if not applicable.
 * @param schemaName database name, NULL or a concrete database name
 * @param tableName table name
 * @param columnName column name
 */
private[hive] class SparkGetColumnsOperation(
    val sqlContext: SQLContext,
    parentSession: HiveSession,
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String)
  extends GetColumnsOperation(parentSession, catalogName, schemaName, tableName, columnName)
  with SparkOperation
  with Logging {

  val catalog: SessionCatalog = sqlContext.sessionState.catalog

  override def runInternal(): Unit = {
    // Do not change cmdStr. It's used for Hive auditing and authorization.
    val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName, tablePattern : $tableName"
    val logMsg = s"Listing columns '$cmdStr, columnName : $columnName'"

    val catalogNameStr = if (catalogName == null) "null" else catalogName
    val schemaNameStr = if (schemaName == null) "null" else schemaName
    logInfo(log"Listing columns 'catalog : ${MDC(CATALOG_NAME, catalogNameStr)}, " +
      log"schemaPattern : ${MDC(DATABASE_NAME, schemaNameStr)}, " +
      log"tablePattern : ${MDC(TABLE_NAME, tableName)}, " +
      log"columnName : ${MDC(COLUMN_NAME, columnName)}' " +
      log"with ${MDC(STATEMENT_ID, statementId)}")

    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    HiveThriftServer2.eventManager.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    val schemaPattern = convertSchemaPattern(schemaName)
    val tablePattern = convertIdentifierPattern(tableName, true)

    var columnPattern: Pattern = null
    if (columnName != null) {
      columnPattern = Pattern.compile(convertIdentifierPattern(columnName, false))
    }

    val db2Tabs = catalog.listDatabases(schemaPattern).map { dbName =>
      (dbName, catalog.listTables(dbName, tablePattern, includeLocalTempViews = false))
    }.toMap

    if (isAuthV2Enabled) {
      val privObjs = getPrivObjs(db2Tabs).asJava
      authorizeMetaGets(HiveOperationType.GET_COLUMNS, privObjs, cmdStr)
    }

    try {
      // Tables and views
      db2Tabs.foreach {
        case (dbName, tables) =>
          catalog.getTablesByName(tables).foreach { catalogTable =>
            addToRowSet(columnPattern, dbName, catalogTable.identifier.table, catalogTable.schema)
          }
      }

      // Global temporary views
      val globalTempViewDb = catalog.globalTempDatabase
      val databasePattern = Pattern.compile(CLIServiceUtils.patternToRegex(schemaName))
      if (databasePattern.matcher(globalTempViewDb).matches()) {
        catalog.globalTempViewManager.listViewNames(tablePattern).foreach { globalTempView =>
          catalog.getRawGlobalTempView(globalTempView).map(_.tableMeta.schema).foreach {
            schema => addToRowSet(columnPattern, globalTempViewDb, globalTempView, schema)
          }
        }
      }

      // Temporary views
      catalog.listLocalTempViews(tablePattern).foreach { localTempView =>
        catalog.getRawTempView(localTempView.table).map(_.tableMeta.schema).foreach {
          schema => addToRowSet(columnPattern, null, localTempView.table, schema)
        }
      }
      setState(OperationState.FINISHED)
    } catch onError()

    HiveThriftServer2.eventManager.onStatementFinish(statementId)
  }

  /**
   * For boolean, numeric and datetime types, it returns the default size of its catalyst type
   * For struct type, when its elements are fixed-size, the summation of all element sizes will be
   * returned.
   * For array, map, string, and binaries, the column size is variable, return null as unknown.
   */
  private def getColumnSize(typ: DataType): Option[Int] = typ match {
    case dt @ (BooleanType | _: NumericType | DateType | TimestampType | TimestampNTZType |
               CalendarIntervalType | NullType | _: AnsiIntervalType) =>
      Some(dt.defaultSize)
    case CharType(n) => Some(n)
    case StructType(fields) =>
      val sizeArr = fields.map(f => getColumnSize(f.dataType))
      if (sizeArr.contains(None)) {
        None
      } else {
        Some(sizeArr.map(_.get).sum)
      }
    case other => None
  }

  /**
   * The number of fractional digits for this type.
   * Null is returned for data types where this is not applicable.
   * For boolean and integrals, the decimal digits is 0
   * For floating types, we follow the IEEE Standard for Floating-Point Arithmetic (IEEE 754)
   * For timestamp values, we support microseconds
   * For decimals, it returns the scale
   */
  private def getDecimalDigits(typ: DataType) = typ match {
    case BooleanType | _: IntegerType => Some(0)
    case FloatType => Some(7)
    case DoubleType => Some(15)
    case d: DecimalType => Some(d.scale)
    case TimestampType | TimestampNTZType => Some(6)
    case _ => None
  }

  private def getNumPrecRadix(typ: DataType): Option[Int] = typ match {
    case _: NumericType => Some(10)
    case _ => None
  }

  private def toJavaSQLType(typ: DataType): Integer = typ match {
    case NullType => java.sql.Types.NULL
    case BooleanType => java.sql.Types.BOOLEAN
    case ByteType => java.sql.Types.TINYINT
    case ShortType => java.sql.Types.SMALLINT
    case IntegerType => java.sql.Types.INTEGER
    case LongType => java.sql.Types.BIGINT
    case FloatType => java.sql.Types.FLOAT
    case DoubleType => java.sql.Types.DOUBLE
    case _: DecimalType => java.sql.Types.DECIMAL
    case StringType => java.sql.Types.VARCHAR
    case VarcharType(_) => java.sql.Types.VARCHAR
    case CharType(_) => java.sql.Types.CHAR
    case BinaryType => java.sql.Types.BINARY
    case DateType => java.sql.Types.DATE
    case TimestampType | TimestampNTZType => java.sql.Types.TIMESTAMP
    case _: ArrayType => java.sql.Types.ARRAY
    case _: MapType => java.sql.Types.JAVA_OBJECT
    case _: StructType => java.sql.Types.STRUCT
    // Hive's year-month and day-time intervals are mapping to java.sql.Types.OTHER
    case _: CalendarIntervalType | _: AnsiIntervalType =>
      java.sql.Types.OTHER
    case _ => throw new IllegalArgumentException(s"Unrecognized type name: ${typ.sql}")
  }

  private def addToRowSet(
      columnPattern: Pattern,
      dbName: String,
      tableName: String,
      schema: StructType): Unit = {
    schema.zipWithIndex.foreach { case (column, pos) =>
      if (columnPattern != null && !columnPattern.matcher(column.name).matches()) {
      } else {
        val rowData = Array[AnyRef](
          null, // TABLE_CAT
          dbName, // TABLE_SCHEM
          tableName, // TABLE_NAME
          column.name, // COLUMN_NAME
          toJavaSQLType(column.dataType), // DATA_TYPE
          column.dataType.sql, // TYPE_NAME
          getColumnSize(column.dataType).map(_.asInstanceOf[AnyRef]).orNull, // COLUMN_SIZE
          null, // BUFFER_LENGTH, unused
          getDecimalDigits(column.dataType).map(_.asInstanceOf[AnyRef]).orNull, // DECIMAL_DIGITS
          getNumPrecRadix(column.dataType).map(_.asInstanceOf[AnyRef]).orNull, // NUM_PREC_RADIX
          (if (column.nullable) 1 else 0).asInstanceOf[AnyRef], // NULLABLE
          column.getComment().getOrElse(""), // REMARKS
          null, // COLUMN_DEF
          null, // SQL_DATA_TYPE
          null, // SQL_DATETIME_SUB
          null, // CHAR_OCTET_LENGTH
          pos.asInstanceOf[AnyRef], // ORDINAL_POSITION
          "YES", // IS_NULLABLE
          null, // SCOPE_CATALOG
          null, // SCOPE_SCHEMA
          null, // SCOPE_TABLE
          null, // SOURCE_DATA_TYPE
          "NO" // IS_AUTO_INCREMENT
        )
        rowSet.addRow(rowData)
      }
    }
  }

  private def getPrivObjs(db2Tabs: Map[String, Seq[TableIdentifier]]): Seq[HivePrivilegeObject] = {
    db2Tabs.foldLeft(Seq.empty[HivePrivilegeObject])({
      case (i, (dbName, tables)) => i ++ tables.map { tableId =>
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableId.table)
      }
    })
  }
}
